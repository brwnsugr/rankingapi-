class RankingSchoolsJob < ApplicationJob
  include ActiveJobRetryControlable
  queue_as :default
  retry_limit 1

  rescue_from(StandardError) do |exception|
    Rails.logger.info exception.message
    raise exception if retry_limit_exceeded?
    retry_job(wait: attempt_number**2)
  end

  def perform(*args)
    Rails.logger.info("RankingUsersJob job is started")
    thisweekday = now.strftime('%u').to_i
    thismonth = now.strftime('%m').to_i
    thisdayofmonth = now.strftime('%d').to_i
    thisweekofyear = now.strftime('%W').to_i
    time_hash(thisweekday, thisdayofmonth)
    certi_count(thisweekday, thisdayofmonth)
    reward_amount(thisweekday, thisdayofmonth)
  end

  private

  def now
    @now = Time.current.midnight
  end

  def ranking_type_hash
    @ranking_type = Hash.new.update("weekly_certi_count"=> 1, "monthly_certi_count"=> 2, "weekly_reward_amount"=>3, "monthly_reward_amount"=>4 )
  end

  def time_hash(thisweekday, thisdayofmonth)
    lastweekdate = (now - (thisweekday-1).days).midnight if thisweekday > 1
    lastweekdate = (now - 7.days).midnight if thisweekday == 1
    lastmonthdate = (now - (thisdayofmonth-1).days).midnight if thisdayofmonth > 1
    lastmonthdate = (now - 1.month).midnight if thisdayofmonth == 1
    @time_hash = Hash.new.update( 1 => lastweekdate, 2=> lastmonthdate, 3 => lastweekdate, 4 => lastmonthdate)
  end

  def save_ranking(arr, ranking_type) # ranking_type이 1이면 누적 인증수 기준, 2면 누적 상금액 기준 
    year = (now-1.days).strftime('%Y').to_i
    RankingSchool.transaction do
      arr.each do |item|
        data = {
          ranking_type: ranking_type,
          ranking: item[:ranking],
          school_id: item[:school_id],
          type_cd: item[:type_cd],
          total_score: item[:total_score],
          sub_score: item[:sub_score],
          selected_count: item[:selected_count],
          rank_diff: item[:rank_diff],
          year: year,
        }
      record = RankingSchool.new(data)
      record.save!
      end
    end
      puts 'Ranking Transaction Executed Successfully!'
    rescue => e
      puts "#{e.message}"
  end

  def set_ranking(arr) # 동점자 ranking 처리  
    i = 0
    rank = 1
    while i < arr.length
      arr[i][:rank_diff] = 999
      arr[i][:ranking] = rank if i == 0
      if i > 0 && arr[i][:total_score] == arr[i-1][:total_score]
        arr[i][:ranking] = rank 
      else
        rank = i + 1
        arr[i][:ranking] = rank
      end
      i+=1
    end
    arr
  end

  def rank_diff_calculate(arr, ranking_type)
    # Return an Array of the Previous Time(a week or month) which needs to Compare with the Current Ranking.
    lastdate = @time_hash[ranking_type]
    school_type = arr[0][:type_cd].present? ? arr[0][:type_cd] : nil
    previous_records = RankingSchool.where('(created_at > ? and created_at < ?)', lastdate, lastdate+10.hours)
                                    .where(ranking_type: ranking_type)
                                    .where(type_cd: school_type)
                                    .order(:ranking)
                                    .pluck(:school_id, :ranking, :total_score)
    i = 0
    if previous_records.present?
      tmp = Hash.new
      while i < arr.length
        previous_records.each do |record|
          if record[0] == arr[i][:school_id] #지난주or지난달 동일한 학교가 랭킹에 들어있을 경우 rank_diff 비교
            arr[i][:rank_diff] = record[1] - arr[i][:ranking]
            break
          end
        end
        if arr[i][:rank_diff] == 999
          # 지난 주/달 top 100 바깥에 있으면서 공동 100위인 유저들을 뽑아서 현재 top 100 학교와 비교할 것   
          tmp[arr[i][:school_id]] = arr[i]
          #이번 top100 학교의 지난주/달의 랭킹 체크 
        end
        i += 1
      end
      check_score = previous_records[-1][2]
      check_ranking = previous_records[-1][1]
      if ranking_type == 1 || ranking_type == 2
        startdate = (ranking_type==1) ? lastdate-7.days : lastdate-1.month
        records_to_check = SelectedSchool.includes(user: :user_missions)
                                      .where('(user_missions.confirmed_at > ? and user_missions.confirmed_at < ?)', startdate, lastdate)
                                      .where('user_missions.results = 1')
                                      .where(school_id: tmp.map{|item| item[0]})
                                      .where(type_cd: school_type)
                                      .references(:user_missions)
                                      .group(:school_id)
                                      .count('user_missions.id')

        if records_to_check.present?
          records_to_check.each do |key, value|
            if value == check_score
              tmp[key][:rank_diff] = check_ranking - tmp[key][:ranking]
            end
          end
        end
      elsif ranking_type == 3 || ranking_type == 4
        startdate = (ranking_type==3) ? lastdate-7.days : lastdate-1.month
        records_to_check = SelectedSchool.includes(user: :challenge_user_results)
                                          .where(type_cd: school_type)
                                          .where(school_id: tmp.map{|item| item[0]})
                                          .where('(challenge_user_results.created_at > ? and challenge_user_results.created_at < ?)', startdate, lastdate)
                                          .references(:challenge_user_results)
                                          .group(:school_id)
                                          .sum('challenge_user_results.final_prize + challenge_user_results.final_scholar')
        
        if records_to_check.present?
          records_to_check.each do |key, value|
            if value = check_score
              tmp[key][:rank_diff] = check_ranking - tmp[key][:ranking]
            end
          end
        end
      end
    end
    return arr
  end

  def delete_temporary_ranking(ranking_type, daysago)
    RankingSchool.transaction do
      rankings_to_delete = RankingSchool.where('(created_at > ? and created_at < ?)', now-daysago.days, now-((daysago-1).days))
                                      .where(ranking_type: ranking_type)

      if rankings_to_delete.present?
        rankings_to_delete.destroy_all
        puts "Deleting the Ranking_type: #{ranking_type}, Data #{daysago} days ago Transaction Executed Successfully" 
      else
        puts "Nothing Exists to be destroyed"
      end
    end
  rescue => e
    puts "#{e.message}"
  end

  def cumulative_ranking(dayoforder, begindate, counts, ranking_type)
    if counts.present?
      ranking_type = @ranking_type[ranking_type]
      
      set_ranking(counts)
      rank_diff_calculate(counts, ranking_type)
      save_ranking(counts, ranking_type) # add new ranking
    end
    delete_temporary_ranking(ranking_type, 2) unless dayoforder == 3 # delete the ranking 2days ago
  end

  def sub_prize_calculate(arr, school_type, begindate)
    results = SelectedSchool.includes(user: :challenge_user_results)
                            .where('type_cd = ?', school_type)
                            .where(school_id: arr)
                            .where('(challenge_user_results.created_at > ? and challenge_user_results.created_at < ?)', begindate,now)
                            .references(:challenge_user_results)
                            .group(:school_id)
                            .sum('challenge_user_results.final_prize + challenge_user_results.final_scholar')
    
    return results
  end

  def sub_certi_calculate(arr, school_type, begindate)
    results = SelectedSchool.includes(user: :user_missions)
                            .where('type_cd = ?', school_type)
                            .where(school_id: arr)
                            .where('(user_missions.confirmed_at > ? and user_missions.confirmed_at < ?)', begindate, now)
                            .references(:user_missions)
                            .group(:school_id)
                            .count('user_missions.id')

    return results 
  end

  def selected_count(arr)
    results = SelectedSchool.includes(:user).where(school_id: arr).group(:school_id).count('users.id')
    return results
  end

  def record_wrap(sub_records, schools, school_type, selected_counts)
    counts = []
    if schools.present?
      schools.each do |school|
        sub_score = 0
        sub_score = sub_records[school[0]].to_i if sub_records[school[0]].present?
        selected_count = 0
        selected_count = selected_counts[school[0]] if selected_counts[school[0]].present?
        counts.push({
          type_cd: school_type,
          school_id: school[0],
          total_score: school[1],
          sub_score: sub_score,
          selected_count: selected_count,
        })
        end
    end
    return counts
  end

  def certi_count(dayofweek, dayofmonth)
    ranking_type_hash
    beginweekdate = now - (dayofweek-1).days if dayofweek > 1
    beginweekdate = now - 7.days if dayofweek == 1
    type_cds = ["SCTP001", "SCTP002", "SCTP003", "SCTP004"] 
    type_cds.each do |school_type|
      week_schools = SelectedSchool.includes(user: :user_missions)
                                    .where('(user_missions.confirmed_at > ? and user_missions.confirmed_at < ?)', beginweekdate, now)
                                    .where('user_missions.results = 1')
                                    .where('type_cd = ?', school_type)
                                    .references(:user_missions)
                                    .group(:school_id)
                                    .order('COUNT(user_missions.id) desc')
                                    .count('user_missions.id')
                                    .first(100)
      
      school_ids = []
      if week_schools.present?
        week_schools.each do |item|
          school_ids.push(item[0])
        end
      end
      selected_counts = selected_count(school_ids)
      week_prizes = sub_prize_calculate(school_ids, school_type, beginweekdate)
      week_counts = record_wrap(week_prizes, week_schools, school_type, selected_counts)
      cumulative_ranking(dayofweek, beginweekdate, week_counts, "weekly_certi_count")
    end

    beginmonthdate = now - (dayofmonth -1).days if dayofmonth > 1
    beginmonthdate = now - 1.month if dayofmonth == 1 
    type_cds.each do |school_type|
      month_schools = SelectedSchool.includes(user: :user_missions)
                                .where('(user_missions.confirmed_at > ? and user_missions.confirmed_at < ?)', beginmonthdate, now)
                                .where('user_missions.results = 1')
                                .where('type_cd = ?', school_type)
                                .references(:user_missions)
                                .group(:school_id)
                                .order('COUNT(user_missions.id) desc')
                                .count('user_missions.id')
                                .first(100)

      school_ids = []
      if month_schools.present?
        month_schools.each do |item|
          school_ids.push(item[0])
        end
      end
      selected_counts = selected_count(school_ids)
      month_prizes = sub_prize_calculate(school_ids, school_type, beginmonthdate)
      month_counts = record_wrap(month_prizes, month_schools, school_type, selected_counts)
      cumulative_ranking(dayofmonth, beginmonthdate, month_counts, "monthly_certi_count")
    end
  end

  def reward_amount(dayofweek, dayofmonth)
    ranking_type_hash
    beginweekdate = now - (dayofweek-1).days if dayofweek > 1
    beginweekdate = now - 7.days if dayofweek == 1
    type_cds = ["SCTP001", "SCTP002", "SCTP003", "SCTP004"]
    type_cds.each do |school_type|
      week_schools = SelectedSchool.includes(user: :challenge_user_results)
                                  .where('type_cd = ?', school_type)
                                  .where('(challenge_user_results.created_at > ? and challenge_user_results.created_at < ?)', beginweekdate,now)
                                  .where('challenge_user_results.final_prize > 0 OR challenge_user_results.final_scholar > 0')
                                  .references(:challenge_user_results)
                                  .group(:school_id)
                                  .sum('challenge_user_results.final_prize + challenge_user_results.final_scholar')
                                  .first(100)
                                
      school_ids = []
      if week_schools.present?
        week_schools.each do |item|
          school_ids.push(item[0])
        end
      end
      selected_counts = selected_count(school_ids)
      week_certi = sub_certi_calculate(school_ids, school_type, beginweekdate)
      week_counts = record_wrap(week_certi, week_schools, school_type, selected_counts)
      cumulative_ranking(dayofweek, beginweekdate, week_counts, "weekly_reward_amount")   
    end

    beginmonthdate = now - (dayofmonth -1).days if dayofmonth > 1
    beginmonthdate = now - 1.month if dayofmonth == 1
    type_cds.each do |school_type|
      month_schools = SelectedSchool.includes(user: :challenge_user_results)
                                  .where('type_cd = ?', school_type)
                                  .where('(challenge_user_results.created_at > ? and challenge_user_results.created_at < ?)', beginmonthdate,now)
                                  .where('challenge_user_results.final_prize > 0 OR challenge_user_results.final_scholar > 0')
                                  .references(:challenge_user_results)
                                  .group(:school_id)
                                  .sum('challenge_user_results.final_prize + challenge_user_results.final_scholar')
                                  .first(100)

      school_ids = []
      if month_schools.present?
        month_schools.each do |item|
          school_ids.push(item[0])
        end
      end
      selected_counts = selected_count(school_ids)
      month_certi = sub_certi_calculate(school_ids, school_type, beginmonthdate)
      month_counts = record_wrap(month_certi, month_schools, school_type, selected_counts)
      cumulative_ranking(dayofmonth, beginmonthdate, month_counts,"monthly_reward_amount")
    end
end
end