module Api
    module V1
        class RankingSchoolsController < BaseApiController
            before_action :authenticate_user_from_token!

            swagger_controller :ranking_schools, 'School Ranking'

            def self.add_common_params(api)
                api.param :header, 'uid', :string, :required, 'conects id'
                api.param :header, 'access-token', :string, :required, 'Authentication token'
                api.param :header, 'token-type', :string, :required, 'Bearer'
            end

            def self.add_common_response(api)
                api.response :forbidden
                api.response :not_found
                api.response :not_acceptable
            end

            swagger_api :ranking_list do |api|
                summary 'Top School Ranking List'
                Api::V1::UsersRankingController::add_common_params(api)
                param :query, 'ranking_type', :string, :required, 'ranking_type parameter 값은 다음과 같은 string 으로 입력한다.
                "weeklycerti" => 주간퀘스트 인증 기준, "monthlycerti"=>월간 퀘스트 인증 기준, "weeklyreward"=>주간 누적 상금 기준, "monthlyreward"=>월간 누적 상금 기준'
                param :query, 'school_type', :string, :required, 'school_type parameter 값은 다음과 같은 string으로 입력한다. 
                "SCTP001"=> 초등학교, "SCTP002"=> 중학교, "SCTP003"=>고등학교, "SCTP004"=> 대학교'
                param :query, 'present_diff', :integer, :optional, '0이면 이번주/이번달, n이면 n주 or n달 전'
                param :query, 'per_page', :integer, :optional, 'per_page'
                param :query, 'page', :integer, :optional, 'page'
                Api::V1::UsersRankingController::add_common_response(api)
            end

            swagger_api :my_ranking do |api|
                summary 'My School Ranking'
                Api::V1::UsersRankingController::add_common_params(api)
                param :query, 'ranking_type', :string, :required, 'ranking_type parameter 값은 다음과 같은 string 으로 입력한다.
                "weeklycerti" => 주간퀘스트 인증 기준, "monthlycerti"=>월간 퀘스트 인증 기준, "weeklyreward"=>주간 누적 상금 기준, "monthlyreward"=>월간 누적 상금 기준'
                param :query, 'present_diff', :integer, :optional, '0이면 이번주/이번달, n이면 n주 or n달 전'
                Api::V1::UsersRankingController::add_common_response(api)
            end

            def now
                @now = Time.current
            end

            def ranking_type_hash
                @ranking_type = Hash.new.update("weeklycerti"=> 1, "monthlycerti"=> 2, "weeklyreward"=>3, "monthlyreward"=>4 )
            end

            def time_hash(present_diff)
                thisweekday = now.strftime('%u').to_i
                thisdayofmonth = now.strftime('%d').to_i
                beginweekdate = (now - (thisweekday-1+(present_diff*7)).days).midnight if thisweekday > 1
                beginweekdate = (now - (7+(present_diff*7)).days).midnight if thisweekday == 1
                previousweekdate = beginweekdate - 7.days
                endweekdate = present_diff == 0 ? now.midnight : (beginweekdate + 7.days)
                beginmonthdate = (now - (thisdayofmonth-1).days - (present_diff).month).midnight if thisdayofmonth > 1
                beginmonthdate = (now - (1+present_diff).month).midnight if thisdayofmonth == 1
                previousmonthdate = beginmonthdate - 1.month 
                endmonthdate = present_diff == 0 ? now.midnight : (beginmonthdate + 1.month)
                @time_hash = Hash.new.update( 1 => [beginweekdate, endweekdate, previousweekdate],
                                              2=> [beginmonthdate, endmonthdate, previousmonthdate], 
                                              3 => [beginweekdate, endweekdate, previousweekdate], 
                                              4 => [beginmonthdate, endmonthdate, previousmonthdate])
            end

            def convert_score(ranking_type, item)
                certi, reward = 0, 0
                if ranking_type == 1 || ranking_type == 2 # 인증횟수 기준 랭킹인 경우 
                    certi = item[:total_score] if item[:total_score].present?
                    reward = item[:sub_score] if item[:sub_score].present?
                elsif ranking_type == 3 || ranking_type == 4 
                    certi = item[:sub_score] if item[:sub_score].present?
                    reward = item[:total_score] if item[:total_score].present?
                end
                return certi, reward
            end

            def ranking_list
                ranking_type_hash
                ranking_type = @ranking_type[params[:ranking_type].strip].to_i
                school_type = params[:school_type].strip
                page = params[:page].present? ? params[:page].to_i : 1
                per_page = params[:per_page].present? ? params[:per_page].to_i : 10
                present_diff = params[:present_diff].present? ? params[:present_diff].to_i : 0

                endtime = time_hash(present_diff)[ranking_type][1]

                ranking_schools = RankingSchool.where(ranking_type: ranking_type).where(type_cd: school_type)
                                            .where('(ranking_schools.created_at >= ? and ranking_schools.created_at <= ?)',endtime, endtime+10.hours).order(ranking: :asc)
                                            .includes(school: :selected_school)
                                            .group(:school_id)
                                            .page(page).per(per_page)
                                            
                ranked_school_json = []
                if ranking_schools.present?
                    ranking_schools.each do |item|
                        count = 0
                        if item.school.present?
                            if item.school.selected_school.present?
                                count = item.school.selected_school.length
                            end
                            certi, reward = convert_score(ranking_type, item)
                            ranked_school_json.push({
                                ranking: item.ranking,
                                school_id: item.school_id,
                                certi: certi.to_i,
                                reward: reward.to_i,
                                name: item.school.name,
                                logo: item.school.logo_images,
                                selected_school_count: count,
                                rank_diff: item.rank_diff,
                            })
                        end
                    end
                end
                render json: {result: ranked_school_json, meta: get_page_info(ranking_schools)}
            end

            def score_calculate(school_id, ranking_type, begintime, endtime)
                score = 0 
                if ranking_type == 1 || ranking_type == 2 #유저 인증 횟수 기준 랭킹(1=>weekly, 2=>monthly)
                    school_record = SelectedSchool.where(school_id: school_id)
                                                .where('(user_missions.confirmed_at > ? and user_missions.confirmed_at < ?)', begintime, endtime)
                                                .where('user_missions.results = 1')
                                                .references(:user_missions)
                                                .includes(user: :user_missions)
                                                .group(:school_id)
                                                .count('user_missions.id')
                    
                    if school_record.present?
                        score = school_record[school_id].to_i
                    end
                elsif ranking_type == 3 || ranking_type == 4 #유저 상금 누적액 기준 랭킹(3=>weekly, 4=>monthly)
                    school_record = SelectedSchool.where(school_id: school_id)
                                                .where('(challenge_user_results.created_at > ? and challenge_user_results.created_at < ?)', begintime, endtime)
                                                .references(:challenge_user_results)
                                                .includes(user: :challenge_user_results)
                                                .group(:school_id)
                                                .sum('challenge_user_results.final_prize + challenge_user_results.final_scholar')

                    if school_record.present?
                        score = school_record[school_id].to_i                        
                    end
                end
                return score
            end

            def check_within_rank(score, time, ranking_type, school_type)
                flag = false
                @last_ranking = nil
                last_ranking = RankingSchool.where(ranking_type: ranking_type).where(type_cd: school_type)
                                    .where('(ranking_schools.created_at >= ? and ranking_schools.created_at <= ?)',time, time+10.hours)
                                    .order(ranking: :asc)
                                    .last
                if last_ranking.present?
                    if score == last_ranking.total_score
                        flag = true
                        @last_ranking = last_ranking.ranking
                    end
                end
                return flag
            end

            def previous_ranking(school_id, time, ranking_type, school_type)
                #지난주에 나의 랭킹이 있는지 체크하여 계산
                previous_ranking = nil
                previous_record = RankingSchool.where(ranking_type: ranking_type)
                                                .where(type_cd: school_type)
                                                .where(school_id: school_id)
                                                .where('(ranking_schools.created_at >= ? and ranking_schools.created_at <= ?)',time, time+10.hours)
                #지난주에 나의 랭킹이 없는 경우 랭크 체크로 확인하여 랭킹을 계산 
                if previous_record[0].present?
                    previous_ranking = previous_record[0].ranking
                    return previous_ranking
                end
                return previous_ranking
            end

            def my_ranking
                ranking_type_hash
                user = current_user
                my_school = SelectedSchool.where(user_id: user.id).includes(:school)
                ranking_type = @ranking_type[params[:ranking_type].strip].to_i
                present_diff = params[:present_diff].present? ? params[:present_diff].to_i : 0
                endtime = time_hash(present_diff)[ranking_type][1]
                begintime = time_hash(present_diff)[ranking_type][0]
                previoustime = time_hash(present_diff)[ranking_type][2]
                ranked_school_json = {}
                if my_school.present?
                    school_type = nil
                    school_type = my_school[0].type_cd if my_school[0].type_cd.present?
                    school_id = my_school[0].school_id
                    my_ranking = RankingSchool.where(ranking_type: ranking_type).where(type_cd: school_type)
                                            .where(school_id: school_id)
                                            .where('(ranking_schools.created_at >= ? and ranking_schools.created_at <= ?)',endtime, endtime+10.hours)
                                            .includes(school: :selected_school)
                    
                    if my_ranking.present?
                        if my_ranking[0].school.present?
                            certi, reward = convert_score(ranking_type, my_ranking[0])
                            ranked_school_json = {
                                ranking: my_ranking[0].ranking,
                                school_id: my_ranking[0].school_id,
                                certi: certi.to_i,
                                reward: reward.to_i,
                                name: my_ranking[0].school.name,
                                logo: my_ranking[0].school.logo_images,
                                selected_school_count: my_ranking[0].school.selected_school.length,
                                rank_diff: my_ranking[0].rank_diff,
                            }
                        end
                    else
                        rankoutschool = School.where(id: school_id).includes(:selected_school)
                        if rankoutschool.present?
                            sub_score = 0
                            total_score = 0
                            total_score = score_calculate(school_id, ranking_type, begintime, endtime)
                            # 서브 스코어는 필터를 반대 기준으로 계산.(ex. 인증 기준이면 -> 상금 기준으로 계산, 상금 기준 -> 인증 기준으로 계산)
                            sub_score = score_calculate(school_id, ranking_type + 2, begintime, endtime) if (ranking_type == 1 || ranking_type == 2)
                            sub_score = score_calculate(school_id, ranking_type - 2, begintime, endtime) if (ranking_type == 3 || ranking_type == 4)
                            count = 0
                            if rankoutschool[0].selected_school.present?
                                count = rankoutschool[0].selected_school.length
                            end
                            ranking = nil
                            rank_diff = 999
                            if check_within_rank(total_score, endtime, ranking_type, school_type) == true # Check if out of top 100 User's school has the same score of 100th Rank.
                                ranking = @last_ranking if @last_ranking.present?
                                # 지난주/지난달과의 랭킹 변동 계산(1. 지난주 랭킹에 속해있는지 먼저 체크(+지난주 랭킹에 속해있지 않으면 지난주/달의 내 점수와 꼴지의 점수를 비교하여 검증), 2. 지난주 랭킹에 속해있지 않으면 현재 랭킹만 출력)
                                previous_ranking = previous_ranking(school_id, begintime, ranking_type, school_type) if ranking.present?
                                rank_diff = (previous_ranking - ranking) if (ranking.present? && previous_ranking.present?)
                                unless previous_ranking.present?
                                    last_score = score_calculate(school_id, ranking_type, previoustime, begintime)
                                    if check_within_rank(last_score, begintime, ranking_type, school_type) == true
                                        previous_ranking = @last_ranking if @last_ranking.present?
                                        rank_diff = (previous_ranking - ranking) if (ranking.present? && previous_ranking.present?)
                                    end
                                end
                            end
                            certi, reward = total_score, sub_score if (ranking_type == 1 || ranking_type == 2)
                            certi, reward = sub_score, total_score if (ranking_type == 3 || ranking_type == 4)
                            ranked_school_json = {
                                ranking: ranking,
                                school_id: school_id,
                                certi: certi.to_i,
                                reward: reward.to_i,
                                name: rankoutschool[0].name,
                                logo: rankoutschool[0].logo_images,
                                selected_school_count: count,
                                rank_diff: rank_diff,
                            }    
                        end
                    end
                end
               render json: {result: ranked_school_json, meta: meta_status} 
            end
        end
    end
end