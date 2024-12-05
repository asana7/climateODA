import pandas as pd
import os

# 연도 범위
start_year = 2013
end_year = 2022

# 결과를 저장할 리스트
all_data = []


# 데이터 처리 함수
def process_yearly_data(file_path, year):
    # 데이터 읽기
    df = pd.read_csv(file_path, delimiter = '|')

    # 그룹 플래그 생성
    df['group_flag'] = None
    df.loc[(df['ClimateMitigation'].isin([1, 2])) & (
                (df['ClimateAdaptation'] == 0) | (df['ClimateAdaptation'].isna())), 'group_flag'] = 'mitigation_only'
    df.loc[(df['ClimateAdaptation'].isin([1, 2])) & (
                (df['ClimateMitigation'] == 0) | (df['ClimateMitigation'].isna())), 'group_flag'] = 'adaptation_only'
    df.loc[(df['ClimateMitigation'].isin([1, 2])) & (df['ClimateAdaptation'].isin([1, 2])), 'group_flag'] = 'both'
    df.loc[((df['ClimateMitigation'] == 0) | (df['ClimateMitigation'].isna())) & (
                (df['ClimateAdaptation'] == 0) | (df['ClimateAdaptation'].isna())), 'group_flag'] = 'neither'

    # 전체 합계 계산
    total_sum = df.groupby('RecipientName')['USD_Disbursement_Defl'].sum().reset_index()
    total_sum.rename(columns={'USD_Disbursement_Defl': 'total_USD_Disbursement_Defl'}, inplace=True)

    # 그룹별 합계 계산
    group_sum = df.groupby(['RecipientName', 'group_flag'])['USD_Disbursement_Defl'].sum().unstack(
        fill_value=0).reset_index()

    # 연도 추가
    total_sum['year'] = year
    group_sum['year'] = year

    # 병합
    merged = pd.merge(total_sum, group_sum, on=['RecipientName', 'year'], how='outer')
    return merged


# 연도별 데이터 처리
for year in range(start_year, end_year + 1):
    file_name = f"res/CRS {year} data.txt"

    if not os.path.exists(file_name):
        print(f"파일 {file_name}이 존재하지 않습니다. 스킵합니다.")
        continue

    print(f"{year}년 데이터 처리 중...")
    yearly_data = process_yearly_data(file_name, year)
    all_data.append(yearly_data)

# 결과를 하나의 데이터프레임으로 병합
final_data = pd.concat(all_data, ignore_index=True)

# 결과 저장
final_data.to_csv("aggregated_data.csv", index=False)
print("결과가 'aggregated_data.csv'로 저장되었습니다.")

