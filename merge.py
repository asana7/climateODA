import pandas as pd
import os

# 연도 범위 설정
start_year = 2013
end_year = 2022


# 데이터를 병합할 리스트
merged_data = []

# 데이터 병합 및 필터링
for year in range(start_year, end_year + 1):
    file_name = f"res/CRS {year} data.txt"

    if not os.path.exists(file_name):
        print(f"파일 {file_name}이 존재하지 않습니다. 스킵합니다.")
        continue

    print(f"{year}년 데이터 읽는 중...")
    # 데이터 읽기
    df = pd.read_csv(file_name, delimiter = '|')

    # `climateadaptation`이 1 또는 2인 값만 남기기
    df_filtered = df[df['ClimateAdaptation'].isin([1, 2])]

    # 병합 리스트에 추가
    merged_data.append(df_filtered)

# 병합된 데이터프레임 생성
final_data = pd.concat(merged_data, ignore_index=True)

# 결과 저장
final_data.to_csv("filtered_climateadaptation_data.csv", index=False)
print("결과가 'filtered_climateadaptation_data.csv'로 저장되었습니다.")

