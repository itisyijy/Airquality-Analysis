# -*- coding: utf-8 -*-
import requests
import pandas as pd
import datetime
import os
import time
import urllib3

# SSL 인증서 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- [설정] ---
SERVICE_KEY = "a83872306c52a0336233127ea4391fa32713dab6954d8da8b0ca829a84dfe3d7"
SHARED_FOLDER_PATH = r"C:\AirKorea_Data"

# 공공데이터포털 URL 리스트 (전력거래량)
URL_LIST = [
    "https://api.odcloud.kr/api/15081098/v1/uddi:abe83228-5d0a-4a83-83b5-b290bbc8bfc0",
    "https://api.odcloud.kr/api/15081098/v1/uddi:3246a2ee-1c23-41a8-a9b7-f4f00b691449",
    "https://api.odcloud.kr/api/15081098/v1/uddi:98d70127-5458-46b0-b5b3-ecf04177784f",
    "https://api.odcloud.kr/api/15081098/v1/uddi:aee44e96-f600-4c89-bbbe-9cdcd31d62ef",
    "https://api.odcloud.kr/api/15081098/v1/uddi:bdc8807e-986b-4d0e-a191-e6bc9f464c6f"
]

def collect_and_transform_kepco_final():
    print("=== KEPCO 데이터 수집 시작 (날짜 버그 수정 버전) ===")
    
    if not os.path.exists(SHARED_FOLDER_PATH):
        try:
            os.makedirs(SHARED_FOLDER_PATH)
        except:
            return

    all_data = []

    for i, url in enumerate(URL_LIST):
        print(f"\n[{i+1}/{len(URL_LIST)}] URL 데이터 수집 시작...")
        base_url = url.split('?')[0]
        page = 1
        
        while True:
            params = { "page": page, "perPage": 5000, "serviceKey": SERVICE_KEY }
            
            try:
                response = requests.get(base_url, params=params, verify=False, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    items = data.get('data', [])
                    
                    if not items:
                        print(f"  └ 페이지 {page}: 데이터 없음 (다음 URL로 이동)")
                        break
                        
                    df = pd.DataFrame(items)
                    cols = df.columns
                    
                    rename_map = {}
                    
                    # [핵심 수정] API마다 제각각인 날짜 컬럼을 모두 'date_time'으로 통일
                    # 이전에는 여기서 '시간' 컬럼을 잘못 가져와서 데이터가 사라졌습니다.
                    if '거래일' in cols: rename_map['거래일'] = 'date_time'
                    elif '거래일자' in cols: rename_map['거래일자'] = 'date_time'
                    elif '거래일시' in cols: rename_map['거래일시'] = 'date_time'
                    elif '기간' in cols: rename_map['기간'] = 'date_time'
                    
                    # 발전량
                    if '전력거래량' in cols: rename_map['전력거래량'] = 'power_value'
                    elif '전력거래량(MWh)' in cols: rename_map['전력거래량(MWh)'] = 'power_value'
                    
                    # 연료원
                    if '연료원' in cols: rename_map['연료원'] = 'fuel_type'
                    
                    df.rename(columns=rename_map, inplace=True)
                    
                    # 필수 데이터가 있는 행만 가져오기
                    if 'date_time' in df.columns and 'power_value' in df.columns and 'fuel_type' in df.columns:
                        # 숫자 변환 (에러 발생 시 0으로 처리)
                        df['power_value'] = pd.to_numeric(df['power_value'], errors='coerce').fillna(0)
                        
                        # 필요한 컬럼만 선택
                        df = df[['date_time', 'fuel_type', 'power_value']]
                        all_data.append(df)
                        
                        if page % 10 == 0:
                            print(f"  └ {page}페이지 수집 중... (누적 {len(all_data)*5000}건 예상)")
                    
                    page += 1
                    time.sleep(0.05) # 서버 부하 방지
                    
                else:
                    print(f"  └ 통신 에러 ({response.status_code})")
                    break
                    
            except Exception as e:
                print(f"  └ 연결 실패: {e}")
                break

    if all_data:
        print("\n🔄 전체 데이터 병합 중...")
        raw_df = pd.concat(all_data, ignore_index=True)
        print(f"📊 총 수집된 행 개수: {len(raw_df)}행")
        
        # [날짜 정리] YYYY-MM-DD 또는 YYYYMMDD 형식을 YYYYMM으로 변환
        raw_df['date_time'] = raw_df['date_time'].astype(str).str.replace(r'[- :]', '', regex=True)
        
        # 날짜 길이가 6자리 이상인 것만 남김 (이상한 데이터 제거)
        raw_df = raw_df[raw_df['date_time'].str.len() >= 6]

        try:
            raw_df['year'] = raw_df['date_time'].str[:4]
            raw_df['month'] = raw_df['date_time'].str[4:6].astype(int)
        except Exception as e:
            print(f"❌ 날짜 변환 오류: {e}")
            return

        # [화력 발전 필터링]
        target_fuels = ['무연탄', '유연탄', '중유', 'LNG', '유류', '가스', '석탄', '석유']
        condition = raw_df['fuel_type'].str.contains('|'.join(target_fuels), na=False)
        filtered_df = raw_df[condition]
        
        print(f"🔥 화력 발전 필터링: {len(filtered_df)}행 (전체 데이터 중)")

        if not filtered_df.empty:
            # [통계 생성] 월별 합계 (Sum)
            # 시간당 발전량을 모두 더해서 '월간 총 발전량'을 만듭니다.
            pivot_df = filtered_df.pivot_table(
                index='month', 
                columns='year', 
                values='power_value', 
                aggfunc='sum'
            )

            filename = "kepco_thermal_power_final.xlsx"
            full_path = os.path.join(SHARED_FOLDER_PATH, filename)
            
            try:
                pivot_df.to_excel(full_path)
                print(f"\n✅ 최종 성공! 파일 저장 완료: {full_path}")
                print("이제 엑셀 파일을 열어보시면 2023, 2024년 데이터가 정상적으로 보일 것입니다!")
            except:
                csv_path = full_path.replace('.xlsx', '.csv')
                pivot_df.to_csv(csv_path, encoding='utf-8-sig')
                print(f"\n✅ (엑셀 대신 CSV 저장) 파일 저장 완료: {csv_path}")
        else:
            print("❌ 필터링 후 데이터가 없습니다. (연료원 이름 확인 필요)")

    else:
        print("❌ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    collect_and_transform_kepco_final()