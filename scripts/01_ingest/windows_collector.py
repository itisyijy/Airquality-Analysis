# -*- coding: utf-8 -*-
import requests
import pandas as pd
import datetime
import os
import time
import urllib.parse
import urllib3

# SSL 경고 메시지 숨기기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- [설정] ---
SERVICE_KEY = "a83872306c52a0336233127ea4391fa32713dab6954d8da8b0ca829a84dfe3d7"
SHARED_FOLDER_PATH = r"C:\AirKorea_Data"

# ---------------------------------------------------------
# 1. 측정소 주소 정보 가져오기 (Address Info)
# ---------------------------------------------------------
def get_station_address_map(service_key):
    print("🔍 전체 측정소 주소 정보를 로딩 중입니다...", end=" ")
    url = "https://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getMsrstnList"
    
    params = {
        "returnType": "json",
        "numOfRows": "2000",
        "pageNo": "1",
        "ver": "1.1"
    }
    
    param_str = "&".join([f"{key}={val}" for key, val in params.items()])
    full_url = f"{url}?serviceKey={service_key}&{param_str}"
    
    try:
        response = requests.get(full_url, verify=False, timeout=10)
        if response.status_code == 200:
            try:
                items = response.json()['response']['body']['items']
                # 주소 매핑 생성
                addr_map = {item['stationName']: item['addr'] for item in items if 'addr' in item}
                print(f"완료 ({len(addr_map)}개 측정소)")
                return addr_map
            except:
                return {}
        return {}
    except:
        return {}

# ---------------------------------------------------------
# 2. 메인 수집 및 변환 함수
# ---------------------------------------------------------
def collect_and_transform():
    print("=== AirKorea 데이터 수집 및 변환 시작 ===")
    
    # (1) 주소 정보 확보
    addr_map = get_station_address_map(SERVICE_KEY)
    
    base_url = "https://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
    sido_list = ['서울', '부산', '대구', '인천', '광주', '대전', '울산', '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주', '세종']
    
    today = datetime.date.today()
    first = today.replace(day=1)
    last = first - datetime.timedelta(days=1)
    year = last.year
    month = last.month
    
    all_data = []

    if not os.path.exists(SHARED_FOLDER_PATH):
        try:
            os.makedirs(SHARED_FOLDER_PATH)
        except:
            print(f"❌ 오류: 폴더 생성 실패: {SHARED_FOLDER_PATH}")
            return

    # (2) 데이터 수집
    for sido in sido_list:
        print(f"[{sido}] 데이터 요청 중...", end=" ")
        
        try:
            sido_encoded = urllib.parse.quote(sido)
            
            # [핵심 변경] ver=1.5로 변경하여 stationCode 포함
            url = (
                f"{base_url}"
                f"?serviceKey={SERVICE_KEY}"
                f"&returnType=json"
                f"&numOfRows=600"
                f"&pageNo=1"
                f"&sidoName={sido_encoded}"
                f"&ver=1.5" 
            )
            
            response = requests.get(url, verify=False, timeout=10)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    items = data['response']['body']['items']
                    if items:
                        df = pd.DataFrame(items)
                        all_data.append(df)
                        print(f"성공 ({len(items)}건)")
                    else:
                        print("데이터 없음")
                except Exception as e:
                    print(f"파싱 실패: {e}")
            else:
                print(f"HTTP 에러 {response.status_code}")
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"연결 실패: {e}")

    # (3) 데이터 병합 및 변환
    if all_data:
        raw_df = pd.concat(all_data, ignore_index=True)
        
        # [핵심 변경] stationCode 매핑 추가
        rename_map = {
            'sidoName': 'region',
            'stationName': 'station_name',
            'stationCode': 'station_code', # 1.5버전부터 제공됨
            'dataTime': 'date_time',
            'so2Value': 'SO2',
            'coValue': 'CO',
            'o3Value': 'O3',
            'no2Value': 'NO2',
            'pm10Value': 'PM10',
            'pm25Value': 'PM25'
        }
        raw_df.rename(columns=rename_map, inplace=True)
        
        # 주소 채우기
        raw_df['address'] = raw_df['station_name'].map(addr_map).fillna("")

        # 날짜 변환
        raw_df['date_time'] = raw_df['date_time'].astype(str).str.replace(r'[- :]', '', regex=True)
        raw_df['date_time'] = raw_df['date_time'].str.slice(0, 10)

        # 컬럼 순서 맞춤
        target_columns = [
            "region", "station_code", "station_name", "date_time",
            "SO2", "CO", "O3", "NO2", "PM10", "PM25", "address"
        ]
        
        # 없는 컬럼은 빈칸으로 채움 (reindex가 알아서 처리)
        final_df = raw_df.reindex(columns=target_columns)
        final_df.fillna("", inplace=True)

        # 저장
        filename = f"data_{year}_{month:02d}.csv"
        full_path = os.path.join(SHARED_FOLDER_PATH, filename)
        
        try:
            final_df.to_csv(full_path, index=False, encoding='utf-8-sig')
            print("========================================")
            print(f"✅ 수집 완료! (station_code, address 포함)")
            print(f"💾 저장 위치: {full_path}")
        except Exception as e:
            print(f"❌ 저장 오류: {e}")
        
    else:
        print("\n❌ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    collect_and_transform()