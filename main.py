# etl/main.py
import os
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from scripts.weather_utils import convert_to_grid

# [CS 지식: 시스템 환경 관리] 보안과 유연성을 위해 외부 설정 파일(.env)에서 환경 변수를 로드합니다.
load_dotenv()

def fetch_and_load_weather():
    """
    기상청 API 허브에서 데이터를 추출(Extract)하여 특정 좌표의 값을 파싱(Transform)한 뒤
    DB에 적재(Load)하는 ETL 프로세스입니다.
    """

    # 1. 위경도를 기상청 격자 좌표(nx, ny)로 변환 (예: 서울 시청)
    nx, ny = convert_to_grid(37.5665, 126.9780)

    # [CS 지식: 시간 데이터 동기화]
    # 기상청 초단기예보는 10분 단위로 생성됩니다.
    # 현재 시각에서 1시간을 빼고, 분(mm)을 '00'으로 고정하여 가장 안정적인 정시 데이터를 요청합니다.
    now = datetime.now() - timedelta(hours=1)
    tmfc = now.strftime('%Y%m%d%H') + "00"

    # 2. API 호출 설정
    url = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_vsrt_grd"
    params = {
        'tmfc': tmfc,                    # 필수: 발표 시간 (YYYYMMDDHHmm)
        'vars': 'T1H',                   # 예보 변수: 기온
        'authKey': os.getenv('WEATHER_API_KEY')
    }

    try:
        # [CS 지식: 네트워크 통신] 타임아웃을 설정하여 무한 대기로 인한 시스템 리소스 점유를 방지합니다.
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        # [CS 지식: 문자열 파싱 알고리즘]
        # 주석(#) 라인을 제외하고, 공백 및 콤마로 구분된 비정형 텍스트를 정제하여 1차원 리스트로 변환합니다.
        lines = response.text.strip().split('\n')
        numeric_data = []
        for line in lines:
            if line and not line.startswith('#'):
                # [CS 지식: 정규화] replace와 split을 연쇄 호출하여 데이터 포맷을 통일합니다.
                numeric_data.extend(line.replace(',', ' ').split())

        # 3. 데이터 인덱싱 및 검증
        # [CS 지식: 자료구조 - 2차원 배열의 선형 매핑(Row-Major Order)]
        # 2차원 격자(149x253)의 (nx, ny) 위치를 1차원 리스트의 인덱스로 변환하는 공식입니다.
        total_len = len(numeric_data)
        target_idx = (ny - 1) * 149 + (nx - 1)

        if total_len > 0 and target_idx < total_len:
            val = numeric_data[target_idx]

            # [CS 지식: 데이터 예외 처리] 기상청의 약속된 결측치(-99.00)를 논리적으로 필터링합니다.
            if val == "-99.00":
                print(f"[{datetime.now()}] 좌표 ({nx}, {ny})는 현재 데이터 준비 중이거나 결측 영역입니다.")
                return

            print(f"[{datetime.now()}] 데이터 수집 성공! 좌표({nx}, {ny}) 기온: {val}°C")

            # 4. DB 적재 로직 (PostgreSQL)
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            cur = conn.cursor()

            # [CS 지식: 데이터 무결성 - UPSERT 전략]
            # 중복 데이터 삽입을 방지하고 최신 정보로 갱신하는 멱등성(Idempotency) 확보 로직입니다.
            sql = """
                  INSERT INTO weather_forecast_cache (nx, ny, category, fcst_value, base_date, base_time)
                  VALUES (%s, %s, %s, %s, %s, %s)
                      ON CONFLICT (nx, ny, base_date, base_time, category)
                DO UPDATE SET fcst_value = EXCLUDED.fcst_value, updated_at = CURRENT_TIMESTAMP; \
                  """
            base_date = tmfc[:8]
            base_time = tmfc[8:]
            cur.execute(sql, (nx, ny, 'T1H', val, base_date, base_time))

            conn.commit()
            print("라떼서버에 데이터 적재 완료.")

        else:
            print(f"에러: 수집된 데이터 부족 (수집량: {total_len}, 필요 인덱스: {target_idx})")
            if total_len == 0:
                print(f"서버 응답 내용: {response.text[:100]}")

    except Exception as e:
        print(f"런타임 에러 발생: {e}")
    finally:
        if 'conn' in locals():
            cur.close()
            conn.close()

if __name__ == "__main__":
    fetch_and_load_weather()