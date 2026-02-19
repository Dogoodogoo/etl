import os
import pandas as pd
import requests
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from scripts.base_etl import BaseETL

class TrashBinETL(BaseETL):
    """
    NCP Geocoding API를 사용하여 가로휴지통 데이터를 수집하는 모듈입니다.
    좌표 확보 실패 시에도 데이터를 누락시키지 않고 전량 DB에 적재합니다.
    """
    def __init__(self):
        super().__init__(table_name='trash_bins')
        self.raw_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
        self.log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'logs')

        # NCP 인증 정보 (Geocoding 전용)
        self.naver_client_id = (os.getenv('NAVER_CLIENT_ID') or "").strip().strip("'").strip('"')
        self.naver_client_secret = (os.getenv('NAVER_CLIENT_SECRET') or "").strip().strip("'").strip('"')
        self.geocoding_url = "https://maps.apigw.ntruss.com/map-geocode/v2/geocode"

        self.session = requests.Session()
        if self.naver_client_id and self.naver_client_secret:
            self.session.headers.update({
                "X-NCP-APIGW-API-KEY-ID": self.naver_client_id,
                "X-NCP-APIGW-API-KEY": self.naver_client_secret,
                "Accept": "application/json"
            })

        self.auth_failed = False

    def clean_address(self, text):
        """주소 오타 교정 및 정제 로직"""
        if not text or pd.isna(text):
            return ""
        text = str(text).strip()

        # 오타 자동 교정 (청게천 -> 청계천)
        text = text.replace('청게천', '청계천')
        text = text.replace('을지로지하', '을지로 지하')

        # 괄호 및 불필요한 특수문자 제거
        text = re.sub(r'\(.*?\)', '', text)
        text = re.sub(r'[^a-zA-Z0-9가-힣\s\-\,]', '', text)
        return ' '.join(text.split()).strip()

    def call_naver_api(self, query):
        """네이버 Geocoding API 호출 내부 메서드"""
        if not query or len(query) < 2:
            return None, None

        try:
            response = self.session.get(self.geocoding_url, params={"query": query}, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'OK' and data.get('addresses'):
                    target = data['addresses'][0]
                    return float(target['y']), float(target['x'])
            elif response.status_code in [401, 403]:
                self.auth_failed = True
            return None, None
        except Exception:
            return None, None

    def get_coordinates(self, city_name, address, location_desc):
        """다단계 NCP Geocoding 전략"""
        if self.auth_failed or not self.naver_client_id:
            return None, None

        clean_addr = self.clean_address(address)

        # 1. 서울특별시 + 자치구명 + 도로명 주소 결합 검색
        full_query = f"서울특별시 {city_name} {clean_addr}"
        lat, lng = self.call_naver_api(full_query)
        if lat: return lat, lng

        # 2. '지하' 키워드 대응
        if '지하' in clean_addr:
            retry_query = f"서울특별시 {city_name} {clean_addr.replace('지하', '').strip()}"
            lat, lng = self.call_naver_api(retry_query)
            if lat: return lat, lng

        # 3. 상세 위치 기반 지하철역 Fallback (NCP Geocoding 시도)
        combined_text = f"{address} {location_desc}"
        if '역' in combined_text:
            match = re.search(r'([가-힣]+역)', combined_text)
            if match:
                station_query = f"서울특별시 {city_name} {match.group(1)}"
                lat, lng = self.call_naver_api(station_query)
                if lat: return lat, lng

        return None, None

    def extract(self):
        """최신 엑셀 파일 로드 (header=4 적용)"""
        if not os.path.exists(self.raw_path):
            return None

        files = [f for f in os.listdir(self.raw_path) if f.endswith('.xlsx')]
        if not files:
            return None

        latest_file = os.path.join(self.raw_path, max(files, key=lambda x: os.path.getctime(os.path.join(self.raw_path, x))))
        print(f"[TrashBin] 데이터 추출 중: {latest_file}")

        try:
            return pd.read_excel(latest_file, header=4)
        except Exception as e:
            print(f"[TrashBin] 파일 로드 실패: {e}")
            return None

    def transform(self, df):
        """데이터 정제 및 20개 워커 기반 병렬 지오코딩 수행"""
        if df is None or df.empty:
            return pd.DataFrame()

        df.columns = df.columns.astype(str).str.strip()
        mapping = {
            '자치구명': 'city_name',
            '설치위치(도로명 주소)': 'address',
            '세부 위치': 'location_desc',
            '수거 쓰레기 종류': 'bin_type',
            '설치 장소 유형': 'bin_place_type'
        }

        df = df[[c for c in mapping.keys() if c in df.columns]].rename(columns=mapping)
        df['city_name'] = df['city_name'].astype(str).str.strip()
        df['address'] = df['address'].astype(str).str.strip()
        df['location_desc'] = df['location_desc'].astype(str).str.strip()

        df = df[~df['address'].isin(['nan', '', 'None'])].dropna(subset=['address'])

        workers = 20
        print(f"[TrashBin] 지오코딩 시작 (Target: {len(df)}건, Workers: {workers})...")
        start_time = time.time()

        cities = df['city_name'].tolist()
        addresses = df['address'].tolist()
        descriptions = df['location_desc'].tolist()
        results = [None] * len(addresses)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_index = {
                executor.submit(self.get_coordinates, city, addr, desc): i
                for i, (city, addr, desc) in enumerate(zip(cities, addresses, descriptions))
            }

            completed = 0
            for future in as_completed(future_to_index):
                if self.auth_failed:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                index = future_to_index[future]
                results[index] = future.result()
                completed += 1
                if completed % 1000 == 0 or completed == len(df):
                    print(f"[TrashBin] 진행률: {completed}/{len(df)} ({time.time()-start_time:.2f}초)")

        df['latitude'] = [r[0] if r else None for r in results[:len(df)]]
        df['longitude'] = [r[1] if r else None for r in results[:len(df)]]

        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        success_count = df['latitude'].notna().sum()
        fail_count = len(df) - success_count

        # 실패 데이터 별도 로깅 (추후 고도화 시 활용)
        if fail_count > 0:
            os.makedirs(self.log_path, exist_ok=True)
            fail_df = df[df['latitude'].isna()]
            log_file = os.path.join(self.log_path, f"geocoding_fail_{time.strftime('%Y%m%d')}.csv")
            fail_df.to_csv(log_file, index=False, encoding='utf-8-sig')
            print(f"[TrashBin] ⚠️ 좌표 확보 실패 {fail_count}건 발생. 로그 저장됨: {log_file}")

        print(f"[TrashBin] 지오코딩 종료. 성공: {success_count}건 / 전체 {len(df)}건 적재 진행")

        return df

    def load(self, df):
        """정제된 데이터를 DB에 적재합니다."""
        if df is None or df.empty:
            return
        super().load(df)