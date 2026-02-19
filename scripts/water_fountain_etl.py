
import os
import requests
import pandas as pd
from scripts.base_etl import BaseETL

class WaterFountainETL(BaseETL):
    """
    서울 열린데이터 광장의 '서울시 상수도본부 아리스 음수대 정보(TbViewGisArisu)' API를 이용한 수집 모듈입니다.
    """
    def __init__(self):
        super().__init__(table_name='drinking_fountains')
        self.api_key = os.getenv('SEOUL_API_KEY')
        # TbViewGisArisu API 엔드포인트
        self.endpoint = f"http://openapi.seoul.go.kr:8088/{self.api_key}/json/TbViewGisArisu/1/1000/"

    def extract(self):
        """
        서울시 아리스 음수대 API로부터 데이터를 추출합니다.
        """
        if not self.api_key:
            print("[WaterFountain] 오류: SEOUL_API_KEY가 .env에 설정되지 않았습니다.")
            return []

        try:
            masked_url = self.endpoint.replace(self.api_key, '********')
            print(f"[WaterFountain] 서울시 API 접속 시도: {masked_url}")

            response = requests.get(self.endpoint, timeout=20)
            response.raise_for_status()

            data = response.json()

            if 'TbViewGisArisu' in data:
                items = data.get('TbViewGisArisu', {}).get('row', [])
                return items
            else:
                err_msg = data.get('RESULT', {}).get('MESSAGE', '알 수 없는 응답 구조입니다.')
                print(f"[WaterFountain] API 호출 실패: {err_msg}")
                return []

        except Exception as e:
            print(f"[WaterFountain] API 추출 오류 발생: {e}")
            return []

    def transform(self, data):
        """
        로그에 출력된 실제 API 컬럼명(CN_PARK_NM, ROAD_NM_ADDR 등)을 기반으로 매핑합니다.
        """
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # 로그 분석 결과에 따른 실제 컬럼 매핑
        # CN_PARK_NM: 공원/시설명, ROAD_NM_ADDR: 도로명주소, YCRD: 위도, XCRD: 경도
        mapping = {
            'CN_PARK_NM': 'fountain_name',
            'ROAD_NM_ADDR': 'address',
            'YCRD': 'latitude',
            'XCRD': 'longitude'
        }

        # 1. 실제 데이터에 존재하는 컬럼만 선택
        existing_keys = [k for k in mapping.keys() if k in df.columns]
        df = df[existing_keys].rename(columns=mapping)

        # 2. 좌표 수치화
        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        # 3. 필수 정보가 없는 행 제거
        return df.dropna(subset=['fountain_name', 'address'])

    def load(self, df):
        """
        BaseETL의 load 메서드를 호출하여 TRUNCATE 후 적재를 수행합니다.
        """
        super().load(df)