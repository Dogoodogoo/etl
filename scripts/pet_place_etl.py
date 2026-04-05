import requests
import pandas as pd
import os
from scripts.base_etl import BaseETL

class PetPlaceETL(BaseETL):
    """
    한국관광공사 반려동물 동반여행 서비스 API(KorPetTourService2)를 이용한
    반려견 동반 가능 시설 수집 모듈입니다.
    """
    def __init__(self):
        super().__init__(table_name='pet_places')
        self.api_key = os.getenv('PUBLIC_DATA_API_KEY')
        # 사용자가 지정한 KorPetTourService2 기반의 지역기반 목록 조회 엔드포인트
        self.endpoint = "https://apis.data.go.kr/B551011/KorPetTourService2/areaBasedList2"

    def extract(self):
        """
        KorPetTourService2 API 규격에 맞는 파라미터를 사용하여 데이터를 추출합니다.
        """
        if not self.api_key:
            print("[PetPlace] 오류: PUBLIC_DATA_API_KEY가 .env에 설정되지 않았습니다.")
            return []

        params = {
            'serviceKey': self.api_key,
            'pageNo': 1,
            'numOfRows': 500,
            'MobileOS': 'ETC',
            'MobileApp': 'DogooDogoo',
            '_type': 'json',
            'arrange': 'A' # 제목순 정렬
        }

        try:
            print(f"[PetPlace] API 접속 시도: {self.endpoint}")
            response = requests.get(self.endpoint, params=params, timeout=20)
            response.raise_for_status()

            data = response.json()
            items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])

            if not items:
                print(f"[PetPlace] 경고: API 응답에 데이터가 없습니다. (결과 메시지: {data.get('response', {}).get('header', {}).get('resultMsg')})")

            return items
        except Exception as e:
            print(f"[PetPlace] API 추출 중 치명적 오류: {e}")
            return []

    def transform(self, data):
        """
        KorPetTourService2의 응답 필드를 DB 스키마에 매핑하고 데이터 길이를 정제합니다.
        """
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # KorPetTourService2 API의 주요 필드 매핑
        mapping = {
            'title': 'facility_name',   # 시설명
            'addr1': 'address',         # 주소
            'mapx': 'longitude',        # 경도
            'mapy': 'latitude',         # 위도
            'tel': 'tel',               # 전화번호
            'cat1': 'category'          # 카테고리
        }

        # 실제 데이터에 존재하는 컬럼만 필터링하여 매핑
        existing_keys = [k for k in mapping.keys() if k in df.columns]
        df = df[existing_keys].rename(columns=mapping)

        # [중요] 데이터 길이 제한 처리 (DB VARCHAR(50) 초과 에러 방지)
        # 50자 제한인 컬럼들은 안전하게 49자로 자릅니다.
        if 'tel' in df.columns:
            df['tel'] = df['tel'].astype(str).str[:49].replace('nan', '')

        if 'category' in df.columns:
            df['category'] = df['category'].astype(str).str[:49].replace('nan', '')

        if 'facility_name' in df.columns:
            df['facility_name'] = df['facility_name'].astype(str).str[:254]

        # 데이터 타입 변환 (문자열 위경도를 숫자로)
        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        # 필수 정보(시설명, 주소)가 없는 행 제거
        df = df.dropna(subset=['facility_name', 'address'])

        # 위경도가 유효한 범위 내에 있는 데이터만 유지
        df = df[(df['latitude'] > 30) & (df['latitude'] < 45)]
        df = df[(df['longitude'] > 120) & (df['longitude'] < 135)]

        print(f"[PetPlace] 변환 완료: {len(df)}건의 유효 데이터를 확보했습니다.")
        return df