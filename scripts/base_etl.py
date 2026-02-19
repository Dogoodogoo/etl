import os
import pandas as pd
import requests
import io
from sqlalchemy import create_engine, text
from abc import ABC, abstractmethod
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

class BaseETL(ABC):
    """
    모든 ETL 프로세스의 공통 기능을 정의하는 추상 베이스 클래스.
    데이터 누적 방지를 위한 TRUNCATE 기능을 추가.
    """
    def __init__(self, table_name):
        self.table_name = table_name
        self.db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        self.engine = create_engine(self.db_url)

    @abstractmethod
    def extract(self):
        """원천 데이터 추출 (API, 파일 등)"""
        pass

    @abstractmethod
    def transform(self, data):
        """데이터 정제 및 변환"""
        pass

    def load(self, df):
        """
        데이터를 DB에 적재합니다.
        적재 전 기존 데이터를 삭제(TRUNCATE)하여 중복 누적을 방지합니다.
        """
        if df is not None and not df.empty:
            try:
                # 1. 중복 방지를 위해 기존 테이블 데이터 삭제
                # RESTART IDENTITY는 일련번호를 1번부터 다시 시작하게 하며,
                # CASCADE는 외래키 관계가 있을 경우 함께 처리합니다.
                with self.engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {self.table_name} RESTART IDENTITY CASCADE"))
                    print(f"[{self.table_name}] 기존 데이터를 삭제하고 초기화하였습니다.")

                # 2. 신규 데이터 적재
                df.to_sql(self.table_name, self.engine, if_exists='append', index=False)

                # 3. PostGIS 공간 데이터(geom) 생성
                with self.engine.begin() as conn:
                    update_geom = text(f"""
                        UPDATE {self.table_name}
                        SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
                        WHERE geom IS NULL 
                          AND latitude IS NOT NULL 
                          AND longitude IS NOT NULL;
                    """)
                    conn.execute(update_geom)

                print(f"[{self.table_name}] 총 {len(df)}건의 데이터를 성공적으로 적재하고 공간 인덱스를 갱신했습니다.")
            except Exception as e:
                print(f"[{self.table_name}] 적재 에러 발생: {e}")
        else:
            print(f"[{self.table_name}] 처리할 데이터가 없어 적재를 건너뜁니다.")

    def run(self):
        """전체 ETL 사이클 실행"""
        print(f"[{self.table_name}] 프로세스 가동...")
        data = self.extract()
        df = self.transform(data)
        self.load(df)
        print(f"[{self.table_name}] 프로세스 종료.")