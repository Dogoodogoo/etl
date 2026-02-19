import sys
import os
import traceback
from dotenv import load_dotenv

# 1. 경로 설정 및 환경 변수 강제 로드
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# main 실행 시점에서 .env를 로드하여 설정 누락 여부를 조기에 파악합니다.
load_dotenv()

def setup_directories():
    """
    ETL 프로세스에 필요한 폴더 구조(data/raw)를 자동으로 생성하고 파일 존재 여부를 확인합니다.
    """
    data_raw_path = os.path.join(BASE_DIR, 'data', 'raw')
    if not os.path.exists(data_raw_path):
        print(f"📂 [폴더 생성] 데이터 저장 경로가 없어 새로 생성합니다: {data_raw_path}")
        os.makedirs(data_raw_path, exist_ok=True)

    # 파일 존재 여부 점검
    files = [f for f in os.listdir(data_raw_path) if f.endswith(('.xlsx', '.csv'))]

    print("-" * 60)
    print(f"✅ [경로 확인] {data_raw_path}")
    if not files:
        print("⚠️  [파일 부재] 로컬 data/raw 폴더에 수집할 엑셀/CSV 파일이 없습니다.")
    else:
        print(f"📄 [파일 목록] 감지된 파일: {files}")
    print("-" * 60)

def check_env_vars():
    """
    핵심 환경 변수가 설정되어 있는지 점검합니다.
    세이핀님의 .env 상태를 기준으로 검증 로직을 강화했습니다.
    """
    required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'TRASH_BIN_URL']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    # 1. 필수 변수 누락 확인
    if missing_vars:
        print(f"⚠️  [설정 누락] .env 파일에서 다음 항목이 비어있습니다: {', '.join(missing_vars)}")

    # 2. API 키 누락 별도 확인 (pet_places 실행 시 필요)
    if not os.getenv('PUBLIC_DATA_API_KEY'):
        print("⚠️  [API 키 누락] PUBLIC_DATA_API_KEY가 없어 반려견 시설 수집이 불가능합니다.")

    # 3. URL 형식 확인 (리다이렉트 주소 방지)
    trash_url = os.getenv('TRASH_BIN_URL', '')
    if 'google.com' in trash_url:
        print("⚠️  https://namu.wiki/w/%EC%98%A4%EB%A5%98 TRASH_BIN_URL이 구글 리다이렉트 주소입니다. 직접 다운로드 링크로 수정이 필요합니다.")

    if not missing_vars and 'google.com' not in trash_url:
        print("✅ [설정 확인] .env 환경 변수 기본 로드 완료")

def main():
    print("="*60)
    print("🚀 DogooDogoo ETL 통합 시스템 진단 및 가동")
    print(f"📍 실행 경로: {os.getcwd()}")

    # 환경 및 폴더 점검
    check_env_vars()
    setup_directories()

    jobs = []

    # 2. 모듈 로드 (실패 시 상세 사유 출력)
    modules = [
        ('pet_places', 'scripts.pet_place_etl', 'PetPlaceETL'),
        ('trash_bins', 'scripts.trash_bin_etl', 'TrashBinETL'),
        ('drinking_fountains', 'scripts.water_fountain_etl', 'WaterFountainETL')
    ]

    for name, module_path, class_name in modules:
        try:
            mod = __import__(module_path, fromlist=[class_name])
            etl_class = getattr(mod, class_name)
            jobs.append(etl_class())
            print(f"✅ [로드 성공] {class_name}")
        except ImportError:
            print(f"❌ [로드 실패] {class_name} (파일이 없거나 경로가 잘못되었습니다.)")
        except Exception as e:
            print(f"❌ [오류 발생] {class_name} 초기화 중 에러: {e}")

    if not jobs:
        print("\n🚫 실행 가능한 ETL 작업이 없습니다. 설정을 다시 확인해 주세요.")
        return

    print(f"\n📊 총 {len(jobs)}개의 작업을 순차적으로 실행합니다.\n")

    for job in jobs:
        try:
            print(f"▶️  [{job.table_name}] 실행 중...")
            job.run()
        except Exception as e:
            print(f"\n❌ [{job.table_name}] 중단됨: {str(e)}")

    print("\n" + "="*60)
    print("🏁 모든 ETL 프로세스 종료")
    print("="*60)

if __name__ == "__main__":
    main()