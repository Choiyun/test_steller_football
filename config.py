import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()


# 데이터베이스 설정
DB_CONFIG = {
    'db_name': 'LIVESCORE_NEW',
    'user': 'chatgpt',
    'password': 'chatgpt11!#',
    'host': '118.220.173.14',
    'port': 1433,
    'db_type': 'mssql',
    'db_role': 'source'
}

TARGET_DB_CONFIG = {
    'db_name': 'steller',
    'user': 'postgres',
    'password': '0827',
    'host': '221.139.107.22',
    'port': 5432,
    'db_type': 'postgresql',
    'db_role': 'target'
}

###옵타 연동 테스트 용
OPTA_DB_CONFIG = {
    'db_name': 'LS_QUARRY',
    'user': 'chatgpt',
    'password': 'wlvlxl3^&',
    'host': '121.167.148.152',
    'port': 1433,
}
