import ydb
import sys

driver_config = ydb.DriverConfig(
    'grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135', '/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8',
    credentials=ydb.credentials_from_env_variables(),
    root_certificates=ydb.load_ydb_root_certificate(),
)
with ydb.Driver(driver_config) as driver:
    try:
        driver.wait(timeout=3)
        print('SUCCESS!', file=sys.stderr)
    except TimeoutError:
        print("Connect failed to YDB")
        print(driver.discovery_debug_details(), file=sys.stderr)
        print('FAIL!', file=sys.stderr)
