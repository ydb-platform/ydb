
from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.sql.lib.test_s3 import S3Base
import os



class TestYdbS3TTL(TestBase, S3Base):
    def test_S3(self):
        s3_client = self.s3_session_client()

        bucket_name = self.bucket_name()
        s3_client.create_bucket(Bucket=self.bucket_name())

        self.create_external_datasource_and_secrets(bucket_name)
        
        self.query(f"""create table a(
                pk Int64,
                primary key(pk)
            )""")
        for i in range(100):
            self.query(f"insert into a(pk) values({i})")
        os.system(f"""ydb -p quickstart export s3 \
                  --s3-endpoint {self.s3_endpoint} \
                  --access-key {self.s3_access_key}  \
                  --secret-key {self.s3_secret_access_key} \
                  --item src=.,dst=.
                  """)
        self.query("drop table a")
        
        os.system(f"""ydb -p quickstart import s3 \
                  --s3-endpoint {self.s3_endpoint} \
                  --access-key {self.s3_access_key}  \
                  --secret-key {self.s3_secret_access_key} \
                  --item src=.,dst=.
                  """)
        rows = self.query("select count(*) as count from a")
        assert len(
                rows) == 1 and rows[0].count == 100, "sad"
        