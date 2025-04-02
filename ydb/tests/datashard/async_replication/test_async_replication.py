from ydb.tests.datashard.lib.base_async_replication import TestBase
import time


class TestAsyncReplication(TestBase):
    def test_t(self):
        self.query("create table a(pk int64, primary key(pk))")
        self.query("insert into a(pk) values(1)")
        self.query(
            "CREATE OBJECT TOKEN_SECRET_NAME (TYPE SECRET) WITH value='my_secret'")
        self.query_async(
            "CREATE OBJECT TOKEN_SECRET_NAME (TYPE SECRET) WITH value='my_secret'")
        cmd = self.query_async(f"""
                        CREATE ASYNC REPLICATION `replication` 
                        FOR `{self.get_database()}/a` AS `{self.get_database()}/a`
                        WITH (
                        CONNECTION_STRING = 'grpc://{self.get_endpoint()}/?database={self.get_database()}',
                        TOKEN_SECRET_NAME='my_secret'
                            )
                         """)
        print(cmd)
        time.sleep(1)
        rows = self.query_async("select count(*) as count from a")
        assert len(rows) == 1 and rows[0].count == 1, "cscdscs"
