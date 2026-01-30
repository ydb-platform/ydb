#include "kqp_sink_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpSinkMvcc) {
    class TSnapshotExpiration : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [1u;["One"]];
                [4000000001u;["BigOne"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));

            auto tx = result.GetTransaction();

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "ChangedOne");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto deadline = TInstant::Now() + TDuration::Seconds(30);
            auto caught = false;
            do {
                Sleep(TDuration::Seconds(1));
                auto result = session1.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/KV` WHERE Key = 1u OR Key = 4000000001u;
                )"), TTxControl::Tx(*tx)).ExtractValueSync();
                if (result.GetStatus() == EStatus::SUCCESS)
                    continue;

                UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR,
                    [](const auto& issue){
                        return issue.GetMessage().contains("has no snapshot at");
                    }), result.GetIssues().ToString());

                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ABORTED);

                caught = true;
                break;
            } while (TInstant::Now() < deadline);
            UNIT_ASSERT_C(caught, "Failed to wait for snapshot expiration.");
        }
    };

    Y_UNIT_TEST(SnapshotExpiration) {
        TSnapshotExpiration tester;
        tester.SetFastSnapshotExpiration(true);
        tester.SetIsOlap(false);
        tester.Execute();
    }

//     Y_UNIT_TEST(OlapSnapshotExpiration) {
//         TSnapshotExpiration tester;
//         tester.SetFastSnapshotExpiration(true);
//         tester.SetIsOlap(true);
//         tester.Execute();
//     }

    class TTxReadsCommitted : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
    
            // tx1 writes (1, 1) and commits
            auto result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (1u, "1");
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx2 reads (1, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2 where Key = 1u;
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
    
            // tx3 reads (1, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2;
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };
    
    Y_UNIT_TEST_TWIN(TxReadsCommitted, IsOlap) {
        TTxReadsCommitted tester;
        tester.SetIsOlap(IsOlap);
        tester.Execute();
    }

    class TTxReadsItsOwnWrites : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
    
            // tx1 writes (1, 1)
            auto result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (1u, "1");
                )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto tx1 = result.GetTransaction();

            // tx1 reads (1, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2 where Key = 1u;
                )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
    
            // tx1 reads (1, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2;
                )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };
    
    Y_UNIT_TEST_TWIN(TxReadsItsOwnWrites, IsOlap) {
        TTxReadsItsOwnWrites tester;
        tester.SetIsOlap(IsOlap);
        tester.Execute();
    }

    class TTxDeleteOwnUncommitted : public TTableDataModificationTester {
    protected:
        void DoExecute() override {

            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
    
            // tx1 writes (1, 1)
            auto result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (1u, "1");
                )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto tx1 = result.GetTransaction();

            // tx1 writes (1, 2)
            result = session1.ExecuteQuery(R"(
                upsert into KV2 (Key, Value) values (1u, "2");
                )", TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            tx1 = result.GetTransaction();

            // this read is very important for reproducing the issue in Column Shards,
            // if we comment it out, the test will pass
            // tx1 reads (1, 2)
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2;
                )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["2"]]])", FormatResultSetYson(result.GetResultSet(0)));
            tx1 = result.GetTransaction();

            // tx1 removes 1
            result = session1.ExecuteQuery(Q1_(R"(
                delete from KV2 where Key = 1u;
                )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            tx1 = result.GetTransaction();

            // if we uncomment this read, the test will pass
            // so this read somehow fixes the visibility of the deleted change

            // tx1 reads 1 and sees nothing
            // result = session1.ExecuteQuery(Q1_(R"(
            //     select * from KV2;
            //     )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            // UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            // CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
            // tx1 = result.GetTransaction();

            // tx1 commits
            result = tx1->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx2 reads 1, sees nothing
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2 where Key = 1u;
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[]", FormatResultSetYson(result.GetResultSet(0)));
        }
    };
    
    Y_UNIT_TEST_TWIN(TxDeleteOwnUncommitted, IsOlap) {
        if (IsOlap) {
            // muted until this is fixed: https://github.com/ydb-platform/ydb/issues/28447
            return;
        }
        TTxDeleteOwnUncommitted tester;
        tester.SetIsOlap(IsOlap);
        tester.Execute();
    }

    class TDirtyReads : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            // given a row (0, 0)
            auto result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (0u, "0");
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx1 writes (1, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (1u, "1");
                )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto tx1 = result.GetTransaction();

            // tx1 updates (0, 0) to (0, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                update KV2 set Value = "1" where Key = 0u;
                )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            tx1 = result.GetTransaction();

            // tx2 reads all the KV, it should see (0, 0)
            result = session2.ExecuteQuery(Q1_(R"(
                select * from KV2;
                )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[0u;["0"]]])", FormatResultSetYson(result.GetResultSet(0)));
            auto tx2 = result.GetTransaction();

            // tx2 reads key=1 and sees nothing
            result = session2.ExecuteQuery(Q1_(R"(
                select * from KV2 where Key = 1u;
                )"), TTxControl::Tx(*tx2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
            tx2 = result.GetTransaction();

            // bonus checks 1: tx1's reads do not affect the visibility of its writes to other concurrent txs
            
            // tx1 sees (0, 1), (1, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2 order by Key;
                )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[0u;["1"]];[1u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
            tx1 = result.GetTransaction();

            // tx2 sees (0, 0)
            result = session2.ExecuteQuery(Q1_(R"(
                select * from KV2;
                )"), TTxControl::Tx(*tx2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[0u;["0"]]])", FormatResultSetYson(result.GetResultSet(0)));
            tx2 = result.GetTransaction();

            // bonus checks 2: tx2's own writes do not affect what it sees from the other concurrent txs

            // tx2 writes (2, 2)
            result = session2.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (2u, "2");
                )"), TTxControl::Tx(*tx2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx2 sees (0, 0), (2, 2)
            result = session2.ExecuteQuery(Q1_(R"(
                select * from KV2 order by Key;
                )"), TTxControl::Tx(*tx2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[0u;["0"]];[2u;["2"]]])", FormatResultSetYson(result.GetResultSet(0)));
            tx2 = result.GetTransaction();

            // we do not check conflicts on commit in this test,
            // so just rollback both txs
            result = tx1->Rollback().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            result = tx2->Rollback().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST_TWIN(DirtyReads, IsOlap) {
        TDirtyReads tester;
        tester.SetIsOlap(IsOlap);
        tester.Execute();
    }

    class TChangeFromTheFuture : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            // there is a business rule, 2 may be = 2, only if there is no 1=0

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            // there is (1, 0)
            auto result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (1u, "0");
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx1 upserts 1 = 1
            result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (1u, "1");
                )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto tx1 = result.GetTransaction();

            // tx2 upserts 2 = 2
            result = session2.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (2u, "2");
                )"),
                TTxControl::BeginTx()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto tx2 = result.GetTransaction();

            // tx1 commits
            result = tx1->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // a sneaky read checks the condition after the write - never underestimate the creativity of the users.
            // tx2 sees no 1, because it was committed later (snapshot-wise) than tx1 started.
            result = session2.ExecuteQuery(Q1_(R"(
                select * from KV2 where Key = 1u;
                )"), TTxControl::Tx(*tx2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["0"]]])", FormatResultSetYson(result.GetResultSet(0)));
            tx2 = result.GetTransaction();

            // tx2 fails to commit
            result = tx2->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST_TWIN(ChangeFromTheFuture, IsOlap) {
        TChangeFromTheFuture tester;
        tester.SetIsOlap(IsOlap);
        tester.Execute();
    }
    
    class TLostUpdate : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            // classic lost update

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            // there is (1, 1) in the database
            auto result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (1u, "1");
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx1 reads (1, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2 where Key = 1u;
                )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
            auto tx1 = result.GetTransaction();

            // tx1 increments the value and writes (1, 2)
            result = session1.ExecuteQuery(Q1_(R"(
                update KV2 set Value = "2" where Key = 1u;
                )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            tx1 = result.GetTransaction();

            // tx2 reads (1, 1)
            result = session2.ExecuteQuery(Q1_(R"(
                select * from KV2 where Key = 1u;
                )"),
                TTxControl::BeginTx()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
            auto tx2 = result.GetTransaction();

            // tx2 increments the value and writes (1, 2)
            result = session2.ExecuteQuery(Q1_(R"(
                update KV2 set Value = "2" where Key = 1u;
                )"), TTxControl::Tx(*tx2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            tx2 = result.GetTransaction();

            // tx1 commits
            result = tx1->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx2 tries to commit and fails
            result = tx2->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());

            // there is (1, 2) in the database
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2 where Key = 1u;
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["2"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST_TWIN(LostUpdate, IsOlap) {
        TLostUpdate tester;
        tester.SetIsOlap(IsOlap);
        tester.Execute();
    }

    class TTransactionFailsAsSoonAsItIsClearItCannotCommit : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            if (GetIsOlap()) {
                // will be fixed in https://github.com/ydb-platform/ydb/issues/25661
                return;
            }

            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            // tx1 acquires a snapshot reading from another table
            auto result = session1.ExecuteQuery(Q1_(R"(
                select * from KV;
                )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto tx1 = result.GetTransaction();

            // tx2 writes (2, 2) commits
            result = session2.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (2u, "2");
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx1 reads all the KV2 and understands that there are new data
            // in the range it reads, so it will not be able to write anything.
            // If there is a write on the way, tx1 must fail right away not waiting for the commit.
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2;
                )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
            tx1 = result.GetTransaction();

            // tx1 tries to write (1, 1) and fails
            result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (1u, "1");
                )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());
            tx1 = result.GetTransaction();
            UNIT_ASSERT_C(!tx1.has_value(), "Transaction must be aborted");
        }
    };

    Y_UNIT_TEST_TWIN(TransactionFailsAsSoonAsItIsClearItCannotCommit, IsOlap) {
        TTransactionFailsAsSoonAsItIsClearItCannotCommit tester;
        tester.SetIsOlap(IsOlap);
        tester.Execute();
    }

    class TWriteSkew : public TTableDataModificationTester {
        YDB_ACCESSOR(TString, WriteOp, "replace");
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            // classic write skew
            // there is a business rule: sum of values must be <= 3

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            // there is (1, 1) and (2, 1) in the database, so sum = 2
            auto result = session1.ExecuteQuery(Q1_(R"(
                upsert into KV2 (Key, Value) values (1u, "1");
                upsert into KV2 (Key, Value) values (2u, "1");
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx1 reads (1, 1) and (2, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2 order by Key;
                )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["1"]];[2u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
            auto tx1 = result.GetTransaction();
            
            // tx1 writes (3, 1), so the sum = 3, which is ok
            result = session1.ExecuteQuery(
                GetWriteOp() + " into KV2 (Key, Value) values (3u, \"1\");",
                TTxControl::Tx(*tx1)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            tx1 = result.GetTransaction();

            // tx2 reads (1, 1) and (2, 1)
            result = session2.ExecuteQuery(Q1_(R"(
                select * from KV2 order by Key;
                )"), TTxControl::BeginTx()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["1"]];[2u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
            auto tx2 = result.GetTransaction();

            // tx2 writes (4, 1), so the sum = 3, which is ok
            result = session2.ExecuteQuery(
                GetWriteOp() + " into KV2 (Key, Value) values (4u, \"1\");",
                TTxControl::Tx(*tx2)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            tx2 = result.GetTransaction();

            // tx1 commits
            // now there are (1, 1), (2, 1) and (3, 1) in the database
            // sum = 3, which is ok
            result = tx1->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // tx2 tries to commit and fails, because that would have made the sum = 4, which is not ok
            result = tx2->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());

            // check the database, there are (1, 1), (2, 1) and (3, 1)
            result = session1.ExecuteQuery(Q1_(R"(
                select * from KV2 order by Key;
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["1"]];[2u;["1"]];[3u;["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST_TWIN(WriteSkewInsert, IsOlap) {
        TWriteSkew tester;
        tester.SetIsOlap(IsOlap);
        tester.SetWriteOp("insert");
        tester.Execute();
    }

    Y_UNIT_TEST_TWIN(WriteSkewUpsert, IsOlap) {
        TWriteSkew tester;
        tester.SetIsOlap(IsOlap);
        tester.SetWriteOp("upsert");
        tester.Execute();
    }

    Y_UNIT_TEST_TWIN(WriteSkewReplace, IsOlap) {
        TWriteSkew tester;
        tester.SetIsOlap(IsOlap);
        tester.SetWriteOp("replace");
        tester.Execute();
    }
    
    class TReadOnlyTxCommitsOnConcurrentWrite : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

            auto tx = result.GetTransaction();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [1u;["One"]];
                [4000000001u;["BigOne"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "ChangedOne");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session2.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Key = 1u;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [1u;["ChangedOne"]];
            ])", FormatResultSetYson(result.GetResultSet(0)));

            result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Key = 1u;
            )"), TTxControl::Tx(*tx)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [1u;["One"]];
            ])", FormatResultSetYson(result.GetResultSet(0)));

            result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Key = 2u OR Key = 4000000002u ORDER BY Key;
            )"), TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([
                [2u;["Two"]];
                [4000000002u;["BigTwo"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(ReadOnlyTxCommitsOnConcurrentWrite) {
        TReadOnlyTxCommitsOnConcurrentWrite tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapReadOnlyTxCommitsOnConcurrentWrite) {
        TReadOnlyTxCommitsOnConcurrentWrite tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TReadWriteTxFailsOnConcurrentWrite1 : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

            auto tx = result.GetTransaction();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [1u;["One"]];
                [4000000001u;["BigOne"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "ChangedOne");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session1.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "TwiceChangedOne");
            )"), TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(ReadWriteTxFailsOnConcurrentWrite1) {
        TReadWriteTxFailsOnConcurrentWrite1 tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapReadWriteTxFailsOnConcurrentWrite1) {
        TReadWriteTxFailsOnConcurrentWrite1 tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TReadWriteTxFailsOnConcurrentWrite2 : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

            auto tx = result.GetTransaction();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [1u;["One"]];
                [4000000001u;["BigOne"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));

            // We need to sleep before the upsert below, otherwise writes
            // might happen in the same step as the snapshot, which would be
            // treated as happening before snapshot and will not break any locks.
            Sleep(TDuration::Seconds(2));

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV2` (Key, Value) VALUES (101u, "SomeText");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session1.ExecuteQuery(Q_(R"(
                UPDATE `/Root/KV` SET Value = "Something" WHERE Key = 1u;
                UPDATE `/Root/KV2` SET Value = "AnotherString" WHERE Key = 101u;
            )"), TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(ReadWriteTxFailsOnConcurrentWrite2) {
        TReadWriteTxFailsOnConcurrentWrite2 tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapReadWriteTxFailsOnConcurrentWrite2) {
        TReadWriteTxFailsOnConcurrentWrite2 tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TReadWriteTxFailsOnConcurrentWrite3 : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

            auto tx = result.GetTransaction();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [1u;["One"]];
                [4000000001u;["BigOne"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (2u, "ChangedTwo");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Key = 2u OR Key = 4000000002u ORDER BY Key;
            )"), TTxControl::Tx(*tx)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([
                [2u;["Two"]];
                [4000000002u;["BigTwo"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));

            result = session1.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (2u, "TwiceChangedTwo");
            )"), TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(ReadWriteTxFailsOnConcurrentWrite3) {
        TReadWriteTxFailsOnConcurrentWrite3 tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapReadWriteTxFailsOnConcurrentWrite3) {
        TReadWriteTxFailsOnConcurrentWrite3 tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TNamedStatement : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();

            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    $data = SELECT * FROM `/Root/KV`;
                    DELETE FROM `/Root/KV` WHERE 1=1;
                    SELECT COUNT(*) FROM `/Root/KV`;
                    SELECT COUNT(*) FROM $data;
                    DELETE FROM `/Root/KV` ON SELECT 424242u AS Key, "One" As Value;
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (424242u, "One");
                    SELECT COUNT(*) FROM `/Root/KV`;
                    SELECT COUNT(*) FROM $data;
                )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[0u]])", FormatResultSetYson(result.GetResultSet(0)));
                CompareYson(R"([[0u]])", FormatResultSetYson(result.GetResultSet(1)));
                CompareYson(R"([[1u]])", FormatResultSetYson(result.GetResultSet(2)));
                CompareYson(R"([[1u]])", FormatResultSetYson(result.GetResultSet(3)));
            }
        }
    };

    Y_UNIT_TEST(OltpNamedStatementNoSink) {
        TNamedStatement tester;
        tester.SetDisableSinks(true);
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OltpNamedStatement) {
        TNamedStatement tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapNamedStatement) {
        TNamedStatement tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TMultiSinks: public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();

            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "1");
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "2");
                )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    SELECT Value FROM `/Root/KV` WHERE Key = 1u;
                )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[["2"]]])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(OltpMultiSinksNoSinks) {
        TMultiSinks tester;
        tester.SetDisableSinks(true);
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OltpMultiSinks) {
        TMultiSinks tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapMultiSinks) {
        TMultiSinks tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TInsertConflictingKey: public TTableDataModificationTester {
        YDB_ACCESSOR(bool, CommitOnInsert, false);
    protected:
        void DoExecute() override {
            auto queryClient = Kikimr->GetQueryClient();

            auto session1 = queryClient.GetSession().GetValueSync().GetSession();
            auto session2 = queryClient.GetSession().GetValueSync().GetSession();

            auto readResult1 = session1
                                .ExecuteQuery(R"(
                        SELECT * FROM `/Root/KV`;
                                        )",
                                    NQuery::TTxControl::BeginTx())
                                .GetValueSync();
            UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, readResult1.GetStatus(), readResult1.GetIssues().ToString());

            auto tx1 = readResult1.GetTransaction();
            UNIT_ASSERT(tx1);

            auto readResult2 = session2
                                .ExecuteQuery(R"(
                        SELECT * FROM `/Root/KV`;
                                        )",
                                    NQuery::TTxControl::BeginTx())
                                .GetValueSync();
            UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, readResult2.GetStatus(), readResult2.GetIssues().ToString());
            auto tx2 = readResult2.GetTransaction();
            UNIT_ASSERT(tx2);

            if (CommitOnInsert) {

                auto insertResult1 = session1
                                        .ExecuteQuery(R"(
                            INSERT INTO `/Root/KV` (Key) VALUES (100)
                    )",
                                            NQuery::TTxControl::Tx(*tx1).CommitTx())
                                        .GetValueSync();
                UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, insertResult1.GetStatus(), insertResult1.GetIssues().ToString());
                auto insertResult2 = session2
                                        .ExecuteQuery(R"(
                            INSERT INTO `/Root/KV` (Key) VALUES (100)
                    )",
                                            NQuery::TTxControl::Tx(*tx2).CommitTx())
                                        .GetValueSync();
                UNIT_ASSERT_EQUAL_C(insertResult2.GetStatus(), NYdb::EStatus::ABORTED, insertResult2.GetIssues().ToString());
            } else {

                auto insertResult1 = session1
                                        .ExecuteQuery(R"(
                            INSERT INTO `/Root/KV` (Key) VALUES (100)
                    )",
                                            NQuery::TTxControl::Tx(*tx1))
                                        .GetValueSync();
                UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, insertResult1.GetStatus(), insertResult1.GetIssues().ToString());
                auto insertResult2 = session2
                                        .ExecuteQuery(R"(
                            INSERT INTO `/Root/KV` (Key) VALUES (100)
                    )",
                                            NQuery::TTxControl::Tx(*tx2))
                                        .GetValueSync();
                UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, insertResult2.GetStatus(), insertResult2.GetIssues().ToString());
                auto commitResult1 = tx1->Commit().GetValueSync();
                UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, commitResult1.GetStatus(), commitResult1.GetIssues().ToString());
                auto commitResult2 = tx2->Commit().GetValueSync();
                UNIT_ASSERT_EQUAL_C(commitResult2.GetStatus(), NYdb::EStatus::ABORTED, commitResult2.GetIssues().ToString());
            }
        }
    };

    Y_UNIT_TEST_QUAD(InsertConflictingKey, IsOlap, CommitOnInsert) {
        TInsertConflictingKey tester;
        tester.SetIsOlap(IsOlap);
        tester.SetCommitOnInsert(CommitOnInsert);
        tester.Execute();
    }

    class TUpdateColumns: public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            if (GetIsOlap()) {
                //TODO fix https://github.com/ydb-platform/ydb/issues/25020
                return;
            }
            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();
            
            auto insertResult1 = session1.ExecuteQuery(R"(
                        UPSERT INTO `/Root/Test` (Group, Name, Amount) VALUES (1u, "Anna", 7000ul)
                )", NQuery::TTxControl::BeginTx()) .GetValueSync();
            UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, insertResult1.GetStatus(), insertResult1.GetIssues().ToString());
            auto tx1 = insertResult1.GetTransaction();
            UNIT_ASSERT(tx1);

            auto insertResult2 = session2.ExecuteQuery(R"(
                        UPSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (1u, "Anna", "Changed")
                )", NQuery::TTxControl::BeginTx()) .GetValueSync();
            UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, insertResult2.GetStatus(), insertResult2.GetIssues().ToString());
            auto tx2 = insertResult2.GetTransaction();
            UNIT_ASSERT(tx2);

            auto commitResult1 = tx1->Commit().GetValueSync();
            UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, commitResult1.GetStatus(), commitResult1.GetIssues().ToString());
            auto commitResult2 = tx2->Commit().GetValueSync();
            UNIT_ASSERT_EQUAL_C(commitResult2.GetStatus(), NYdb::EStatus::SUCCESS, commitResult2.GetIssues().ToString());

            auto readResult = client.ExecuteQuery(R"(
                        SELECT * FROM `/Root/Test` WHERE Group = 1u AND Name = "Anna";
                    )", NQuery::TTxControl::NoTx()) .GetValueSync();
            UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, readResult.GetStatus(), readResult.GetIssues().ToString());
            CompareYson("[[[7000u];[\"Changed\"];1u;\"Anna\"]]", FormatResultSetYson(readResult.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST_TWIN(UpdateColumns, IsOlap) {
        TUpdateColumns tester;
        tester.SetIsOlap(IsOlap);
        tester.Execute();
    }

}

} // namespace NKqp
} // namespace NKikimr
