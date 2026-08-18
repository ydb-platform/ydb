#include "kqp_sink_common.h"

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpReadYourWrites) {

// ============================================================================
// Basic write → point read
// ============================================================================

// After inserting a new row, a point lookup in the same transaction must see it.
class TInsertThenSelectPointRead : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenSelectPointRead(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (1u, "inserted");
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["inserted"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(InsertThenSelectPointRead_Serializable, IsOlap) {
    TInsertThenSelectPointRead tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenSelectPointRead_Snapshot, IsOlap) {
    TInsertThenSelectPointRead tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenSelectPointRead_ReadCommitted) {
    TInsertThenSelectPointRead tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// After updating a row, a point lookup in the same transaction must see the new value.
class TUpdateThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TUpdateThenSelect(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (1u, "original");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            UPDATE KV2 SET Value = "updated" WHERE Key = 1u;
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["updated"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(UpdateThenSelect_Serializable, IsOlap) {
    TUpdateThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateThenSelect_Snapshot, IsOlap) {
    TUpdateThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(UpdateThenSelect_ReadCommitted) {
    TUpdateThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// After deleting a row, a point lookup in the same transaction must return nothing.
class TDeleteThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TDeleteThenSelect(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (1u, "original");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            DELETE FROM KV2 WHERE Key = 1u;
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(DeleteThenSelect_Serializable, IsOlap) {
    TDeleteThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(DeleteThenSelect_Snapshot, IsOlap) {
    TDeleteThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(DeleteThenSelect_ReadCommitted) {
    TDeleteThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// ============================================================================
// Write → range scan
// ============================================================================

// After inserting a new row, a full table scan in the same transaction must include it.
class TInsertThenRangeScan : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenRangeScan(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (3u, "C");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (2u, "B");
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Key, Value FROM KV2 ORDER BY Key;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[1u;["A"]];[2u;["B"]];[3u;["C"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(InsertThenRangeScan_Serializable, IsOlap) {
    TInsertThenRangeScan tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenRangeScan_Snapshot, IsOlap) {
    TInsertThenRangeScan tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenRangeScan_ReadCommitted) {
    TInsertThenRangeScan tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// After updating a row, a scan filtered on the updated column must reflect the new value.
class TUpdateThenRangeScanByUpdatedColumn : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TUpdateThenRangeScanByUpdatedColumn(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (2u, "A"), (3u, "A");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            UPDATE KV2 SET Value = "B" WHERE Key = 2u;
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Key, Value FROM KV2 WHERE Value = "B";
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[2u;["B"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(UpdateThenRangeScanByUpdatedColumn_Serializable, IsOlap) {
    TUpdateThenRangeScanByUpdatedColumn tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateThenRangeScanByUpdatedColumn_Snapshot, IsOlap) {
    TUpdateThenRangeScanByUpdatedColumn tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(UpdateThenRangeScanByUpdatedColumn_ReadCommitted) {
    TUpdateThenRangeScanByUpdatedColumn tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// After deleting a row, a full table scan in the same transaction must not include it.
class TDeleteThenRangeScan : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TDeleteThenRangeScan(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (2u, "B"), (3u, "C");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            DELETE FROM KV2 WHERE Key = 2u;
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Key, Value FROM KV2 ORDER BY Key;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[1u;["A"]];[3u;["C"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(DeleteThenRangeScan_Serializable, IsOlap) {
    TDeleteThenRangeScan tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(DeleteThenRangeScan_Snapshot, IsOlap) {
    TDeleteThenRangeScan tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(DeleteThenRangeScan_ReadCommitted) {
    TDeleteThenRangeScan tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

} // Y_UNIT_TEST_SUITE

} // namespace NKqp
} // namespace NKikimr
