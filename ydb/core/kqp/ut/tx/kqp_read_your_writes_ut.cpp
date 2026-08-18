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

// ============================================================================
// Multi-write chains
// ============================================================================

// INSERT → (SELECT sees "inserted") → UPDATE → SELECT sees "updated".
class TInsertThenUpdateThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenUpdateThenSelect(TTxSettings txSettings)
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
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["inserted"]]])", FormatResultSetYson(result.GetResultSet(0)));
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            UPDATE KV2 SET Value = "updated" WHERE Key = 1u;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["updated"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(InsertThenUpdateThenSelect_Serializable, IsOlap) {
    TInsertThenUpdateThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenUpdateThenSelect_Snapshot, IsOlap) {
    TInsertThenUpdateThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenUpdateThenSelect_ReadCommitted) {
    TInsertThenUpdateThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// INSERT → UPDATE (no inter-write SELECT) → commit → result is the updated value.
class TInsertThenUpdateAfterCommit : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenUpdateAfterCommit(TTxSettings txSettings)
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
            UPDATE KV2 SET Value = "updated" WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["updated"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(InsertThenUpdateAfterCommit_Serializable, IsOlap) {
    TInsertThenUpdateAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenUpdateAfterCommit_Snapshot, IsOlap) {
    TInsertThenUpdateAfterCommit tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenUpdateAfterCommit_ReadCommitted) {
    TInsertThenUpdateAfterCommit tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// INSERT → (SELECT sees "inserted") → DELETE → SELECT sees nothing.
class TInsertThenDeleteThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenDeleteThenSelect(TTxSettings txSettings)
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
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["inserted"]]])", FormatResultSetYson(result.GetResultSet(0)));
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            DELETE FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(InsertThenDeleteThenSelect_Serializable, IsOlap) {
    TInsertThenDeleteThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenDeleteThenSelect_Snapshot, IsOlap) {
    TInsertThenDeleteThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenDeleteThenSelect_ReadCommitted) {
    TInsertThenDeleteThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// INSERT → DELETE (no inter-write SELECT) → commit → row is absent.
class TInsertThenDeleteAfterCommit : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenDeleteAfterCommit(TTxSettings txSettings)
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
            DELETE FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(InsertThenDeleteAfterCommit_Serializable, IsOlap) {
    TInsertThenDeleteAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenDeleteAfterCommit_Snapshot, IsOlap) {
    TInsertThenDeleteAfterCommit tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenDeleteAfterCommit_ReadCommitted) {
    TInsertThenDeleteAfterCommit tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// UPDATE → (SELECT sees new value) → UPDATE using new value → SELECT sees final value.
// Uses the Test table (has an Amount column suitable for arithmetic).
class TUpdateThenUpdateBasedOnNewValueThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TUpdateThenUpdateBasedOnNewValueThenSelect(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO Test (Group, Name, Amount) VALUES (1u, "A", 100ul);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            UPDATE Test SET Amount = 200ul WHERE Group = 1u AND Name = "A";
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[200u]]])", FormatResultSetYson(result.GetResultSet(0)));
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            UPDATE Test SET Amount = Amount + 50ul WHERE Group = 1u AND Name = "A";
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[250u]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(UpdateThenUpdateBasedOnNewValueThenSelect_Serializable, IsOlap) {
    TUpdateThenUpdateBasedOnNewValueThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateThenUpdateBasedOnNewValueThenSelect_Snapshot, IsOlap) {
    TUpdateThenUpdateBasedOnNewValueThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(UpdateThenUpdateBasedOnNewValueThenSelect_ReadCommitted) {
    TUpdateThenUpdateBasedOnNewValueThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// UPDATE → UPDATE using new value (no inter-write SELECT) → commit → final value is correct.
class TUpdateThenUpdateBasedOnNewValueAfterCommit : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TUpdateThenUpdateBasedOnNewValueAfterCommit(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO Test (Group, Name, Amount) VALUES (1u, "A", 100ul);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            UPDATE Test SET Amount = 200ul WHERE Group = 1u AND Name = "A";
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        // Amount + 50 must read 200 (written above), not 100 (original).
        result = session.ExecuteQuery(R"(
            UPDATE Test SET Amount = Amount + 50ul WHERE Group = 1u AND Name = "A";
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[250u]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(UpdateThenUpdateBasedOnNewValueAfterCommit_Serializable, IsOlap) {
    TUpdateThenUpdateBasedOnNewValueAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateThenUpdateBasedOnNewValueAfterCommit_Snapshot, IsOlap) {
    TUpdateThenUpdateBasedOnNewValueAfterCommit tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(UpdateThenUpdateBasedOnNewValueAfterCommit_ReadCommitted) {
    TUpdateThenUpdateBasedOnNewValueAfterCommit tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// UPDATE → (SELECT sees new value) → DELETE WHERE updated value → row is gone.
class TUpdateThenPredicateDeleteThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TUpdateThenPredicateDeleteThenSelect(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (2u, "A"), (3u, "B");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            UPDATE KV2 SET Value = "X" WHERE Key = 2u;
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Key FROM KV2 WHERE Value = "X";
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[2u]])", FormatResultSetYson(result.GetResultSet(0)));
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            DELETE FROM KV2 WHERE Value = "X";
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Key, Value FROM KV2 ORDER BY Key;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[1u;["A"]];[3u;["B"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(UpdateThenPredicateDeleteThenSelect_Serializable, IsOlap) {
    TUpdateThenPredicateDeleteThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateThenPredicateDeleteThenSelect_Snapshot, IsOlap) {
    TUpdateThenPredicateDeleteThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(UpdateThenPredicateDeleteThenSelect_ReadCommitted) {
    TUpdateThenPredicateDeleteThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// UPDATE → DELETE WHERE updated value (no inter-write SELECT) → commit → row is gone.
class TUpdateThenPredicateDeleteAfterCommit : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TUpdateThenPredicateDeleteAfterCommit(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (2u, "A"), (3u, "B");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            UPDATE KV2 SET Value = "X" WHERE Key = 2u;
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            DELETE FROM KV2 WHERE Value = "X";
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            SELECT Key, Value FROM KV2 ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[1u;["A"]];[3u;["B"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(UpdateThenPredicateDeleteAfterCommit_Serializable, IsOlap) {
    TUpdateThenPredicateDeleteAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateThenPredicateDeleteAfterCommit_Snapshot, IsOlap) {
    TUpdateThenPredicateDeleteAfterCommit tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(UpdateThenPredicateDeleteAfterCommit_ReadCommitted) {
    TUpdateThenPredicateDeleteAfterCommit tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// DELETE → (SELECT sees nothing) → INSERT same key → SELECT sees new value.
class TDeleteThenInsertSameKeyThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TDeleteThenInsertSameKeyThenSelect(TTxSettings txSettings)
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
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            INSERT INTO KV2 (Key, Value) VALUES (1u, "new");
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["new"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(DeleteThenInsertSameKeyThenSelect_Serializable, IsOlap) {
    TDeleteThenInsertSameKeyThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(DeleteThenInsertSameKeyThenSelect_Snapshot, IsOlap) {
    TDeleteThenInsertSameKeyThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(DeleteThenInsertSameKeyThenSelect_ReadCommitted) {
    TDeleteThenInsertSameKeyThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// DELETE → INSERT same key (no inter-write SELECT) → commit → new value is visible.
class TDeleteThenInsertSameKeyAfterCommit : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TDeleteThenInsertSameKeyAfterCommit(TTxSettings txSettings)
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
            INSERT INTO KV2 (Key, Value) VALUES (1u, "new");
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["new"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(DeleteThenInsertSameKeyAfterCommit_Serializable, IsOlap) {
    TDeleteThenInsertSameKeyAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(DeleteThenInsertSameKeyAfterCommit_Snapshot, IsOlap) {
    TDeleteThenInsertSameKeyAfterCommit tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(DeleteThenInsertSameKeyAfterCommit_ReadCommitted) {
    TDeleteThenInsertSameKeyAfterCommit tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// INSERT → (SELECT) → UPDATE → (SELECT) → DELETE → SELECT sees nothing.
class TInsertThenUpdateThenDeleteThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenUpdateThenDeleteThenSelect(TTxSettings txSettings)
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
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["inserted"]]])", FormatResultSetYson(result.GetResultSet(0)));
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            UPDATE KV2 SET Value = "updated" WHERE Key = 1u;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["updated"]]])", FormatResultSetYson(result.GetResultSet(0)));
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            DELETE FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(InsertThenUpdateThenDeleteThenSelect_Serializable, IsOlap) {
    TInsertThenUpdateThenDeleteThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenUpdateThenDeleteThenSelect_Snapshot, IsOlap) {
    TInsertThenUpdateThenDeleteThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenUpdateThenDeleteThenSelect_ReadCommitted) {
    TInsertThenUpdateThenDeleteThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// INSERT → UPDATE → DELETE (no inter-write SELECTs) → commit → row is absent.
class TInsertThenUpdateThenDeleteAfterCommit : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenUpdateThenDeleteAfterCommit(TTxSettings txSettings)
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
            UPDATE KV2 SET Value = "updated" WHERE Key = 1u;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            DELETE FROM KV2 WHERE Key = 1u;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            SELECT Value FROM KV2 WHERE Key = 1u;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(InsertThenUpdateThenDeleteAfterCommit_Serializable, IsOlap) {
    TInsertThenUpdateThenDeleteAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenUpdateThenDeleteAfterCommit_Snapshot, IsOlap) {
    TInsertThenUpdateThenDeleteAfterCommit tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenUpdateThenDeleteAfterCommit_ReadCommitted) {
    TInsertThenUpdateThenDeleteAfterCommit tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// UPDATE row B → (SELECT B sees new value) → write that value into row A → SELECT A sees it.
class TUpdateBasedOnEarlierUpdatedRowThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TUpdateBasedOnEarlierUpdatedRowThenSelect(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO Test (Group, Name, Amount) VALUES (1u, "A", 100ul), (2u, "B", 200ul);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            UPDATE Test SET Amount = Amount * 2ul WHERE Group = 2u AND Name = "B";
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        // Read B's new value within the same transaction.
        result = session.ExecuteQuery(R"(
            SELECT Amount FROM Test WHERE Group = 2u AND Name = "B";
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[400u]]])", FormatResultSetYson(result.GetResultSet(0)));
        tx = result.GetTransaction();

        // Copy B's new value into A.
        result = session.ExecuteQuery(R"(
            $b = (SELECT Amount FROM Test WHERE Group = 2u AND Name = "B");
            UPDATE Test SET Amount = $b WHERE Group = 1u AND Name = "A";
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        tx = result.GetTransaction();

        result = session.ExecuteQuery(R"(
            SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[400u]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(UpdateBasedOnEarlierUpdatedRowThenSelect_Serializable, IsOlap) {
    TUpdateBasedOnEarlierUpdatedRowThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateBasedOnEarlierUpdatedRowThenSelect_Snapshot, IsOlap) {
    TUpdateBasedOnEarlierUpdatedRowThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(UpdateBasedOnEarlierUpdatedRowThenSelect_ReadCommitted) {
    TUpdateBasedOnEarlierUpdatedRowThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// UPDATE row B → copy B's new value into A in a single combined statement (no explicit mid-tx SELECT).
class TUpdateBasedOnEarlierUpdatedRowAfterCommit : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TUpdateBasedOnEarlierUpdatedRowAfterCommit(TTxSettings txSettings)
        : TxSettings_(txSettings)
    {
        SetFillTables(false);
    }
protected:
    void DoExecute() override {
        auto client = Kikimr->GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            INSERT INTO Test (Group, Name, Amount) VALUES (1u, "A", 100ul), (2u, "B", 200ul);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            UPDATE Test SET Amount = Amount * 2ul WHERE Group = 2u AND Name = "B";
        )", TTxControl::BeginTx(TxSettings_)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto tx = result.GetTransaction();

        // Read B's current (updated) value and write it into A in a single statement.
        result = session.ExecuteQuery(R"(
            $b = (SELECT Amount FROM Test WHERE Group = 2u AND Name = "B");
            UPDATE Test SET Amount = $b WHERE Group = 1u AND Name = "A";
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteQuery(R"(
            SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[400u]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
};

Y_UNIT_TEST_TWIN(UpdateBasedOnEarlierUpdatedRowAfterCommit_Serializable, IsOlap) {
    TUpdateBasedOnEarlierUpdatedRowAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateBasedOnEarlierUpdatedRowAfterCommit_Snapshot, IsOlap) {
    TUpdateBasedOnEarlierUpdatedRowAfterCommit tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(UpdateBasedOnEarlierUpdatedRowAfterCommit_ReadCommitted) {
    TUpdateBasedOnEarlierUpdatedRowAfterCommit tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

} // Y_UNIT_TEST_SUITE

} // namespace NKqp
} // namespace NKikimr
