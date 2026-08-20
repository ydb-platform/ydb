#include "kqp_sink_common.h"

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

// Helper that wraps a query session and tracks the current transaction.
//
//   Exec(query)            — execute in current tx (begin if first call), assert SUCCESS
//   Check(query, yson)     — execute in current tx, assert SUCCESS, compare YSON result set 0
//   ExecCommit(query)      — execute in current tx and commit, assert SUCCESS
//   CheckCommit(query, y)  — execute in current tx, commit, assert SUCCESS, compare YSON
//   ExecAuto(query)        — execute in a fresh auto-committed tx, assert SUCCESS (setup/teardown)
//   CheckAuto(query, yson) — same as ExecAuto but also compares YSON (post-commit verification)
//   ExecExpectError(query) — execute in current tx expecting failure; resets tx, returns EStatus
struct TTestTx {
    NYdb::NQuery::TQueryClient Client;
    NYdb::NQuery::TSession Session;
    TTxSettings TxSettings;
    std::optional<NYdb::NQuery::TTransaction> Tx;

    TTestTx(NYdb::NQuery::TQueryClient client, TTxSettings txSettings)
        : Client(std::move(client))
        , Session(Client.GetSession().GetValueSync().GetSession())
        , TxSettings(txSettings)
    {}

    void Exec(const TString& query) {
        auto ctrl = Tx ? TTxControl::Tx(*Tx) : TTxControl::BeginTx(TxSettings);
        auto result = Session.ExecuteQuery(query, ctrl).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        Tx = result.GetTransaction();
    }

    void Check(const TString& query, const TString& yson) {
        auto ctrl = Tx ? TTxControl::Tx(*Tx) : TTxControl::BeginTx(TxSettings);
        auto result = Session.ExecuteQuery(query, ctrl).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        Tx = result.GetTransaction();
        CompareYson(yson, FormatResultSetYson(result.GetResultSet(0)));
    }

    void ExecCommit(const TString& query) {
        auto ctrl = Tx ? TTxControl::Tx(*Tx).CommitTx() : TTxControl::BeginTx(TxSettings).CommitTx();
        auto result = Session.ExecuteQuery(query, ctrl).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        Tx.reset();
    }

    void CheckCommit(const TString& query, const TString& yson) {
        auto ctrl = Tx ? TTxControl::Tx(*Tx).CommitTx() : TTxControl::BeginTx(TxSettings).CommitTx();
        auto result = Session.ExecuteQuery(query, ctrl).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        Tx.reset();
        CompareYson(yson, FormatResultSetYson(result.GetResultSet(0)));
    }

    void ExecAuto(const TString& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void CheckAuto(const TString& query, const TString& yson) {
        auto result = Session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(yson, FormatResultSetYson(result.GetResultSet(0)));
    }

    EStatus ExecExpectError(const TString& query) {
        auto ctrl = Tx ? TTxControl::Tx(*Tx) : TTxControl::BeginTx(TxSettings);
        auto result = Session.ExecuteQuery(query, ctrl).ExtractValueSync();
        Tx.reset();
        return result.GetStatus();
    }
};

Y_UNIT_TEST_SUITE(KqpReadYourWrites) {

// ============================================================================
// Basic write → point read
// ============================================================================

// After inserting a new row, a point lookup in the same transaction must see it.
class TInsertThenSelectPointRead : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenSelectPointRead(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "inserted");)");
        tx.CheckCommit(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["inserted"]]])");
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
    TUpdateThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "original");)");
        tx.Exec(R"(UPDATE KV2 SET Value = "updated" WHERE Key = 1u;)");
        tx.CheckCommit(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["updated"]]])");
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
    TDeleteThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "original");)");
        tx.Exec(R"(DELETE FROM KV2 WHERE Key = 1u;)");
        tx.CheckCommit(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([])");
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
    TInsertThenRangeScan(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (3u, "C");)");
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (2u, "B");)");
        tx.CheckCommit(R"(SELECT Key, Value FROM KV2 ORDER BY Key;)",
            R"([[1u;["A"]];[2u;["B"]];[3u;["C"]]])");
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
    TUpdateThenRangeScanByUpdatedColumn(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (2u, "A"), (3u, "A");)");
        tx.Exec(R"(UPDATE KV2 SET Value = "B" WHERE Key = 2u;)");
        tx.CheckCommit(R"(SELECT Key, Value FROM KV2 WHERE Value = "B";)", R"([[2u;["B"]]])");
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
    TDeleteThenRangeScan(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (2u, "B"), (3u, "C");)");
        tx.Exec(R"(DELETE FROM KV2 WHERE Key = 2u;)");
        tx.CheckCommit(R"(SELECT Key, Value FROM KV2 ORDER BY Key;)", R"([[1u;["A"]];[3u;["C"]]])");
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
    TInsertThenUpdateThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "inserted");)");
        tx.Check(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["inserted"]]])");
        tx.Exec(R"(UPDATE KV2 SET Value = "updated" WHERE Key = 1u;)");
        tx.CheckCommit(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["updated"]]])");
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
    TInsertThenUpdateAfterCommit(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "inserted");)");
        tx.ExecCommit(R"(UPDATE KV2 SET Value = "updated" WHERE Key = 1u;)");
        tx.CheckAuto(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["updated"]]])");
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
    TInsertThenDeleteThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "inserted");)");
        tx.Check(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["inserted"]]])");
        tx.Exec(R"(DELETE FROM KV2 WHERE Key = 1u;)");
        tx.CheckCommit(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([])");
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
    TInsertThenDeleteAfterCommit(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "inserted");)");
        tx.ExecCommit(R"(DELETE FROM KV2 WHERE Key = 1u;)");
        tx.CheckAuto(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([])");
    }
};

Y_UNIT_TEST_TWIN(InsertThenDeleteAfterCommit_Serializable, IsOlap) {
    TInsertThenDeleteAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenDeleteAfterCommit_Snapshot, IsOlap) {
    if (!IsOlap) {
        return; // TODO: SnapshotRW: DELETE doesn't see uncommitted INSERT from same tx without mid-tx read
    }
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
    TUpdateThenUpdateBasedOnNewValueThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO Test (Group, Name, Amount) VALUES (1u, "A", 100ul);)");
        tx.Exec(R"(UPDATE Test SET Amount = 200ul WHERE Group = 1u AND Name = "A";)");
        tx.Check(R"(SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";)", R"([[[200u]]])");
        tx.Exec(R"(UPDATE Test SET Amount = Amount + 50ul WHERE Group = 1u AND Name = "A";)");
        tx.CheckCommit(R"(SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";)", R"([[[250u]]])");
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
    TUpdateThenUpdateBasedOnNewValueAfterCommit(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO Test (Group, Name, Amount) VALUES (1u, "A", 100ul);)");
        tx.Exec(R"(UPDATE Test SET Amount = 200ul WHERE Group = 1u AND Name = "A";)");
        // Amount + 50 must read 200 (written above), not 100 (original).
        tx.ExecCommit(R"(UPDATE Test SET Amount = Amount + 50ul WHERE Group = 1u AND Name = "A";)");
        tx.CheckAuto(R"(SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";)", R"([[[250u]]])");
    }
};

Y_UNIT_TEST_TWIN(UpdateThenUpdateBasedOnNewValueAfterCommit_Serializable, IsOlap) {
    if (IsOlap) {
        return; // TODO: OLAP: second UPDATE doesn't see first UPDATE's value without mid-tx read
    }
    TUpdateThenUpdateBasedOnNewValueAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateThenUpdateBasedOnNewValueAfterCommit_Snapshot, IsOlap) {
    if (IsOlap) {
        return; // TODO: OLAP: second UPDATE doesn't see first UPDATE's value without mid-tx read
    }
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
    TUpdateThenPredicateDeleteThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (2u, "A"), (3u, "B");)");
        tx.Exec(R"(UPDATE KV2 SET Value = "X" WHERE Key = 2u;)");
        tx.Check(R"(SELECT Key FROM KV2 WHERE Value = "X";)", R"([[2u]])");
        tx.Exec(R"(DELETE FROM KV2 WHERE Value = "X";)");
        tx.CheckCommit(R"(SELECT Key, Value FROM KV2 ORDER BY Key;)", R"([[1u;["A"]];[3u;["B"]]])");
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
    TUpdateThenPredicateDeleteAfterCommit(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "A"), (2u, "A"), (3u, "B");)");
        tx.Exec(R"(UPDATE KV2 SET Value = "X" WHERE Key = 2u;)");
        tx.ExecCommit(R"(DELETE FROM KV2 WHERE Value = "X";)");
        tx.CheckAuto(R"(SELECT Key, Value FROM KV2 ORDER BY Key;)", R"([[1u;["A"]];[3u;["B"]]])");
    }
};

Y_UNIT_TEST_TWIN(UpdateThenPredicateDeleteAfterCommit_Serializable, IsOlap) {
    if (IsOlap) {
        return; // TODO: OLAP: predicate DELETE doesn't see UPDATE from same tx without mid-tx read
    }
    TUpdateThenPredicateDeleteAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(UpdateThenPredicateDeleteAfterCommit_Snapshot, IsOlap) {
    if (IsOlap) {
        return; // TODO: OLAP: predicate DELETE doesn't see UPDATE from same tx without mid-tx read
    }
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
    TDeleteThenInsertSameKeyThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "original");)");
        tx.Exec(R"(DELETE FROM KV2 WHERE Key = 1u;)");
        tx.Check(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([])");
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "new");)");
        tx.CheckCommit(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["new"]]])");
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
    TDeleteThenInsertSameKeyAfterCommit(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "original");)");
        tx.Exec(R"(DELETE FROM KV2 WHERE Key = 1u;)");
        tx.ExecCommit(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "new");)");
        tx.CheckAuto(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["new"]]])");
    }
};

Y_UNIT_TEST_TWIN(DeleteThenInsertSameKeyAfterCommit_Serializable, IsOlap) {
    if (IsOlap) {
        return; // TODO: OLAP: INSERT doesn't see DELETE from same tx without mid-tx read
    }
    TDeleteThenInsertSameKeyAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(DeleteThenInsertSameKeyAfterCommit_Snapshot, IsOlap) {
    return; // TODO: SnapshotRW + row table: INSERT doesn't see DELETE from same tx without mid-tx read; OLAP same issue
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
    TInsertThenUpdateThenDeleteThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "inserted");)");
        tx.Check(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["inserted"]]])");
        tx.Exec(R"(UPDATE KV2 SET Value = "updated" WHERE Key = 1u;)");
        tx.Check(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["updated"]]])");
        tx.Exec(R"(DELETE FROM KV2 WHERE Key = 1u;)");
        tx.CheckCommit(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([])");
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
    TInsertThenUpdateThenDeleteAfterCommit(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "inserted");)");
        tx.Exec(R"(UPDATE KV2 SET Value = "updated" WHERE Key = 1u;)");
        tx.ExecCommit(R"(DELETE FROM KV2 WHERE Key = 1u;)");
        tx.CheckAuto(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([])");
    }
};

Y_UNIT_TEST_TWIN(InsertThenUpdateThenDeleteAfterCommit_Serializable, IsOlap) {
    TInsertThenUpdateThenDeleteAfterCommit tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenUpdateThenDeleteAfterCommit_Snapshot, IsOlap) {
    if (!IsOlap) {
        return; // TODO: SnapshotRW: DELETE doesn't see uncommitted writes from same tx without mid-tx read
    }
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
    TUpdateBasedOnEarlierUpdatedRowThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO Test (Group, Name, Amount) VALUES (1u, "A", 100ul), (2u, "B", 200ul);)");
        tx.Exec(R"(UPDATE Test SET Amount = Amount * 2ul WHERE Group = 2u AND Name = "B";)");
        // Read B's new value within the same transaction.
        tx.Check(R"(SELECT Amount FROM Test WHERE Group = 2u AND Name = "B";)", R"([[[400u]]])");
        // Copy B's new value into A.
        tx.Exec(R"(
            $b = (SELECT Amount FROM Test WHERE Group = 2u AND Name = "B");
            UPDATE Test SET Amount = $b WHERE Group = 1u AND Name = "A";
        )");
        tx.CheckCommit(R"(SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";)", R"([[[400u]]])");
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
    TUpdateBasedOnEarlierUpdatedRowAfterCommit(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO Test (Group, Name, Amount) VALUES (1u, "A", 100ul), (2u, "B", 200ul);)");
        tx.Exec(R"(UPDATE Test SET Amount = Amount * 2ul WHERE Group = 2u AND Name = "B";)");
        // Read B's current (updated) value and write it into A in a single statement.
        tx.ExecCommit(R"(
            $b = (SELECT Amount FROM Test WHERE Group = 2u AND Name = "B");
            UPDATE Test SET Amount = $b WHERE Group = 1u AND Name = "A";
        )");
        tx.CheckAuto(R"(SELECT Amount FROM Test WHERE Group = 1u AND Name = "A";)", R"([[[400u]]])");
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

// ============================================================================
// Index scenarios (row tables only — OLAP does not support secondary indexes)
// ============================================================================

// After inserting a row, a lookup by secondary index in the same transaction must find it.
class TInsertThenSelectByIndex : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenSelectByIndex(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO Test2 (Group, Name, Comment) VALUES (1u, "A", "idx_val");)");
        tx.CheckCommit(R"(SELECT Group, Name FROM Test2 VIEW idx_comment WHERE Comment = "idx_val";)",
            R"([[1u;"A"]])");
    }
};

Y_UNIT_TEST(InsertThenSelectByIndex_Serializable) {
    TInsertThenSelectByIndex tester(TTxSettings::SerializableRW());
    tester.Execute();
}

Y_UNIT_TEST(InsertThenSelectByIndex_Snapshot) {
    TInsertThenSelectByIndex tester(TTxSettings::SnapshotRW());
    tester.Execute();
}

Y_UNIT_TEST(InsertThenSelectByIndex_ReadCommitted) {
    TInsertThenSelectByIndex tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// After updating an indexed column, the old index value must be gone and the new one visible.
class TUpdateIndexedColumnThenSelectByIndex : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TUpdateIndexedColumnThenSelectByIndex(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO Test2 (Group, Name, Comment) VALUES (1u, "A", "old_val");)");
        tx.Exec(R"(UPDATE Test2 SET Comment = "new_val" WHERE Group = 1u AND Name = "A";)");
        tx.Check(R"(SELECT Group, Name FROM Test2 VIEW idx_comment WHERE Comment = "new_val";)",
            R"([[1u;"A"]])");
        tx.CheckCommit(R"(SELECT Group, Name FROM Test2 VIEW idx_comment WHERE Comment = "old_val";)",
            R"([])");
    }
};

Y_UNIT_TEST(UpdateIndexedColumnThenSelectByIndex_Serializable) {
    TUpdateIndexedColumnThenSelectByIndex tester(TTxSettings::SerializableRW());
    tester.Execute();
}

Y_UNIT_TEST(UpdateIndexedColumnThenSelectByIndex_Snapshot) {
    TUpdateIndexedColumnThenSelectByIndex tester(TTxSettings::SnapshotRW());
    tester.Execute();
}

Y_UNIT_TEST(UpdateIndexedColumnThenSelectByIndex_ReadCommitted) {
    TUpdateIndexedColumnThenSelectByIndex tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// After deleting a row, a lookup by secondary index in the same transaction must return nothing.
class TDeleteThenSelectByIndex : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TDeleteThenSelectByIndex(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO Test2 (Group, Name, Comment) VALUES (1u, "A", "idx_val");)");
        tx.Exec(R"(DELETE FROM Test2 WHERE Group = 1u AND Name = "A";)");
        tx.CheckCommit(R"(SELECT Group, Name FROM Test2 VIEW idx_comment WHERE Comment = "idx_val";)",
            R"([])");
    }
};

Y_UNIT_TEST(DeleteThenSelectByIndex_Serializable) {
    TDeleteThenSelectByIndex tester(TTxSettings::SerializableRW());
    tester.Execute();
}

Y_UNIT_TEST(DeleteThenSelectByIndex_Snapshot) {
    TDeleteThenSelectByIndex tester(TTxSettings::SnapshotRW());
    tester.Execute();
}

Y_UNIT_TEST(DeleteThenSelectByIndex_ReadCommitted) {
    TDeleteThenSelectByIndex tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// ============================================================================
// Conflict / unique constraint scenarios
// ============================================================================

// INSERT → (SELECT sees first row) → INSERT same PK → tx aborts → DB is clean.
class TInsertThenInsertSamePkConflictThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenInsertSamePkConflictThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "first");)");
        tx.Check(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([[["first"]]])");
        UNIT_ASSERT_VALUES_EQUAL(
            tx.ExecExpectError(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "second");)"),
            EStatus::PRECONDITION_FAILED);
        // Tx is aborted — first INSERT must also be rolled back.
        tx.CheckAuto(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([])");
    }
};

Y_UNIT_TEST_TWIN(InsertThenInsertSamePkConflictThenSelect_Serializable, IsOlap) {
    TInsertThenInsertSamePkConflictThenSelect tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenInsertSamePkConflictThenSelect_Snapshot, IsOlap) {
    TInsertThenInsertSamePkConflictThenSelect tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenInsertSamePkConflictThenSelect_ReadCommitted) {
    TInsertThenInsertSamePkConflictThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// INSERT → INSERT same PK (no mid-tx read) → tx aborts → DB is clean.
class TInsertThenInsertSamePkConflictAfterAbort : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenInsertSamePkConflictAfterAbort(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "first");)");
        UNIT_ASSERT_VALUES_EQUAL(
            tx.ExecExpectError(R"(INSERT INTO KV2 (Key, Value) VALUES (1u, "second");)"),
            EStatus::PRECONDITION_FAILED);
        tx.CheckAuto(R"(SELECT Value FROM KV2 WHERE Key = 1u;)", R"([])");
    }
};

Y_UNIT_TEST_TWIN(InsertThenInsertSamePkConflictAfterAbort_Serializable, IsOlap) {
    TInsertThenInsertSamePkConflictAfterAbort tester(TTxSettings::SerializableRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST_TWIN(InsertThenInsertSamePkConflictAfterAbort_Snapshot, IsOlap) {
    TInsertThenInsertSamePkConflictAfterAbort tester(TTxSettings::SnapshotRW());
    tester.SetIsOlap(IsOlap);
    tester.Execute();
}

Y_UNIT_TEST(InsertThenInsertSamePkConflictAfterAbort_ReadCommitted) {
    TInsertThenInsertSamePkConflictAfterAbort tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// INSERT → (SELECT by index sees first row) → INSERT same unique index value → tx aborts → DB is clean.
class TInsertThenInsertSameUniqueIndexConflictThenSelect : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenInsertSameUniqueIndexConflictThenSelect(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO Test2 (Group, Name, Comment) VALUES (1u, "A", "idx_val");)");
        tx.Check(R"(SELECT Group, Name FROM Test2 VIEW idx_comment WHERE Comment = "idx_val";)",
            R"([[1u;"A"]])");
        UNIT_ASSERT_VALUES_EQUAL(
            tx.ExecExpectError(R"(INSERT INTO Test2 (Group, Name, Comment) VALUES (2u, "B", "idx_val");)"),
            EStatus::PRECONDITION_FAILED);
        tx.CheckAuto(R"(SELECT Group, Name FROM Test2 VIEW idx_comment WHERE Comment = "idx_val";)", R"([])");
    }
};

Y_UNIT_TEST(InsertThenInsertSameUniqueIndexConflictThenSelect_Serializable) {
    TInsertThenInsertSameUniqueIndexConflictThenSelect tester(TTxSettings::SerializableRW());
    tester.Execute();
}

Y_UNIT_TEST(InsertThenInsertSameUniqueIndexConflictThenSelect_Snapshot) {
    TInsertThenInsertSameUniqueIndexConflictThenSelect tester(TTxSettings::SnapshotRW());
    tester.Execute();
}

Y_UNIT_TEST(InsertThenInsertSameUniqueIndexConflictThenSelect_ReadCommitted) {
    TInsertThenInsertSameUniqueIndexConflictThenSelect tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// INSERT → INSERT same unique index value (no mid-tx read) → tx aborts → DB is clean.
class TInsertThenInsertSameUniqueIndexConflictAfterAbort : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TInsertThenInsertSameUniqueIndexConflictAfterAbort(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.Exec(R"(INSERT INTO Test2 (Group, Name, Comment) VALUES (1u, "A", "idx_val");)");
        UNIT_ASSERT_VALUES_EQUAL(
            tx.ExecExpectError(R"(INSERT INTO Test2 (Group, Name, Comment) VALUES (2u, "B", "idx_val");)"),
            EStatus::PRECONDITION_FAILED);
        tx.CheckAuto(R"(SELECT Group, Name FROM Test2 VIEW idx_comment WHERE Comment = "idx_val";)", R"([])");
    }
};

Y_UNIT_TEST(InsertThenInsertSameUniqueIndexConflictAfterAbort_Serializable) {
    TInsertThenInsertSameUniqueIndexConflictAfterAbort tester(TTxSettings::SerializableRW());
    tester.Execute();
}

Y_UNIT_TEST(InsertThenInsertSameUniqueIndexConflictAfterAbort_Snapshot) {
    TInsertThenInsertSameUniqueIndexConflictAfterAbort tester(TTxSettings::SnapshotRW());
    tester.Execute();
}

Y_UNIT_TEST(InsertThenInsertSameUniqueIndexConflictAfterAbort_ReadCommitted) {
    TInsertThenInsertSameUniqueIndexConflictAfterAbort tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

// DELETE row → INSERT different row with same unique index value → must succeed.
// The unique constraint is satisfied because the holder of the index value was deleted first.
class TDeleteThenInsertSameUniqueIndexValue : public TTableDataModificationTester {
    TTxSettings TxSettings_;
public:
    TDeleteThenInsertSameUniqueIndexValue(TTxSettings txSettings) : TxSettings_(txSettings) { SetFillTables(false); }
protected:
    void DoExecute() override {
        TTestTx tx(Kikimr->GetQueryClient(), TxSettings_);
        tx.ExecAuto(R"(INSERT INTO Test2 (Group, Name, Comment) VALUES (1u, "A", "idx_val");)");
        tx.Exec(R"(DELETE FROM Test2 WHERE Group = 1u AND Name = "A";)");
        tx.Check(R"(SELECT Group, Name FROM Test2 VIEW idx_comment WHERE Comment = "idx_val";)", R"([])");
        tx.Exec(R"(INSERT INTO Test2 (Group, Name, Comment) VALUES (2u, "B", "idx_val");)");
        tx.CheckCommit(R"(SELECT Group, Name FROM Test2 VIEW idx_comment WHERE Comment = "idx_val";)",
            R"([[2u;"B"]])");
    }
};

Y_UNIT_TEST(DeleteThenInsertSameUniqueIndexValue_Serializable) {
    TDeleteThenInsertSameUniqueIndexValue tester(TTxSettings::SerializableRW());
    tester.Execute();
}

Y_UNIT_TEST(DeleteThenInsertSameUniqueIndexValue_Snapshot) {
    TDeleteThenInsertSameUniqueIndexValue tester(TTxSettings::SnapshotRW());
    tester.Execute();
}

Y_UNIT_TEST(DeleteThenInsertSameUniqueIndexValue_ReadCommitted) {
    TDeleteThenInsertSameUniqueIndexValue tester(TTxSettings::ReadCommittedRW());
    tester.Execute();
}

} // Y_UNIT_TEST_SUITE

} // namespace NKqp
} // namespace NKikimr
