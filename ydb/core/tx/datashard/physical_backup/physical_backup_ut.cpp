#include <ydb/core/tablet_flat/flat_executor_ut_common.h>
#include <ydb/core/tablet_flat/flat_part_store.h>
#include <ydb/core/tablet_flat/flat_store_hotdog.h>
#include <ydb/core/tablet_flat/flat_database.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

// ============================================================================
// Physical Backup Round-Trip Test
//
// Proves the core concept:
//   1. Create a table and insert rows on a source tablet
//   2. Flush memtable (snapshot) so rows become SST parts
//   3. Enumerate SST parts and serialize as TBundle protos (physical export)
//   4. BorrowSnapshot to get the transferable proto
//   5. Create a destination tablet with the same schema
//   6. LoanTable to inject parts (physical restore)
//   7. Read rows from destination and verify they match source
// ============================================================================

namespace {

ui32 CountOf(const TString& s, char c) {
    ui32 n = 0;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == c) ++n;
    }
    return n;
}

enum : ui32 {
    TableId = 101,
    ColumnKeyId = 1,
    ColumnValueId = 20,
};

struct TTxSchema : public ITransaction {
    TTxSchema(TIntrusiveConstPtr<TCompactionPolicy> policy)
        : Policy(std::move(policy))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (txc.DB.GetScheme().GetTableInfo(TableId))
            return true;

        txc.DB.Alter()
            .AddTable("test" + ToString(ui32(TableId)), TableId)
            .AddColumn(TableId, "key", ColumnKeyId, NScheme::TInt64::TypeId, false, false)
            .AddColumn(TableId, "value", ColumnValueId, NScheme::TString::TypeId, false, false)
            .AddColumnToKey(TableId, ColumnKeyId)
            .SetCompactionPolicy(TableId, *Policy);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

    TIntrusiveConstPtr<TCompactionPolicy> Policy;
};

struct TTxAddRows : public ITransaction {
    TTxAddRows(ui64 startKey, ui64 count)
        : StartKey(startKey), Count(count)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        for (ui64 i = 0; i < Count; ++i) {
            const auto key = NScheme::TInt64::TInstance(StartKey + i);
            TString val = "value_" + ToString(StartKey + i);
            const auto value = NScheme::TString::TInstance(val);
            NTable::TUpdateOp ops{ColumnValueId, NTable::ECellOp::Set, value};
            txc.DB.Update(TableId, NTable::ERowOp::Upsert, {key}, {ops});
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    ui64 StartKey;
    ui64 Count;
};

// Snapshot context that signals completion via TEvReturn
class TTestSnapContext : public NFake::TDummySnapshotContext {
public:
    TTestSnapContext(TVector<ui32> tables)
        : Tables(std::move(tables))
    {}

    NFake::TEvExecute* OnFinished() override {
        // Return nullptr wrapped in TEvExecute to signal completion
        // Actually, we need to send TEvReturn. Let's use a no-op tx.
        return nullptr;
    }

    TConstArrayRef<ui32> TablesToSnapshot() const override {
        return Tables;
    }

private:
    TVector<ui32> Tables;
};

// Transaction that makes a snapshot and stores the context for later use
struct TTxMakeSnapshot : public ITransaction {
    TIntrusivePtr<TTableSnapshotContext>& SnapContext;

    TTxMakeSnapshot(TIntrusivePtr<TTableSnapshotContext>& snapContext)
        : SnapContext(snapContext)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SnapContext = new TTestSnapContext({TableId});
        txc.Env.MakeSnapshot(SnapContext);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
};

// Enumerate parts and verify we can serialize TBundle protos
struct TTxEnumerateParts : public ITransaction {
    ui32& PartCount;
    ui64& TotalRows;
    ui64& TotalBytes;

    TTxEnumerateParts(ui32& partCount, ui64& totalRows, ui64& totalBytes)
        : PartCount(partCount), TotalRows(totalRows), TotalBytes(totalBytes)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        PartCount = 0;
        TotalRows = 0;
        TotalBytes = 0;

        txc.DB.EnumerateTableParts(TableId,
            [&](const NTable::TPartView& partView) {
                ++PartCount;
                TotalRows += partView.Part->Stat.Rows;
                TotalBytes += partView.Part->Stat.Bytes;

                // Verify TBundle serialization works
                auto* partStore = dynamic_cast<const NTable::TPartStore*>(partView.Part.Get());
                Y_ENSURE(partStore, "Expected TPartStore");

                NKikimrExecutorFlat::TBundle bundleProto;
                TPageCollectionProtoHelper helper(false);
                helper.Do(&bundleProto, partView);

                Y_ENSURE(bundleProto.PageCollectionsSize() > 0,
                    "TBundle must have at least one page collection");

                // Verify blob ID collection works
                TVector<TLogoBlobID> blobIds;
                for (ui32 room = 0; room < partStore->PageCollections.size(); ++room) {
                    const auto* packet = partStore->Packet(room);
                    if (packet) {
                        packet->SaveAllBlobIdsTo(blobIds);
                    }
                }
                Y_ENSURE(!blobIds.empty(), "Part must have at least one blob");
            });

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
};

// BorrowSnapshot: serialize parts as TDatabaseBorrowPart proto
struct TTxBorrow : public ITransaction {
    TString& SnapBody;
    TIntrusivePtr<TTableSnapshotContext> SnapContext;
    ui64 TargetTabletId;

    TTxBorrow(TString& snapBody, TIntrusivePtr<TTableSnapshotContext> snapContext, ui64 targetTabletId)
        : SnapBody(snapBody)
        , SnapContext(std::move(snapContext))
        , TargetTabletId(targetTabletId)
    {}

    bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
        // BorrowSnapshot needs Executor access. Use the Env's LoanTable
        // approach instead - just collect parts info.
        // For this test, we use the standard borrow path.
        SnapBody.clear();
        // Note: BorrowSnapshot is on TExecutor, not accessible from ITransaction.
        // We need ITransactionWithExecutor. Since that's defined in flat_executor_ut.cpp,
        // we'll skip the borrow step and test LoanTable directly with compaction.
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
};

// LoanTable: inject borrowed parts into the destination tablet
struct TTxLoanSnapshot : public ITransaction {
    TString SnapBody;

    TTxLoanSnapshot(TString snapBody)
        : SnapBody(std::move(snapBody))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (!SnapBody.empty()) {
            txc.Env.LoanTable(TableId, SnapBody);
        }
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        return true;
    }

    void Complete(const TActorContext&) override {}
};

// Read all rows and produce a string for verification
struct TTxReadAllRows : public ITransaction {
    TString& Result;

    TTxReadAllRows(TString& result) : Result(result) {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Result.clear();

        TVector<NTable::TTag> tags;
        tags.push_back(ColumnKeyId);
        tags.push_back(ColumnValueId);

        auto iter = txc.DB.IterateRange(TableId, {}, tags);

        while (iter->Next(NTable::ENext::All) == NTable::EReady::Data) {
            const auto& key = iter->Row().Get(0);
            const auto& value = iter->Row().Get(1);

            if (!key.IsNull()) {
                i64 k = key.AsValue<i64>();
                TStringBuf v = value.IsNull() ? TStringBuf("null") : value.AsBuf();
                Result += TStringBuilder() << "key=" << k << " value=" << v << "\n";
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(PhysicalBackup) {

    // Test that EnumerateTableParts returns correct metadata after compaction
    Y_UNIT_TEST(EnumeratePartsAfterCompaction) {
        TMyEnvBase env;

        env.FireDummyTablet();

        // Create schema
        env.SendSync(new NFake::TEvExecute{new TTxSchema(new TCompactionPolicy())});

        // Insert rows
        env.SendSync(new NFake::TEvExecute{new TTxAddRows(1, 50)});

        // Force memtable compaction to create SST parts
        env.SendSync(new NFake::TEvCompact(TableId, true /* memOnly */));
        env.WaitFor<NFake::TEvCompacted>();

        // Enumerate and verify
        ui32 partCount = 0;
        ui64 totalRows = 0;
        ui64 totalBytes = 0;
        env.SendSync(new NFake::TEvExecute{
            new TTxEnumerateParts(partCount, totalRows, totalBytes)});

        Cerr << "Parts: " << partCount
             << ", Rows: " << totalRows
             << ", Bytes: " << totalBytes << Endl;

        UNIT_ASSERT_GE(partCount, 1u);
        UNIT_ASSERT_VALUES_EQUAL(totalRows, 50u);
        UNIT_ASSERT_GT(totalBytes, 0u);
    }

    // Test that we can serialize TBundle protos and collect blob IDs
    // (the core of physical export)
    Y_UNIT_TEST(SerializeBundleAndCollectBlobs) {
        TMyEnvBase env;

        env.FireDummyTablet();
        env.SendSync(new NFake::TEvExecute{new TTxSchema(new TCompactionPolicy())});
        env.SendSync(new NFake::TEvExecute{new TTxAddRows(1, 100)});

        // Compact to create SST parts
        env.SendSync(new NFake::TEvCompact(TableId, true));
        env.WaitFor<NFake::TEvCompacted>();

        // Insert more rows and compact again (creates multiple parts)
        env.SendSync(new NFake::TEvExecute{new TTxAddRows(101, 100)});
        env.SendSync(new NFake::TEvCompact(TableId, true));
        env.WaitFor<NFake::TEvCompacted>();

        // Enumerate and verify serialization
        ui32 partCount = 0;
        ui64 totalRows = 0;
        ui64 totalBytes = 0;
        env.SendSync(new NFake::TEvExecute{
            new TTxEnumerateParts(partCount, totalRows, totalBytes)});

        Cerr << "Parts: " << partCount
             << ", Rows: " << totalRows
             << ", Bytes: " << totalBytes << Endl;

        UNIT_ASSERT_GE(partCount, 2u); // At least 2 parts from 2 compactions
        UNIT_ASSERT_VALUES_EQUAL(totalRows, 200u);
    }

    // Test reading rows back to establish baseline
    Y_UNIT_TEST(ReadRowsBaseline) {
        TMyEnvBase env;

        env.FireDummyTablet();
        env.SendSync(new NFake::TEvExecute{new TTxSchema(new TCompactionPolicy())});
        env.SendSync(new NFake::TEvExecute{new TTxAddRows(1, 10)});

        TString data;
        env.SendSync(new NFake::TEvExecute{new TTxReadAllRows(data)});

        ui32 lineCount = 0;
        for (char c : data) {
            if (c == '\n') lineCount++;
        }

        UNIT_ASSERT_VALUES_EQUAL(lineCount, 10u);
        UNIT_ASSERT(data.Contains("key=1 "));
        UNIT_ASSERT(data.Contains("key=10 "));
        UNIT_ASSERT(data.Contains("value_1"));
    }

    // Test LoanTable injection (the core of physical restore)
    // Uses the NFake::TDummy tablet's built-in snapshot/borrow support
    Y_UNIT_TEST(LoanTableRestoreRoundTrip) {
        TMyEnvBase env;

        // ================================================================
        // Source tablet: create table, insert rows, compact
        // ================================================================
        Cerr << "=== Source tablet ===" << Endl;
        env.FireDummyTablet();
        env.SendSync(new NFake::TEvExecute{new TTxSchema(new TCompactionPolicy())});
        env.SendSync(new NFake::TEvExecute{new TTxAddRows(1, 50)});

        // Compact to SST parts
        env.SendSync(new NFake::TEvCompact(TableId, true));
        env.WaitFor<NFake::TEvCompacted>();

        // Read source data for comparison
        TString sourceData;
        env.SendSync(new NFake::TEvExecute{new TTxReadAllRows(sourceData)});
        Cerr << "Source rows: " << CountOf(sourceData, '\n') << Endl;
        UNIT_ASSERT(!sourceData.empty());

        // ================================================================
        // BorrowSnapshot via NFake::TEvCall (accesses Executor directly)
        // ================================================================
        Cerr << "=== BorrowSnapshot ===" << Endl;
        TString snapBody;
        TIntrusivePtr<TTableSnapshotContext> snapContext;

        // Make snapshot
        env.SendSync(new NFake::TEvExecute{new TTxMakeSnapshot(snapContext)});
        // Wait for compaction triggered by snapshot
        env.WaitFor<NFake::TEvCompacted>();

        // Borrow via TEvCall which gives us direct Executor access
        env.SendSync(new NFake::TEvCall([&](NFake::TEvCall::IExecutor* executor, const TActorContext&) {
            snapBody = executor->BorrowSnapshot(TableId, *snapContext, {}, {}, env.Tablet + 1);
        }));

        Cerr << "Borrow proto size: " << snapBody.size() << " bytes" << Endl;
        UNIT_ASSERT(!snapBody.empty());

        // ================================================================
        // Stop source, start destination tablet
        // ================================================================
        Cerr << "=== Destination tablet ===" << Endl;
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.WaitForGone();

        ++env.Tablet;
        env.FireDummyTablet();
        env.SendSync(new NFake::TEvExecute{new TTxSchema(new TCompactionPolicy())});

        // ================================================================
        // LoanTable: inject parts (physical restore)
        // ================================================================
        Cerr << "=== LoanTable (physical restore) ===" << Endl;
        env.SendSync(new NFake::TEvExecute{new TTxLoanSnapshot(snapBody)});

        // Wait for part switch to complete
        env->SimulateSleep(TDuration::MilliSeconds(100));

        // ================================================================
        // Verify restored data matches source
        // ================================================================
        Cerr << "=== Verify ===" << Endl;
        TString destData;
        env.SendSync(new NFake::TEvExecute{new TTxReadAllRows(destData)});
        Cerr << "Dest rows: " << CountOf(destData, '\n') << Endl;

        UNIT_ASSERT_VALUES_EQUAL(sourceData, destData);
        Cerr << "=== Round-trip PASSED ===" << Endl;
    }

} // Y_UNIT_TEST_SUITE

} // namespace NTabletFlatExecutor
} // namespace NKikimr
