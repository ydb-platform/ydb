#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>

#include <ydb/core/tablet_flat/flat_ops_compact.h>
#include <ydb/core/tablet_flat/flat_scan_feed.h>

#include <ydb/core/base/fulltext.h>
#include <ydb/core/base/table_index.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

using namespace NTest;

namespace {

    // Column tags for fulltext index table
    constexpr ui32 TagToken = 1;
    constexpr ui32 TagGen = 2;
    constexpr ui32 TagMaxId = 3;
    constexpr ui32 TagAdded = 4;
    constexpr ui32 TagSegment = 5;
    constexpr ui32 TagPrefix = 6;

    // Build a delta-encoded segment from a sorted list of doc IDs
    TString MakeSegment(const TVector<ui64>& docIds, bool withFreq = false, bool sign = false) {
        NFulltext::TDeltaWriter writer;
        writer.Reset(withFreq, sign);
        for (ui64 id : docIds) {
            writer.Add(id, 1);
        }
        auto buf = writer.GetBuf();
        return TString((const char*)buf.data(), buf.size());
    }

    // Read doc IDs back from a delta-encoded segment
    TVector<ui64> ReadSegment(TStringBuf data, bool withFreq = false, bool sign = false) {
        NFulltext::TDeltaReader reader(
            TConstArrayRef<ui8>((const ui8*)data.data(), data.size()), withFreq, sign);
        TVector<ui64> result;
        ui64 docId;
        ui32 freq;
        while (reader.Read(docId, freq)) {
            result.push_back(docId);
        }
        return result;
    }

    NTest::TLayoutCook MakeFulltextLayout() {
        NTest::TLayoutCook lay;
        lay
            .Col(0, TagToken,   NScheme::NTypeIds::String)
            .Col(0, TagGen,     NScheme::NTypeIds::Uint64)
            .Col(0, TagMaxId,   NScheme::NTypeIds::Uint64)
            .Col(0, TagAdded,   NScheme::NTypeIds::Bool)
            .Col(0, TagSegment, NScheme::NTypeIds::String)
            .Key({ TagToken, TagGen, TagMaxId });
        return lay;
    }

    NTest::TLayoutCook MakePrefixedFulltextLayout() {
        NTest::TLayoutCook lay;
        lay
            .Col(0, TagPrefix,  NScheme::NTypeIds::String)
            .Col(0, TagToken,   NScheme::NTypeIds::String)
            .Col(0, TagGen,     NScheme::NTypeIds::Uint64)
            .Col(0, TagMaxId,   NScheme::NTypeIds::Uint64)
            .Col(0, TagAdded,   NScheme::NTypeIds::Bool)
            .Col(0, TagSegment, NScheme::NTypeIds::String)
            .Key({ TagPrefix, TagToken, TagGen, TagMaxId });
        return lay;
    }

    // Test compaction helper that routes through TFulltextCompact
    class TFulltextCompaction : protected IVersionScan {
        class TMyScan : public TFeed {
        public:
            TMyScan(IScan *scan, const TSubset &subset, IPages *env)
                : TFeed(scan, subset)
                , Env(env)
            {
                Conf.NoErased = false;
            }

            IPages* MakeEnv() override { return Env; }

            TPartView LoadPart(const TIntrusiveConstPtr<TColdPart>&) override {
                Y_ABORT("not supported");
            }

            IPages * const Env = nullptr;
        };

    public:
        TFulltextCompaction(bool isFinal, ui32 maxSegment = 10000)
            : IsFinal_(isFinal)
            , MaxSegment_(maxSegment)
            , Env(new NTest::TTestEnv)
        {}

        TPartEggs Do(const TPartEggs &eggs, TRowVersion minVer) {
            return Do(eggs.Scheme, { &eggs }, minVer);
        }

        TPartEggs Do(TIntrusiveConstPtr<TRowScheme> scheme, TVector<const TPartEggs*> eggs, TRowVersion minVer) {
            TVector<TPartView> partView;
            for (auto &one : eggs) {
                for (const auto &part : one->Parts) {
                    Y_ENSURE(part->Slices, "Missing part slices");
                    partView.push_back({ part, nullptr, part->Slices });
                }
            }
            return Do(TSubset(TEpoch::Zero(), std::move(scheme), std::move(partView)), minVer);
        }

        TPartEggs Do(const TSubset &subset, TRowVersion minVer) {
            auto logo = TLogoBlobID(0, 0, ++Serial, 0, 0, 0);
            auto *scheme = new TPartScheme(subset.Scheme->Cols);

            NPage::TConf conf;
            conf.MaxRows = subset.MaxRows();
            conf.MinRowVersion = minVer;
            conf.Final = IsFinal_;

            NTest::TWriterBundle blocks(scheme->Groups.size(), logo);

            Tags = subset.Scheme->Tags();
            Writer = new TPartWriter(scheme, Tags, blocks, conf, subset.Epoch());

            // Set up TCompactCfg for fulltext
            auto params = MakeHolder<TCompactionParams>();
            params->IsFinal = IsFinal_;
            CompactCfg = MakeHolder<NTabletFlatExecutor::TCompactCfg>(std::move(params));
            CompactCfg->IsFulltextCompact = true;
            CompactCfg->FulltextAddedTag = TagAdded;
            CompactCfg->FulltextSegmentTag = TagSegment;
            CompactCfg->FulltextMaxSegment = MaxSegment_;
            CompactCfg->FulltextKeySize = 8;
            CompactCfg->FulltextWithRelevance = false;
            CompactCfg->FulltextKeySigned = false;
            CompactCfg->Layout.MinRowVersion = minVer;

            FtState = NTabletFlatExecutor::TFulltextCompact::Init(
                CompactCfg.Get(), subset.Scheme, Writer);
            Y_ENSURE(FtState, "TFulltextCompact::Init failed");

            TMyScan scanner(this, subset, Env.Get());

            while (true) {
                const auto ready = scanner.Process();
                if (ready == EReady::Data) {
                    // continue
                } else if (ready == EReady::Gone) {
                    FtState->FlushFulltextToken();
                    auto eggs = blocks.Flush(subset.Scheme, Writer->Finish());
                    return eggs;
                } else {
                    Y_ABORT("Unexpected scan result %d", (int)ready);
                }
            }
        }

    private:
        TInitialState Prepare(IDriver*, TIntrusiveConstPtr<TScheme>) override {
            Y_ABORT("Not used in test env");
        }

        EScan Seek(TLead &lead, ui64 seq) override {
            Y_ENSURE(seq < 2, "Too many Seek() calls");
            lead.To(Tags, { }, ESeek::Lower);
            return seq == 0 ? EScan::Feed : EScan::Final;
        }

        EScan BeginKey(TArrayRef<const TCell> key) override {
            FtState->BeginKey(key);
            return EScan::Feed;
        }

        EScan BeginDeltas() override {
            return EScan::Feed;
        }

        EScan Feed(const TRow &row, ui64 txId) override {
            auto res = Deltas.try_emplace(txId, row);
            if (res.second) {
                DeltasOrder.emplace_back(txId);
            } else if (!res.first->second.IsFinalized()) {
                res.first->second.Merge(row);
            }
            return EScan::Feed;
        }

        EScan Feed(ELockMode mode, ui64 txId) override {
            if (!IsLocked) {
                FtState->SetLock(mode, txId);
                IsLocked = true;
            }
            return EScan::Feed;
        }

        EScan EndDeltas() override {
            FtState->SaveDeltas(Deltas, DeltasOrder);
            return EScan::Feed;
        }

        EScan Feed(const TRow &row, TRowVersion &rowVersion) override {
            FtState->SaveVersion(row, rowVersion);
            return EScan::Feed;
        }

        EScan EndKey() override {
            FtState->EndKey();
            IsLocked = false;
            return EScan::Feed;
        }

        TAutoPtr<IDestructable> Finish(EStatus) override {
            Y_ABORT("Not used in test env");
        }

        void Describe(IOutputStream &out) const override {
            out << "FulltextCompact{test}";
        }

        bool IsFinal_;
        ui32 MaxSegment_;
        ui32 Serial = 0;
        TAutoPtr<IPages> Env;
        TVector<ui32> Tags;
        TIntrusivePtr<TPartWriter> Writer;
        THolder<NTabletFlatExecutor::TCompactCfg> CompactCfg;
        std::unique_ptr<NTabletFlatExecutor::TFulltextCompact> FtState;
        THashMap<ui64, TRow> Deltas;
        TSmallVec<ui64> DeltasOrder;
        bool IsLocked = false;
    };

    // Struct to hold a decoded fulltext row
    struct TFtRow {
        ERowOp RowOp = ERowOp::Upsert;
        TString Prefix;
        TString Token;
        ui64 Gen = 0;
        ui64 MaxId = 0;
        bool Added = true;
        TVector<ui64> DocIds;
    };

    // Read all fulltext rows from compacted output
    TVector<TFtRow> ReadFulltextRows(const TPartEggs& eggs) {
        TVector<TFtRow> rows;

        NTest::TWrapPart wrap(eggs);
        wrap.Make(new NTest::TTestEnv);

        auto ready = wrap.Seek(TArrayRef<const TCell>(), ESeek::Lower);
        while (ready == EReady::Data) {
            const auto& state = wrap.Apply();
            TFtRow row;
            row.RowOp = state.GetRowState();

            // Key columns are set via KeyPins during RollUp
            // Read from state by position
            auto& scheme = *eggs.Scheme;
            for (const auto& col : scheme.Cols) {
                const auto& cell = state.Get(col.Pos);
                if (col.Tag == TagToken && !cell.IsNull()) {
                    row.Token = TString(cell.Data(), cell.Size());
                } else if (col.Tag == TagPrefix && !cell.IsNull()) {
                    row.Prefix = TString(cell.Data(), cell.Size());
                } else if (col.Tag == TagGen && !cell.IsNull()) {
                    row.Gen = cell.AsValue<ui64>();
                } else if (col.Tag == TagMaxId && !cell.IsNull()) {
                    row.MaxId = cell.AsValue<ui64>();
                } else if (col.Tag == TagAdded && !cell.IsNull()) {
                    row.Added = *reinterpret_cast<const bool*>(cell.Data());
                } else if (col.Tag == TagSegment && !cell.IsNull()) {
                    row.DocIds = ReadSegment(TStringBuf(cell.Data(), cell.Size()));
                }
            }
            rows.push_back(std::move(row));
            ready = wrap.Next();
        }
        return rows;
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(TFulltextCompaction) {

    Y_UNIT_TEST(NonFinalAddedOnly) {
        auto lay = MakeFulltextLayout();

        auto seg1 = MakeSegment({1, 5, 15});
        auto seg2 = MakeSegment({10, 11, 20});
        auto seg3 = MakeSegment({100, 200});
        auto seg4 = MakeSegment({300, 400});
        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({50, 1}).AddN(TString("hello"), 100_u64, 15_u64, true, seg1)
            .Ver({100, 1}).AddN(TString("hello"), 101_u64, 20_u64, true, seg2)
            // erased key - should be skipped because it's mergeable
            // (it just means some bad guy added and removed a delta segment)
            .Ver({100, 1}).AddOpN(ERowOp::Erase, TString("hello"), 102_u64, 20_u64)
            .Ver({80, 1}).AddN(TString("world"), 200_u64, 200_u64, true, seg3)
            .Ver({90, 1}).AddN(TString("world"), 201_u64, 400_u64, true, seg4)
            .Finish();

        auto result = TFulltextCompaction(false /* non-final */).Do(part, {100, 1});
        auto rows = ReadFulltextRows(result);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(rows[0].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Gen, 100); // max
        UNIT_ASSERT_VALUES_EQUAL(rows[0].MaxId, 20);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Added, true);
        UNIT_ASSERT(rows[0].DocIds == (TVector<ui64>{1, 5, 10, 11, 15, 20}));

        UNIT_ASSERT_VALUES_EQUAL(rows[1].Token, "world");
        UNIT_ASSERT_VALUES_EQUAL(rows[1].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Gen, 200);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].MaxId, 400);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Added, true);
        UNIT_ASSERT(rows[1].DocIds == (TVector<ui64>{100, 200, 300, 400}));
    }

    Y_UNIT_TEST(NonFinalPrefixed) {
        auto lay = MakePrefixedFulltextLayout();

        auto seg1 = MakeSegment({1, 5, 15});
        auto seg2 = MakeSegment({10, 11, 20});
        auto seg3 = MakeSegment({100, 200});
        auto seg4 = MakeSegment({300, 400});
        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({50, 1}).AddN(TString("hello"), 100_u64, 15_u64, true, seg1, TString("user1"))
            .Ver({100, 1}).AddN(TString("hello"), 101_u64, 20_u64, true, seg2, TString("user1"))
            .Ver({80, 1}).AddN(TString("hello"), 200_u64, 200_u64, true, seg3, TString("user2"))
            .Ver({90, 1}).AddN(TString("hello"), 201_u64, 400_u64, true, seg4, TString("user2"))
            .Finish();

        auto result = TFulltextCompaction(false /* non-final */).Do(part, {100, 1});
        auto rows = ReadFulltextRows(result);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(rows[0].Prefix, "user1");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Gen, 100); // min
        UNIT_ASSERT_VALUES_EQUAL(rows[0].MaxId, 20);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Added, true);
        UNIT_ASSERT(rows[0].DocIds == (TVector<ui64>{1, 5, 10, 11, 15, 20}));

        UNIT_ASSERT_VALUES_EQUAL(rows[1].Prefix, "user2");
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[1].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Gen, 200);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].MaxId, 400);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Added, true);
        UNIT_ASSERT(rows[1].DocIds == (TVector<ui64>{100, 200, 300, 400}));
    }

    Y_UNIT_TEST(NonMergeable) {
        auto lay = MakeFulltextLayout();

        auto seg1 = MakeSegment({1, 5, 15});
        auto seg2 = MakeSegment({10, 11, 20});
        auto seg3 = MakeSegment({2, 4, 50});
        auto part = NTest::TPartCook(lay, { false, 4096 })
            // non-mergeable erase
            .Ver({101, 1}).AddOpN(ERowOp::Erase, TString("hello"), UINT64_MAX-3, 20_u64)
            .Ver({100, 1}).AddN(TString("hello"), UINT64_MAX-2, 15_u64, true, seg1)
            // non-mergeable delete
            .Ver({101, 1}).AddN(TString("hello"), UINT64_MAX-1, 20_u64, false, seg2)
            .Ver({100, 1}).AddN(TString("hello"), UINT64_MAX, 50_u64, true, seg3)
            .Finish();

        auto result = TFulltextCompaction(false).Do(part, {100, 1});
        auto rows = ReadFulltextRows(result);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);

        // non-mergeable erase preserved
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].RowOp, ERowOp::Erase);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Gen, UINT64_MAX-3);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].MaxId, 20);
        UNIT_ASSERT(rows[0].DocIds.empty());

        // mergeable rows merged into a single insert
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[1].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Gen, UINT64_MAX-2);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].MaxId, 50);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Added, true);
        UNIT_ASSERT(rows[1].DocIds == (TVector<ui64>{1, 2, 4, 5, 15, 50}));

        // non-mergeable delete preserved
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[2].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Gen, UINT64_MAX-1);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].MaxId, 20);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Added, false);
        UNIT_ASSERT(rows[2].DocIds == (TVector<ui64>{10, 11, 20}));
    }

    Y_UNIT_TEST(NonMergeableBetweenAddDel) {
        auto lay = MakeFulltextLayout();

        auto seg1 = MakeSegment({1, 5, 15});
        auto seg2 = MakeSegment({10, 11, 20});
        auto seg3 = MakeSegment({2, 4, 50});
        auto seg4 = MakeSegment({21, 30});
        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({100, 1}).AddN(TString("hello"), UINT64_MAX-3, 15_u64, false, seg1)
            .Ver({101, 1}).AddN(TString("hello"), UINT64_MAX-2, 20_u64, true, seg2)
            .Ver({100, 1}).AddN(TString("hello"), UINT64_MAX-1, 50_u64, true, seg3)
            .Ver({100, 1}).AddN(TString("hello"), UINT64_MAX, 30_u64, true, seg4)
            .Finish();

        auto result = TFulltextCompaction(false).Do(part, {100, 1});
        auto rows = ReadFulltextRows(result);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);

        // deletes first
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Gen, UINT64_MAX-3);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].MaxId, 15);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Added, false);
        UNIT_ASSERT(rows[0].DocIds == (TVector<ui64>{1, 5, 15}));

        // preserved row
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[1].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Gen, UINT64_MAX-2);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].MaxId, 20);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Added, true);
        UNIT_ASSERT(rows[1].DocIds == (TVector<ui64>{10, 11, 20}));

        // merged additions
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[2].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Gen, UINT64_MAX-1);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].MaxId, 50);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Added, true);
        UNIT_ASSERT(rows[2].DocIds == (TVector<ui64>{2, 4, 21, 30, 50}));
    }

    Y_UNIT_TEST(NonMergeableMaxGenAbortsMerge) {
        auto lay = MakeFulltextLayout();

        auto seg1 = MakeSegment({1, 5, 15});
        auto seg2 = MakeSegment({10, 11, 20});
        auto seg3 = MakeSegment({2, 4, 50});
        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({100, 1}).AddN(TString("hello"), UINT64_MAX-2, 15_u64, true, seg1)
            .Ver({100, 1}).AddN(TString("hello"), UINT64_MAX-1, 20_u64, true, seg2)
            // non-mergeable manual table modification
            .Ver({101, 1}).AddN(TString("hello"), UINT64_MAX, 50_u64, true, seg3)
            .Finish();

        auto result = TFulltextCompaction(false).Do(part, {100, 1});
        auto rows = ReadFulltextRows(result);

        // all rows preserved - manual modification aborts compaction
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);

        UNIT_ASSERT_VALUES_EQUAL(rows[0].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Gen, UINT64_MAX-2);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].MaxId, 15);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Added, true);
        UNIT_ASSERT(rows[0].DocIds == (TVector<ui64>{1, 5, 15}));

        UNIT_ASSERT_VALUES_EQUAL(rows[1].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[1].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Gen, UINT64_MAX-1);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].MaxId, 20);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Added, true);
        UNIT_ASSERT(rows[1].DocIds == (TVector<ui64>{10, 11, 20}));

        UNIT_ASSERT_VALUES_EQUAL(rows[2].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[2].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Gen, UINT64_MAX);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].MaxId, 50);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Added, true);
        UNIT_ASSERT(rows[2].DocIds == (TVector<ui64>{2, 4, 50}));
    }

    Y_UNIT_TEST(ErasedMaxGenAbortsMerge) {
        auto lay = MakeFulltextLayout();

        auto seg1 = MakeSegment({1, 5, 15});
        auto seg2 = MakeSegment({2, 4, 50});
        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({100, 1}).AddN(TString("hello"), UINT64_MAX-2, 15_u64, true, seg1)
            // mergeable manual table modification but with an erase
            .Ver({100, 1}).AddOpN(ERowOp::Erase, TString("hello"), UINT64_MAX, 20_u64)
            .Ver({100, 1}).AddN(TString("hello"), UINT64_MAX, 50_u64, true, seg2)
            .Finish();

        auto result = TFulltextCompaction(false).Do(part, {100, 1});
        auto rows = ReadFulltextRows(result);

        // all rows preserved - erased gen==max rows at non-final level abort compaction
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);

        UNIT_ASSERT_VALUES_EQUAL(rows[0].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Gen, UINT64_MAX-2);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].MaxId, 15);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Added, true);
        UNIT_ASSERT(rows[0].DocIds == (TVector<ui64>{1, 5, 15}));

        UNIT_ASSERT_VALUES_EQUAL(rows[1].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[1].RowOp, ERowOp::Erase);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Gen, UINT64_MAX);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].MaxId, 20);
        UNIT_ASSERT(rows[1].DocIds.empty());

        UNIT_ASSERT_VALUES_EQUAL(rows[2].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[2].RowOp, ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Gen, UINT64_MAX);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].MaxId, 50);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].Added, true);
        UNIT_ASSERT(rows[2].DocIds == (TVector<ui64>{2, 4, 50}));
    }

    Y_UNIT_TEST(NonFinalWithDeletes) {
        auto lay = MakeFulltextLayout();

        auto segAdded1 = MakeSegment({1, 5, 10, 100});
        auto segAdded2 = MakeSegment({101, 150, 200});
        auto segRemoved1 = MakeSegment({5, 50});
        auto segRemoved2 = MakeSegment({10, 150});

        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({100, 1}).AddN(TString("hello"), 10_u64, 100_u64, true, segAdded1)
            .Ver({50, 1}).AddN(TString("hello"), 20_u64, 50_u64, false, segRemoved1)
            .Ver({80, 1}).AddN(TString("hello"), 40_u64, 150_u64, false, segRemoved2)
            // Assume the user is a bad guy and inserted a segment with gen=max manually
            .Ver({60, 1}).AddN(TString("hello"), UINT64_MAX, 200_u64, true, segAdded2)
            .Finish();

        auto result = TFulltextCompaction(false /* non-final */).Do(part, {100, 1});
        auto rows = ReadFulltextRows(result);

        // Should produce exactly 2 rows: merged added + merged removed
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(rows[0].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Gen, 10);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].MaxId, 150);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Added, false);
        UNIT_ASSERT(rows[0].DocIds == (TVector<ui64>{5, 10, 50, 150}));

        UNIT_ASSERT_VALUES_EQUAL(rows[1].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Gen, 20);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].MaxId, 200);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Added, true);
        UNIT_ASSERT(rows[1].DocIds == (TVector<ui64>{1, 5, 10, 100, 101, 150, 200}));
    }

    Y_UNIT_TEST(FinalWithDeletes) {
        auto lay = MakeFulltextLayout();

        auto segAdded1 = MakeSegment({1, 5, 10, 100});
        auto segAdded2 = MakeSegment({101, 150, 200});
        auto segRemoved1 = MakeSegment({5, 50});
        auto segRemoved2 = MakeSegment({10, 150});

        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({100, 1}).AddN(TString("hello"), 10_u64, 100_u64, true, segAdded1)
            .Ver({50, 1}).AddN(TString("hello"), 20_u64, 50_u64, false, segRemoved1)
            .Ver({60, 1}).AddN(TString("hello"), 30_u64, 200_u64, true, segAdded2)
            .Ver({80, 1}).AddN(TString("hello"), 40_u64, 150_u64, false, segRemoved2)
            .Ver({90, 1}).AddOpN(ERowOp::Erase, TString("hello"), UINT64_MAX, 20_u64)
            .Finish();

        auto result = TFulltextCompaction(true /* final */).Do(part, {100, 1});
        auto rows = ReadFulltextRows(result);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].MaxId, 200);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Added, true);
        UNIT_ASSERT(rows[0].DocIds == (TVector<ui64>{1, 100, 101, 200}));
    }

    Y_UNIT_TEST(NonFinalMultipleParts) {
        auto lay = MakeFulltextLayout();

        auto seg1 = MakeSegment({1, 5, 10});
        auto seg2 = MakeSegment({11, 20, 30});

        auto part1 = NTest::TPartCook(lay, { false, 4096 }, {}, TEpoch::FromIndex(1))
            .Ver({50, 1}).AddN(TString("hello"), 100_u64, 10_u64, true, seg1)
            .Finish();

        auto part2 = NTest::TPartCook(lay, { false, 4096 }, {}, TEpoch::FromIndex(2))
            .Ver({100, 1}).AddN(TString("hello"), 101_u64, 30_u64, true, seg2)
            .Finish();

        auto result = TFulltextCompaction(false).Do(lay.RowScheme(), { &part1, &part2 }, {100, 1});
        auto rows = ReadFulltextRows(result);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Token, "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].MaxId, 30);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Added, true);
        UNIT_ASSERT(rows[0].DocIds == (TVector<ui64>{1, 5, 10, 11, 20, 30}));
    }

    Y_UNIT_TEST(NonFinalGenSwapWhenNeeded) {
        auto lay = MakeFulltextLayout();

        auto segAdded = MakeSegment({10, 20});
        auto segRemoved = MakeSegment({1, 5});

        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({100, 1}).AddN(TString("hello"), 30_u64, 20_u64, true, segAdded)
            .Ver({100, 1}).AddN(TString("hello"), 50_u64, 20_u64, false, segRemoved)
            .Finish();

        auto result = TFulltextCompaction(false).Do(part, {100, 1});
        auto rows = ReadFulltextRows(result);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);

        // Removed should have smaller gen
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Gen, 30);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Added, false);
        UNIT_ASSERT(rows[0].DocIds == (TVector<ui64>{1, 5}));

        UNIT_ASSERT_VALUES_EQUAL(rows[1].Gen, 50);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Added, true);
        UNIT_ASSERT(rows[1].DocIds == (TVector<ui64>{10, 20}));
    }

}

} // namespace NTable
} // namespace NKikimr
