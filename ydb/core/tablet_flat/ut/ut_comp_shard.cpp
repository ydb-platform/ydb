#include "flat_comp_ut_common.h"

#include <ydb/core/tablet_flat/flat_comp_shard.h>

#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/rows/heap.h>
#include <ydb/core/tablet_flat/test/libs/rows/rows.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_comp.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include <ydb/core/tablet_flat/protos/flat_table_shard.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/util/pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <optional>

namespace NKikimr {
namespace NTable {
namespace NCompShard {

using namespace NTest;

namespace {

    /**
     * A special kind of TTestEnv that will fail loading every page until
     * Lock() call, and then would only pass previously requested pages
     */
    class TStrictEnv : public TTestEnv {
    public:
        const TSharedData *TryGetPage(const TPart *part, TPageId ref, TGroupId groupId) override {
            ui64 token = ref | (ui64(groupId.Raw()) << 32);
            if (Locked) {
                if (auto* info = Parts.FindPtr(part)) {
                    if (info->Seen.contains(token)) {
                        return TTestEnv::TryGetPage(part, ref, groupId);
                    }
                }
            } else {
                Parts[part].Seen.insert(token);
            }

            return nullptr;
        }

        void Lock() {
            Locked = true;
        }

    private:
        struct TPartInfo {
            THashSet<ui64> Seen;
        };

    private:
        bool Locked = false;
        THashMap<const TPart*, TPartInfo> Parts;
    };

    struct TSimpleConsumer : public ISliceSplitResultConsumer {
        std::optional<TSliceSplitResult> Result;

        void OnSliceSplitResult(TSliceSplitResult result) override {
            Y_VERIFY(!Result, "Cannot store multiple results");
            Result.emplace(std::move(result));
        }
    };

    NPage::TConf CreateConf(ui32 rowsPerPage) {
        NPage::TConf conf(false, 8192);
        conf.Group(0).PageRows = rowsPerPage;
        return conf;
    }

    NPage::TConf CreateConf(ui32 rowsPerPage, NPage::IKeySpace* underlayMask) {
        NPage::TConf conf = CreateConf(rowsPerPage);
        conf.UnderlayMask = underlayMask;
        return conf;
    }

    NPage::TConf CreateConf(ui32 rowsPerPage, NPage::ISplitKeys* splitKeys) {
        NPage::TConf conf = CreateConf(rowsPerPage);
        conf.SplitKeys = splitKeys;
        return conf;
    }

    TPartView CreatePart(const TLayoutCook& lay, const TRowsHeap& rows, ui32 rowsPerPage) {
        return TPartCook(lay, CreateConf(rowsPerPage))
            .Add(rows.begin(), rows.end())
            .Finish()
            .ToPartView();
    }

    struct TSizeChange {
        ui64 AddLeftSize = 0;
        ui64 AddLeftRows = 0;
        ui64 SubRightSize = 0;
        ui64 SubRightRows = 0;

        TSizeChange& AddLeft(ui64 leftSize, ui64 leftRows) {
            AddLeftSize += leftSize;
            AddLeftRows += leftRows;
            return *this;
        }

        TSizeChange& SubRight(ui64 rightSize, ui64 rightRows) {
            SubRightSize += rightSize;
            SubRightRows += rightRows;
            return *this;
        }

        TSizeChange& Expect() {
            return *this;
        }
    };

    using TSizeChanges = TMap<ui64, TSizeChange>;

    struct TSizeStat {
        ui64 Key;
        ui64 LeftSize;
        ui64 LeftRows;
        ui64 RightSize;
        ui64 RightRows;

        bool operator==(const TSizeStat& o) const {
            return (
                Key == o.Key &&
                LeftSize == o.LeftSize &&
                LeftRows == o.LeftRows &&
                RightSize == o.RightSize &&
                RightRows == o.RightRows);
        }

        friend IOutputStream& operator<<(IOutputStream& out, const TSizeStat& v) {
            out << "key=" << v.Key
                << " left=" << v.LeftSize << "b/" << v.LeftRows << "r"
                << " right=" << v.RightSize << "b/" << v.RightRows << "r";
            return out;
        }

        friend IOutputStream& operator<<(IOutputStream& out, const TVector<TSizeStat>& v) {
            out << '{';
            bool first = true;
            for (auto& value : v) {
                if (first) {
                    first = false;
                } else {
                    out << ',';
                }
                out << ' ';
                out << value;
            }
            out << " }";
            return out;
        }
    };

    void VerifySizeChanges(TSplitStatIterator& it, const TSizeChanges& sizeChanges) {
        TVector<TSizeStat> expected;
        {
            ui64 leftSize = 0;
            ui64 leftRows = 0;
            ui64 rightSize = it.RightSize();
            ui64 rightRows = it.RightRows();
            for (auto& kv : sizeChanges) {
                leftSize += kv.second.AddLeftSize;
                leftRows += kv.second.AddLeftRows;
                rightSize -= kv.second.SubRightSize;
                rightRows -= kv.second.SubRightRows;
                expected.push_back({ kv.first, leftSize, leftRows, rightSize, rightRows });
            }
        }

        TVector<TSizeStat> actual;
        while (it.Next()) {
            auto keyCells = it.CurrentKey();
            UNIT_ASSERT_VALUES_EQUAL(keyCells.size(), 1u);
            auto key = keyCells[0].AsValue<ui64>();
            actual.push_back({ key, it.LeftSize(), it.LeftRows(), it.RightSize(), it.RightRows() });
        }

        auto getFirstDiffKey = [&]() -> ui64 {
            auto a = actual.begin();
            auto b = expected.begin();
            while (a != actual.end() && b != expected.end()) {
                if (!(*a == *b)) {
                    return Min(a->Key, b->Key);
                }
                ++a;
                ++b;
            }
            if (a != actual.end()) {
                return a->Key;
            }
            if (b != expected.end()) {
                return b->Key;
            }
            return Max<ui64>();
        };

        UNIT_ASSERT_C(actual == expected,
            "Diff at " << getFirstDiffKey()
            << ": got " << actual
            << ", expected " << expected);
    }

}

Y_UNIT_TEST_SUITE(TShardedCompaction) {

    Y_UNIT_TEST(SplitSingleKey) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 1,  NScheme::NTypeIds::Uint32)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0, 1 });

        TRowTool tool(*lay);
        TRowsHeap rows(64 * 1024);
        for (ui64 seq = 0; seq < 4*4; ++seq) {
            rows.Put(*TSchemedCookRow(*lay).Col(seq, 500_u32, 42_u32));
        }

        auto partView = CreatePart(lay, rows, 4);

        for (size_t beginRow = 0; beginRow < rows.Size(); ++beginRow) {
            for (size_t endRow = beginRow + 1; endRow <= rows.Size(); ++endRow) {
                for (ui64 flags = 0; flags < 4; ++flags) {
                    TSlice slice;
                    slice.FirstInclusive = (flags & 1) == 1;
                    slice.LastInclusive = (flags & 2) == 2;
                    if (!slice.FirstInclusive && beginRow == 0) {
                        continue;
                    }
                    if (!slice.LastInclusive && endRow == rows.Size()) {
                        continue;
                    }
                    if (!slice.FirstInclusive && !slice.LastInclusive && endRow - beginRow <= 1) {
                        continue;
                    }
                    slice.FirstRowId = beginRow - !slice.FirstInclusive;
                    slice.LastRowId = endRow - slice.LastInclusive;
                    slice.FirstKey = TSerializedCellVec(tool.KeyCells(rows[slice.FirstRowId]));
                    slice.LastKey = TSerializedCellVec(tool.KeyCells(rows[slice.LastRowId]));

                    for (ui64 splitRow = slice.FirstRowId; splitRow <= slice.LastRowId; ++splitRow) {
                        for (ui64 splitFlags = 0; splitFlags < 2; ++splitFlags) {
                            bool moveLeft = (splitFlags & 1) == 1;
                            if (splitRow == slice.FirstRowId && (moveLeft || !slice.FirstInclusive)) {
                                continue;
                            }

                            auto splitCells = tool.KeyCells(rows[splitRow]);
                            if (moveLeft) {
                                splitCells[1] = Cimple(0_u32);
                            }

                            TTableInfo table;
                            table.RowScheme = lay.RowScheme();
                            table.SplitKeys[1] = TSerializedCellVec(splitCells);
                            TTableShard left;
                            TTableShard right;
                            left.RightKey = 1;
                            right.LeftKey = 1;
                            TVector<TTableShard*> pshards;
                            pshards.push_back(&left);
                            pshards.push_back(&right);

                            TStrictEnv env;
                            TSimpleConsumer consumer;
                            TSliceSplitOp op(&consumer, &table, pshards, partView.Part, slice);

                            bool ok1 = op.Execute(&env);
                            UNIT_ASSERT_VALUES_EQUAL(ok1, false);

                            env.Lock();
                            bool ok2 = op.Execute(&env);
                            UNIT_ASSERT_VALUES_EQUAL(ok2, true);

                            auto& result = consumer.Result.value();
                            size_t pos = 0;
                            if (beginRow < splitRow) {
                                TSlice output = result.NewSlices.at(pos++);
                                UNIT_ASSERT_VALUES_EQUAL(output.BeginRowId(), beginRow);
                                UNIT_ASSERT_VALUES_EQUAL(output.EndRowId(), splitRow);
                                UNIT_ASSERT(output.LastInclusive);
                                if (output.Rows() == 1) {
                                    UNIT_ASSERT(output.FirstInclusive);
                                }
                            }
                            if (splitRow < endRow) {
                                TSlice output = result.NewSlices.at(pos++);
                                UNIT_ASSERT_VALUES_EQUAL(output.BeginRowId(), splitRow);
                                UNIT_ASSERT_VALUES_EQUAL(output.EndRowId(), endRow);
                                UNIT_ASSERT(output.FirstInclusive);
                                if (output.Rows() == 1) {
                                    UNIT_ASSERT(output.LastInclusive);
                                }
                            }
                            UNIT_ASSERT_VALUES_EQUAL(result.NewSlices.size(), pos);
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(SplitMiddleEdgeCase) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 1,  NScheme::NTypeIds::Uint32)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0, 1 });

        TRowTool tool(*lay);
        TRowsHeap rows(64 * 1024);
        for (ui64 seq = 0; seq < 4*4+2; ++seq) {
            rows.Put(*TSchemedCookRow(*lay).Col(seq, 500_u32, 42_u32));
        }

        auto partView = CreatePart(lay, rows, 4);

        TSlice slice;
        slice.FirstRowId = 0;
        slice.FirstInclusive = true;
        slice.LastRowId = rows.Size() - 1;
        slice.LastInclusive = true;
        slice.FirstKey = TSerializedCellVec(tool.KeyCells(rows[slice.FirstRowId]));
        slice.LastKey = TSerializedCellVec(tool.KeyCells(rows[slice.LastRowId]));

        TRowId splitRow = rows.Size() / 2;

        TTableInfo table;
        table.RowScheme = lay.RowScheme();
        TIntrusiveListWithAutoDelete<TTableShard, TDelete> shards;
        shards.PushBack(new TTableShard);
        for (ui32 k = 1; k <= 5; ++k) {
            auto cells = tool.KeyCells(rows[splitRow]);
            cells[1] = Cimple(k);
            table.SplitKeys[k] = TSerializedCellVec(cells);
            auto* left = shards.Back();
            shards.PushBack(new TTableShard);
            auto* right = shards.Back();
            left->RightKey = k;
            right->LeftKey = k;
        }
        TVector<TTableShard*> pshards;
        for (auto& shard : shards) {
            pshards.push_back(&shard);
        }

        TStrictEnv env;
        TSimpleConsumer consumer;
        TSliceSplitOp op(&consumer, &table, pshards, partView.Part, slice);

        bool ok1 = op.Execute(&env);
        UNIT_ASSERT_VALUES_EQUAL(ok1, false);

        env.Lock();
        bool ok2 = op.Execute(&env);
        UNIT_ASSERT_VALUES_EQUAL(ok2, true);

        auto& result = consumer.Result.value();
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[0].FirstRowId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[0].LastRowId, splitRow-1);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[1].FirstRowId, splitRow);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[1].LastRowId, rows.Size()-1);
    }

    Y_UNIT_TEST(SplitOutOfBoundsKeys) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0 });

        TRowTool tool(*lay);
        TRowsHeap rows(64 * 1024);
        for (ui64 seq = 0; seq < 10*4+3; ++seq) {
            rows.Put(*TSchemedCookRow(*lay).Col(1000 + seq, 42_u32));
        }

        auto partView = CreatePart(lay, rows, 4);

        TSlice slice;
        slice.FirstRowId = 3; // the first 4 rows are not included
        slice.FirstInclusive = false;
        slice.LastRowId = rows.Size() - 3; // the last 3 rows are not included
        slice.LastInclusive = false;
        slice.FirstKey = TSerializedCellVec(tool.KeyCells(rows[slice.FirstRowId]));
        slice.LastKey = TSerializedCellVec(tool.KeyCells(rows[slice.LastRowId]));

        TRowId splitRow = rows.Size() / 2;
        TVector<TSerializedCellVec> splitKeys;
        splitKeys.emplace_back(tool.KeyCells(*TSchemedCookRow(*lay).Col(500_u64, 42_u32)));
        splitKeys.emplace_back(tool.KeyCells(rows[splitRow]));
        splitKeys.emplace_back(tool.KeyCells(*TSchemedCookRow(*lay).Col(5000_u64, 42_u32)));

        TTableInfo table;
        table.RowScheme = lay.RowScheme();
        TIntrusiveListWithAutoDelete<TTableShard, TDelete> shards;
        shards.PushBack(new TTableShard);
        for (size_t k = 0; k < splitKeys.size(); ++k) {
            table.SplitKeys[k+1] = splitKeys[k];
            auto* left = shards.Back();
            shards.PushBack(new TTableShard);
            auto* right = shards.Back();
            left->RightKey = k + 1;
            right->LeftKey = k + 1;
        }

        TVector<TTableShard*> pshards;
        for (auto& shard : shards) {
            pshards.push_back(&shard);
        }

        TStrictEnv env;
        TSimpleConsumer consumer;
        TSliceSplitOp op(&consumer, &table, pshards, partView.Part, slice);

        bool ok1 = op.Execute(&env);
        UNIT_ASSERT_VALUES_EQUAL(ok1, false);

        env.Lock();
        bool ok2 = op.Execute(&env);
        UNIT_ASSERT_VALUES_EQUAL(ok2, true);

        auto& result = consumer.Result.value();
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices.size(), 2u);

        // The first row inclusion is currently a side effect
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[0].FirstRowId, 4u);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[0].FirstInclusive, true);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[0].LastRowId, splitRow - 1);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[0].LastInclusive, true);

        // The last row is not included just like in the original
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[1].FirstRowId, splitRow);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[1].FirstInclusive, true);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[1].LastRowId, rows.Size() - 3);
        UNIT_ASSERT_VALUES_EQUAL(result.NewSlices[1].LastInclusive, false);
    }

    Y_UNIT_TEST(SplitStatSimple) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0 });

        const ui64 pageCount = 16;
        const ui64 rowsPerPage = 4;

        TRowTool tool(*lay);
        TRowsHeap rows(64 * 1024);
        for (ui64 seq = 0; seq < rowsPerPage * pageCount; ++seq) {
            rows.Put(*TSchemedCookRow(*lay).Col(1000 + seq, 42_u32));
        }

        auto partView = CreatePart(lay, rows, rowsPerPage);
        UNIT_ASSERT_VALUES_EQUAL(partView.Slices->size(), 1u);

        const auto* part = partView.Part.Get();
        const auto slice = partView.Slices->front();

        // All pages are expected to have the same size
        auto pageSize = part->GetPageSize(part->Index->Begin()->GetPageId());

        for (int count = 1; count <= 3; ++count) {
            TSplitStatIterator it(*(*lay).Keys);

            // It shouldn't matter that we're adding the same slice
            // What matters is they all produce the exact same key
            for (int i = 0; i < count; ++i) {
                it.AddSlice(part, slice, pageSize * pageCount);
            }

            ui64 keyIndex = 0;
            ui64 leftSize = 0;
            ui64 leftRows = 0;
            ui64 rightSize = pageSize * pageCount * count;
            ui64 rightRows = rowsPerPage * pageCount * count;

            while (it.Next()) {
                // The first key is expected to be the first slice key
                auto key = it.CurrentKey();
                UNIT_ASSERT_VALUES_EQUAL(key.size(), 1u);
                auto expected = tool.KeyCells(rows[keyIndex]);
                UNIT_ASSERT_VALUES_EQUAL(expected.size(), 1u);

                UNIT_ASSERT_VALUES_EQUAL(key[0].AsValue<ui64>(), expected[0].AsValue<ui64>());
                UNIT_ASSERT_VALUES_EQUAL(it.LeftSize(), leftSize);
                UNIT_ASSERT_VALUES_EQUAL(it.RightSize(), rightSize);
                UNIT_ASSERT_VALUES_EQUAL(it.LeftRows(), leftRows);
                UNIT_ASSERT_VALUES_EQUAL(it.RightRows(), rightRows);

                keyIndex += rowsPerPage;
                leftSize += pageSize * count;
                leftRows += rowsPerPage * count;
                rightSize -= pageSize * count;
                rightRows -= rowsPerPage * count;
            }

            UNIT_ASSERT_VALUES_EQUAL(keyIndex, rowsPerPage * pageCount);
        }
    }

    Y_UNIT_TEST(SplitStatComplex) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0 });

        const ui64 pageCount = 16;

        TRowsHeap rows1(64 * 1024);
        for (ui64 seq = 0; seq < 4 * pageCount; ++seq) {
            rows1.Put(*TSchemedCookRow(*lay).Col(1000 + seq, 42_u32));
        }

        TRowsHeap rows2(64 * 1024);
        for (ui64 seq = 0; seq < 2 * pageCount; ++seq) {
            rows2.Put(*TSchemedCookRow(*lay).Col(1002 + seq * 2, 42_u32));
        }

        TRowsHeap rows3(64 * 1024);
        for (ui64 seq = 0; seq < 1 * pageCount; ++seq) {
            rows3.Put(*TSchemedCookRow(*lay).Col(1003 + seq * 4, 42_u32));
        }

        auto partView1 = CreatePart(lay, rows1, 4);
        auto partView2 = CreatePart(lay, rows2, 2);
        auto partView3 = CreatePart(lay, rows3, 1);
        for (const auto& partView : { partView1, partView2, partView3 }) {
            UNIT_ASSERT_VALUES_EQUAL(partView.Slices->size(), 1u);
        }

        auto pageSize1 = partView1.Part->GetPageSize(partView1.Part->Index->Begin()->GetPageId());
        auto pageSize2 = partView2.Part->GetPageSize(partView2.Part->Index->Begin()->GetPageId());
        auto pageSize3 = partView3.Part->GetPageSize(partView3.Part->Index->Begin()->GetPageId());

        // The expected keys are:
        // 1000, 1002, 1003, 1004, 1006, 1007, 1008, ...
        TSizeChanges sizeChanges;

        for (ui64 page = 0; page < pageCount; ++page) {
            // 1000, 1004, 1008, ...
            sizeChanges[1000 + page * 4]
                .Expect();

            // 1002, 1006, 1010, ...
            sizeChanges[1002 + page * 4]
                .AddLeft(pageSize1, 4);

            // 1003, 1007, 1011, ...
            sizeChanges[1003 + page * 4]
                .AddLeft(pageSize2, 2);

            if (page > 0) {
                // 1004, 1008, ...
                sizeChanges[1000 + page * 4]
                    .SubRight(pageSize1, 4)
                    .AddLeft(pageSize3, 1)
                    .SubRight(pageSize3, 1);

                // 1006, 1010, ...
                sizeChanges[1002 + page * 4]
                    .SubRight(pageSize2, 2);
            }
        }

        TSplitStatIterator it(*(*lay).Keys);
        it.AddSlice(partView1.Part.Get(), partView1.Slices->front(), pageSize1 * pageCount);
        it.AddSlice(partView2.Part.Get(), partView2.Slices->front(), pageSize2 * pageCount);
        it.AddSlice(partView3.Part.Get(), partView3.Slices->front(), pageSize3 * pageCount);
        VerifySizeChanges(it, sizeChanges);
    }

    Y_UNIT_TEST(SplitStatNextVersusStartStop) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0 });

        const ui64 pageCount = 4;

        TRowsHeap rows1(64 * 1024);
        for (ui64 seq = 0; seq < 4 * pageCount; ++seq) {
            rows1.Put(*TSchemedCookRow(*lay).Col(1000 + seq, 42_u32));
        }

        TRowsHeap rows2(64 * 1024);
        for (ui64 seq = 0; seq < 2 * pageCount; ++seq) {
            rows2.Put(*TSchemedCookRow(*lay).Col(1006 + seq * 2, 42_u32));
        }

        auto partView1 = CreatePart(lay, rows1, 4);
        auto partView2 = CreatePart(lay, rows2, 2);
        for (const auto& partView : { partView1, partView2 }) {
            UNIT_ASSERT_VALUES_EQUAL(partView.Slices->size(), 1u);
        }

        auto pageSize1 = partView1.Part->GetPageSize(partView1.Part->Index->Begin()->GetPageId());
        auto pageSize2 = partView2.Part->GetPageSize(partView2.Part->Index->Begin()->GetPageId());

        // Expected keys are 1000, 1004, 1006, 1008, 1010, 1012, ...
        TSizeChanges sizeChanges;

        for (ui64 page = 0; page < pageCount; ++page) {
            // 1000, 1004, 1008, ...
            sizeChanges[1000 + 4 * page].Expect();

            // 1006, 1010, 1014, ...
            sizeChanges[1006 + 4 * page].Expect();

            if (page > 0) {
                // 1004, 1008, ...
                sizeChanges[1000 + 4 * page].SubRight(pageSize1, 4);

                // 1010, 1014, ...
                sizeChanges[1006 + 4 * page].SubRight(pageSize2, 2);
            }

            if (page > 1) {
                // 1008, 1012, ...
                sizeChanges[1000 + 4 * page].AddLeft(pageSize2, 2);
            }

            if (page < pageCount - 1) {
                // 1006, 1010, ...
                sizeChanges[1006 + 4 * page].AddLeft(pageSize1, 4);
            }
        }

        sizeChanges[1004].AddLeft(pageSize1, 4);
        sizeChanges[1006 + (pageCount - 1) * 4].SubRight(pageSize1, 4);
        sizeChanges[1006 + (pageCount - 1) * 4].AddLeft(pageSize2, 2);

        TSplitStatIterator it(*(*lay).Keys);
        it.AddSlice(partView1.Part.Get(), partView1.Slices->front(), pageSize1 * pageCount);
        it.AddSlice(partView2.Part.Get(), partView2.Slices->front(), pageSize2 * pageCount);
        VerifySizeChanges(it, sizeChanges);
    }

    Y_UNIT_TEST(SplitStatPageInsidePage) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0 });

        const ui64 pageCount = 3;

        TRowsHeap rows1(64 * 1024);
        for (ui64 seq = 0; seq < 4 * pageCount; ++seq) {
            rows1.Put(*TSchemedCookRow(*lay).Col(1000 + seq, 42_u32));
        }

        TRowsHeap rows2(64 * 1024);
        rows2.Put(*TSchemedCookRow(*lay).Col(1005_u64, 42_u32));
        rows2.Put(*TSchemedCookRow(*lay).Col(1006_u64, 42_u32));

        auto partView1 = CreatePart(lay, rows1, 4);
        auto partView2 = CreatePart(lay, rows2, 4);
        for (const auto& partView : { partView1, partView2 }) {
            UNIT_ASSERT_VALUES_EQUAL(partView.Slices->size(), 1u);
        }

        auto pageSize1 = partView1.Part->GetPageSize(partView1.Part->Index->Begin()->GetPageId());
        auto pageSize2 = partView2.Part->GetPageSize(partView2.Part->Index->Begin()->GetPageId());

        // Expected keys are 1000, 1004, 1005, 1008
        TSizeChanges sizeChanges;

        sizeChanges[1000].Expect();
        sizeChanges[1004]
            .AddLeft(pageSize1, 4)
            .SubRight(pageSize1, 4);
        sizeChanges[1005]
            .AddLeft(pageSize1, 4);
        sizeChanges[1008]
            .AddLeft(pageSize2, 2)
            .SubRight(pageSize2, 2)
            .SubRight(pageSize1, 4);

        TSplitStatIterator it(*(*lay).Keys);
        it.AddSlice(partView1.Part.Get(), partView1.Slices->front(), pageSize1 * pageCount);
        it.AddSlice(partView2.Part.Get(), partView2.Slices->front(), pageSize2 * pageCount);
        VerifySizeChanges(it, sizeChanges);
    }

    Y_UNIT_TEST(PageReuseTest) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0 });

        const ui64 pageCount = 16;
        TRowTool tool(*lay);

        TRowsHeap rows1(64 * 1024);
        for (ui64 seq = 0; seq < 4 * pageCount; ++seq) {
            rows1.Put(*TSchemedCookRow(*lay).Col(1000 + seq, 42_u32));
        }

        TRowsHeap rows2(64 * 1024);
        for (ui64 seq = 0; seq < 5; ++seq) {
            rows2.Put(*TSchemedCookRow(*lay).Col(1009 + seq, 42_u32));
        }

        auto partView1 = CreatePart(lay, rows1, 4);
        auto partView2 = CreatePart(lay, rows2, 4);
        for (const auto& partView : { partView1, partView2 }) {
            UNIT_ASSERT_VALUES_EQUAL(partView.Slices->size(), 1u);
        }

        TPageReuseBuilder builder(*lay.RowScheme()->Keys);

        {
            TSlice slice = partView1.Slices->at(0);
            slice.FirstRowId += 2;
            slice.LastRowId -= 2;
            slice.FirstKey = TSerializedCellVec(tool.KeyCells(rows1[slice.FirstRowId]));
            slice.LastKey = TSerializedCellVec(tool.KeyCells(rows1[slice.LastRowId]));
            builder.AddSlice(partView1.Part.Get(), slice, true);
        }

        builder.AddSlice(partView2.Part.Get(), partView2.Slices->at(0), true);

        auto results = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(results.Reusable.size(), 2u);

        auto& reusable1 = results.Reusable.at(0);
        UNIT_ASSERT_VALUES_EQUAL(reusable1.Slice.FirstRowId, 4u);
        UNIT_ASSERT_VALUES_EQUAL(reusable1.Slice.FirstInclusive, true);
        UNIT_ASSERT_VALUES_EQUAL(reusable1.Slice.LastRowId, 8u);
        UNIT_ASSERT_VALUES_EQUAL(reusable1.Slice.LastInclusive, false);

        auto& reusable2 = results.Reusable.at(1);
        UNIT_ASSERT_VALUES_EQUAL(reusable2.Slice.FirstRowId, 16u);
        UNIT_ASSERT_VALUES_EQUAL(reusable2.Slice.FirstInclusive, true);
        UNIT_ASSERT_VALUES_EQUAL(reusable2.Slice.LastRowId, partView1->Index.GetEndRowId() - 4);
        UNIT_ASSERT_VALUES_EQUAL(reusable2.Slice.LastInclusive, false);

        UNIT_ASSERT_VALUES_EQUAL(results.ExpectedSlices, 5u);
    }

    Y_UNIT_TEST(CompactWithUnderlayMask) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0 });

        const ui64 rowsCount = 20;

        TRowTool tool(*lay);
        TRowsHeap rows(64 * 1024);
        TPartCook cook(lay, { false, 4096 });
        for (ui64 seq = 0; seq < rowsCount; ++seq) {
            rows.Put(*TSchemedCookRow(*lay).Col(1000 + seq, 42_u32));
            cook.AddOpN(ERowOp::Erase, 1000 + seq);
        }

        auto source = cook.Finish();

        {
            auto partView = source.ToPartView();
            UNIT_ASSERT_VALUES_EQUAL(partView.Slices->size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(partView.Slices->at(0).Rows(), rowsCount);
        }

        TVector<TBounds> underlayMaskValues;

        {
            auto& bounds = underlayMaskValues.emplace_back();
            bounds.FirstKey = TSerializedCellVec(tool.KeyCells(rows[3]));
            bounds.LastKey = TSerializedCellVec(tool.KeyCells(rows[7]));
            bounds.FirstInclusive = false;
            bounds.LastInclusive = true;
        }

        {
            auto& bounds = underlayMaskValues.emplace_back();
            bounds.FirstKey = TSerializedCellVec(tool.KeyCells(rows[11]));
            bounds.LastKey = TSerializedCellVec(tool.KeyCells(rows[14]));
            bounds.FirstInclusive = true;
            bounds.LastInclusive = false;
        }

        TVector<const TBounds*> underlayMaskPointers;
        underlayMaskPointers.emplace_back(&underlayMaskValues.at(0));
        underlayMaskPointers.emplace_back(&underlayMaskValues.at(1));
        auto underlayMask = TUnderlayMask::Build(lay.RowScheme(), underlayMaskPointers);

        auto born = TCompaction(nullptr, CreateConf(16, underlayMask.Get()))
            .Do(lay.RowScheme(), { &source });

        // Check only erase markers under the mask are kept intact
        TCheckIt(born, { nullptr, 0 }, nullptr, true /* expand defaults */)
            .To(10).Seek({ }, ESeek::Lower).Is(EReady::Data)
            .To(21).IsOpN(ERowOp::Erase, 1004_u64, ECellOp::Empty).Next()
            .To(22).IsOpN(ERowOp::Erase, 1005_u64, ECellOp::Empty).Next()
            .To(23).IsOpN(ERowOp::Erase, 1006_u64, ECellOp::Empty).Next()
            .To(24).IsOpN(ERowOp::Erase, 1007_u64, ECellOp::Empty).Next()
            .To(25).IsOpN(ERowOp::Erase, 1011_u64, ECellOp::Empty).Next()
            .To(26).IsOpN(ERowOp::Erase, 1012_u64, ECellOp::Empty).Next()
            .To(27).IsOpN(ERowOp::Erase, 1013_u64, ECellOp::Empty).Next()
            .To(30).Is(EReady::Gone);
    }

    Y_UNIT_TEST(CompactWithSplitKeys) {
        TLayoutCook lay;
        lay
                .Col(0, 0,  NScheme::NTypeIds::Uint64)
                .Col(0, 8,  NScheme::NTypeIds::Uint32)
                .Key({ 0 });

        const ui64 pageCount = 5;

        TRowTool tool(*lay);
        TRowsHeap rows(64 * 1024);
        for (ui64 seq = 0; seq < 4 * pageCount; ++seq) {
            rows.Put(*TSchemedCookRow(*lay).Col(1000 + seq, 42_u32));
        }

        auto source = TPartCook(lay, CreateConf(4))
            .Add(rows.begin(), rows.end())
            .Finish();

        {
            auto partView = source.ToPartView();
            UNIT_ASSERT_VALUES_EQUAL(partView.Slices->size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(partView->Index->Records, pageCount);
        }

        TVector<TSerializedCellVec> splitKeyValues;
        splitKeyValues.emplace_back(tool.KeyCells(rows[6]));
        splitKeyValues.emplace_back(tool.KeyCells(rows[14]));
        TSplitKeys splitKeys(lay.RowScheme(), std::move(splitKeyValues));

        auto born = TCompaction(nullptr, CreateConf(4, &splitKeys))
            .Do(lay.RowScheme(), { &source });

        {
            auto partView = born.ToPartView();
            UNIT_ASSERT_VALUES_EQUAL(partView.Slices->size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(partView->Index->Records, 6u);

            UNIT_ASSERT_VALUES_EQUAL(partView->Index->Record(0)->GetRowId(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(partView->Index->Record(1)->GetRowId(), 4u);
            UNIT_ASSERT_VALUES_EQUAL(partView->Index->Record(2)->GetRowId(), 6u);
            UNIT_ASSERT_VALUES_EQUAL(partView->Index->Record(3)->GetRowId(), 10u);
            UNIT_ASSERT_VALUES_EQUAL(partView->Index->Record(4)->GetRowId(), 14u);
            UNIT_ASSERT_VALUES_EQUAL(partView->Index->Record(5)->GetRowId(), 18u);

            auto key0 = TSerializedCellVec::Serialize(tool.KeyCells(rows[0]));
            auto key5 = TSerializedCellVec::Serialize(tool.KeyCells(rows[5]));
            auto& slice0 = partView.Slices->at(0);
            UNIT_ASSERT_VALUES_EQUAL(slice0.FirstRowId, 0u);
            UNIT_ASSERT_VALUES_EQUAL(slice0.LastRowId, 5u);
            UNIT_ASSERT_VALUES_EQUAL(slice0.FirstKey.GetBuffer(), key0);
            UNIT_ASSERT_VALUES_EQUAL(slice0.LastKey.GetBuffer(), key5);

            auto key6 = TSerializedCellVec::Serialize(tool.KeyCells(rows[6]));
            auto key13 = TSerializedCellVec::Serialize(tool.KeyCells(rows[13]));
            auto& slice1 = partView.Slices->at(1);
            UNIT_ASSERT_VALUES_EQUAL(slice1.FirstRowId, 6u);
            UNIT_ASSERT_VALUES_EQUAL(slice1.LastRowId, 13u);
            UNIT_ASSERT_VALUES_EQUAL(slice1.FirstKey.GetBuffer(), key6);
            UNIT_ASSERT_VALUES_EQUAL(slice1.LastKey.GetBuffer(), key13);

            auto key14 = TSerializedCellVec::Serialize(tool.KeyCells(rows[14]));
            auto key19 = TSerializedCellVec::Serialize(tool.KeyCells(rows[19]));
            auto& slice2 = partView.Slices->at(2);
            UNIT_ASSERT_VALUES_EQUAL(slice2.FirstRowId, 14u);
            UNIT_ASSERT_VALUES_EQUAL(slice2.LastRowId, 19u);
            UNIT_ASSERT_VALUES_EQUAL(slice2.FirstKey.GetBuffer(), key14);
            UNIT_ASSERT_VALUES_EQUAL(slice2.LastKey.GetBuffer(), key19);
        }
    }

}

Y_UNIT_TEST_SUITE(TShardedCompactionScenarios) {

    struct Schema : NIceDb::Schema {
        struct Data : Table<1> {
            struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
            struct Value : Column<2, NScheme::NTypeIds::Uint32> { };

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<Key, Value>;
        };

        using TTables = SchemaTables<Data>;
    };

    Y_UNIT_TEST(SimpleNoResharding) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            // special policy with disabled nursery
            TCompactionPolicy policy;
            policy.ShardPolicy.SetMinSliceSize(0);
            backend.DB.Alter().SetCompactionPolicy(1, policy);

            backend.Commit();
        }

        // Insert some initial rows and compact them outside of strategy
        {
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < 64; ++seq) {
                db.Table<Schema::Data>().Key(1000 + seq).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(1);

            UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(1).size(), 1u);
        }

        TShardedCompactionStrategy strategy(1, &backend, &broker, &logger, &time, "suffix");

        {
            TCompactionState initialState;
            strategy.Start(initialState);
        }

        // Don't expect any tasks or change requests
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(!backend.PendingReads);
        UNIT_ASSERT(!backend.StartedCompactions);
        UNIT_ASSERT(!backend.CheckChangesFlag());

        // Erase and insert some more rows
        {
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < 256; ++seq) {
                if (seq < 128) {
                    db.Table<Schema::Data>().Key(1000 - 64 + seq).Delete();
                } else {
                    db.Table<Schema::Data>().Key(1000 - 64 + seq).Update<Schema::Data::Value>(43);
                }
            }
            backend.Commit();
        }

        // Start a memtable compaction using this strategy
        {
            auto memCompactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, 0);
            UNIT_ASSERT(memCompactionId != 0);
            auto outcome = backend.RunCompaction(memCompactionId);

            UNIT_ASSERT(outcome.Params);
            UNIT_ASSERT(!outcome.Params->Parts);
            UNIT_ASSERT(!outcome.Params->IsFinal);

            // We expect that only 64 out of 128 drops survive
            UNIT_ASSERT(outcome.Result);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Result->Parts.size(), 1u);
            UNIT_ASSERT(outcome.Result->Parts[0]);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Result->Parts[0]->Stat.Drops, 64u);

            auto changes = strategy.CompactionFinished(
                    memCompactionId, std::move(outcome.Params), std::move(outcome.Result));

            // Don't expect any slice changes at this time
            UNIT_ASSERT(changes.SliceChanges.empty());
        }

        // There should be a compaction task pending right now
        UNIT_ASSERT(broker.RunPending());
        UNIT_ASSERT(!broker.HasPending());

        // There should be compaction started right now
        UNIT_ASSERT_VALUES_EQUAL(backend.StartedCompactions.size(), 1u);

        // Perform this compaction
        {
            auto outcome = backend.RunCompaction();
            UNIT_ASSERT(outcome.Params->Parts);
            UNIT_ASSERT(outcome.Params->IsFinal);

            // We expect that none of the original drops survive
            UNIT_ASSERT_VALUES_EQUAL(outcome.Result->Parts.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Result->Parts[0]->Stat.Drops, 0u);

            auto changes = strategy.CompactionFinished(
                    outcome.CompactionId, std::move(outcome.Params), std::move(outcome.Result));

            // Don't expect any slice changes at this time
            UNIT_ASSERT(changes.SliceChanges.empty());
        }

        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(!backend.StartedCompactions);
    }

    Y_UNIT_TEST(NurserySequential) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        const ui64 triggerSize = 16 * 1024;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            // special policy with a small nursery
            TCompactionPolicy policy;
            policy.ShardPolicy.SetMinSliceSize(triggerSize);
            backend.DB.Alter().SetCompactionPolicy(1, policy);

            backend.Commit();
        }

        TShardedCompactionStrategy strategy(1, &backend, &broker, &logger, &time, "suffix");
        strategy.Start({ });

        size_t memCompactions = 0;
        for (ui64 base = 0; base < 16 * 1024; base += 128) {
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < 128; ++seq) {
                db.Table<Schema::Data>().Key(base + seq).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(&strategy);
            ++memCompactions;

            UNIT_ASSERT_C(!broker.HasPending(),
                "Strategy shouldn't request tasks in fully sequential case");
        }

        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(!broker.HasRunning());
        UNIT_ASSERT(!backend.StartedCompactions);

        size_t countAll = 0;
        size_t countBig = 0;
        size_t countSmall = 0;
        for (auto& part : backend.TableParts(1)) {
            ++countAll;
            if (part->BackingSize() >= triggerSize) {
                ++countBig;
            } else {
                ++countSmall;
            }
        }

        UNIT_ASSERT_C(countAll < memCompactions && countBig > countSmall,
            "Produced " << countAll << " parts after " << memCompactions << " compactions ("
                << countBig << " big and " << countSmall << " small)");
    }

    Y_UNIT_TEST(SequentialOverlap) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            // special policy without nursery and extremely reusable slices
            TCompactionPolicy policy;
            policy.ShardPolicy.SetMinSliceSize(0);
            policy.ShardPolicy.SetMinSliceSizeToReuse(1);
            policy.ShardPolicy.SetNewDataPercentToCompact(50);
            backend.DB.Alter().SetCompactionPolicy(1, policy);

            backend.Commit();
        }

        TShardedCompactionStrategy strategy(1, &backend, &broker, &logger, &time, "suffix");
        strategy.Start({ });

        const ui64 rowsPerTx = 16 * 1024;
        for (ui64 attempt = 0; attempt < 2; ++attempt) {
            const ui64 base = (rowsPerTx - 1) * attempt;
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < rowsPerTx; ++seq) {
                db.Table<Schema::Data>().Key(base + seq).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(&strategy);

            // Run all pending tasks and compactions
            while (broker.RunPending()) {
                while (backend.SimpleTableCompaction(1, &broker, &strategy)) {
                    // nothing
                }
            }
        }

        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(!broker.HasRunning());
        UNIT_ASSERT(!backend.StartedCompactions);

        // We expect overlap to be compacted with the rest reused
        UNIT_ASSERT_VALUES_EQUAL(
            backend.DumpKeyRanges(1, true),
            "[{0}, {16168})@1/4 [{16168}, {16758}]@2/8 [{16759}, {32766}]@2/7");
    }

    Y_UNIT_TEST(SequentialSplit) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            // special policy without nursery and small shard size
            TCompactionPolicy policy;
            policy.ShardPolicy.SetMinSliceSize(0);
            policy.ShardPolicy.SetMaxShardSize(512 * 1024);
            backend.DB.Alter().SetCompactionPolicy(1, policy);

            backend.Commit();
        }

        TShardedCompactionStrategy strategy(1, &backend, &broker, &logger, &time, "suffix");
        strategy.Start({ });

        const ui64 rowsPerTx = 16 * 1024;
        for (ui64 index = 0; index < 2; ++index) {
            const ui64 base = rowsPerTx * index;
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < rowsPerTx; ++seq) {
                db.Table<Schema::Data>().Key(base + seq).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(&strategy);

            UNIT_ASSERT_C(!broker.HasPending(), "Pending task at index " << index);
            UNIT_ASSERT_C(!backend.StartedCompactions, "Started compaction at index " << index);

            UNIT_ASSERT_C(!backend.PendingReads, "Pending read at index " << index);
        }

        auto& state = backend.TableState[1];

        NProto::TShardedStrategyStateInfo header;
        UNIT_ASSERT(state.contains(0));
        UNIT_ASSERT(ParseFromStringNoSizeLimit(header, state[0]));

        // We expect two shards
        UNIT_ASSERT_VALUES_EQUAL(header.GetShards().size(), 2u);

        // We expect a single split key
        UNIT_ASSERT_VALUES_EQUAL(header.GetSplitKeys().size(), 1u);

        // We expect split key to be the start of the second part
        TSerializedCellVec splitKey(state.at(header.GetSplitKeys(0)));
        UNIT_ASSERT(splitKey.GetCells());
        UNIT_ASSERT_VALUES_EQUAL(splitKey.GetCells().at(0).AsValue<ui64>(), rowsPerTx);
    }

    Y_UNIT_TEST(NormalSplit) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            // special policy without nursery, small shard size and large compaction trigger
            TCompactionPolicy policy;
            policy.ShardPolicy.SetMinSliceSize(0);
            policy.ShardPolicy.SetMaxShardSize(512 * 1024);
            policy.ShardPolicy.SetNewDataPercentToCompact(300);
            backend.DB.Alter().SetCompactionPolicy(1, policy);

            backend.Commit();
        }

        TShardedCompactionStrategy strategy(1, &backend, &broker, &logger, &time, "suffix");
        strategy.Start({ });

        const ui64 rowsPerTx = 16 * 1024;
        for (ui64 index = 0; index < 2; ++index) {
            const ui64 base = index;
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < rowsPerTx; ++seq) {
                db.Table<Schema::Data>().Key(base + seq * 3).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(&strategy);

            UNIT_ASSERT_C(!broker.HasPending(), "Pending task at index " << index);
            UNIT_ASSERT_C(!backend.StartedCompactions, "Started compaction at index " << index);

            if (index == 0) {
                UNIT_ASSERT_C(!backend.PendingReads, "Pending read at index " << index);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(backend.PendingReads.size(), 2u);
        while (backend.PendingReads) {
            TStrictEnv env;
            auto first = backend.RunRead(&env);
            UNIT_ASSERT(!first.Completed);
            env.Lock();
            auto second = backend.RunRead(first.ReadId, &env);
            UNIT_ASSERT(second.Completed);
        }

        UNIT_ASSERT(backend.CheckChangesFlag());
        auto changes = strategy.ApplyChanges();
        UNIT_ASSERT(changes.SliceChanges);
        backend.ApplyChanges(1, std::move(changes));

        auto& state = backend.TableState[1];

        NProto::TShardedStrategyStateInfo header;
        UNIT_ASSERT(state.contains(0));
        UNIT_ASSERT(ParseFromStringNoSizeLimit(header, state[0]));

        // We expect two shards
        UNIT_ASSERT_VALUES_EQUAL(header.GetShards().size(), 2u);

        // We expect a single split key
        UNIT_ASSERT_VALUES_EQUAL(header.GetSplitKeys().size(), 1u);

        // Stop current strategy
        strategy.Stop();

        // Start a new strategy instance from the same initial state
        TShardedCompactionStrategy reloaded(1, &backend, &broker, &logger, &time, "suffix");
        {
            TCompactionState initialState;
            initialState.StateSnapshot = backend.TableState[1];
            reloaded.Start(std::move(initialState));
        }

        // Strategy must accept current slices without unexpected actions
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(!backend.PendingReads);
        UNIT_ASSERT(!backend.StartedCompactions);

        for (ui64 index = 2; index < 3; ++index) {
            const ui64 base = index;
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < rowsPerTx; ++seq) {
                db.Table<Schema::Data>().Key(base + seq * 3).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(&reloaded);

            UNIT_ASSERT_C(!broker.HasPending(), "Pending task at index " << index);
            UNIT_ASSERT_C(!backend.StartedCompactions, "Started compaction at index " << index);

            // The part should be generated without additional reads
            UNIT_ASSERT(!backend.PendingReads);
        }

        // There should be 3 parts by now
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(1).size(), 3u);

        for (auto& partView : backend.TableParts(1)) {
            // All parts are expected to have 2 slices
            UNIT_ASSERT_VALUES_EQUAL(partView.Slices->size(), 2u);
        }
    }

    Y_UNIT_TEST(CompactionHoles) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            // special policy without nursery and 99% compaction trigger
            TCompactionPolicy policy;
            policy.ShardPolicy.SetMinSliceSize(0);
            policy.ShardPolicy.SetNewDataPercentToCompact(99);
            policy.ShardPolicy.SetNewRowsPercentToCompact(99);
            backend.DB.Alter().SetCompactionPolicy(1, policy);

            backend.Commit();
        }

        auto strategy = MakeHolder<TShardedCompactionStrategy>(1, &backend, &broker, &logger, &time, "suffix");
        strategy->Start({ });

        // Arrange parts (epochs) like this in key space:
        // . 5 . 6 .
        // . 3 . 4 .
        // 7 1 8 2 9
        // We ignore compaction requests so it's not messed up until we're done.
        const ui64 rowsPerTx = 16 * 1024;
        for (ui64 level = 0; level < 3; ++level) {
            for (ui64 index = 0; index < 2; ++index) {
                const ui64 base = rowsPerTx + index * 2 * rowsPerTx;
                auto db = backend.Begin();
                for (ui64 seq = 0; seq < rowsPerTx; ++seq) {
                    db.Table<Schema::Data>().Key(base + seq).Update<Schema::Data::Value>(42);
                }
                backend.Commit();
                backend.SimpleMemCompaction(strategy.Get());
            }
        }
        for (ui64 index = 0; index < 3; ++index) {
            const ui64 base = index * 2 * rowsPerTx;
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < rowsPerTx; ++seq) {
                db.Table<Schema::Data>().Key(base + seq).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(strategy.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(backend.DumpKeyRanges(1),
            "[{0}, {16383}]@7 [{16384}, {32767}]@1 [{16384}, {32767}]@3 [{16384}, {32767}]@5 [{32768}, {49151}]@8 [{49152}, {65535}]@2 [{49152}, {65535}]@4 [{49152}, {65535}]@6 [{65536}, {81919}]@9");

        auto savedState = strategy->SnapshotState();
        strategy->Stop();
        strategy.Reset();

        // Strategy must discard all its current tasks when stopping
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(!backend.PendingReads);
        UNIT_ASSERT(!backend.StartedCompactions);

        strategy = MakeHolder<TShardedCompactionStrategy>(1, &backend, &broker, &logger, &time, "suffix");
        strategy->Start(std::move(savedState));

        // We expect a single compaction of two upper levels
        UNIT_ASSERT(broker.RunPending());
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT_VALUES_EQUAL(backend.StartedCompactions.size(), 1u);
        UNIT_ASSERT(backend.SimpleTableCompaction(1, &broker, strategy.Get()));

        // We expect the following at the end of compaction:
        // . 6 . 6 .
        // 7 1 8 2 9
        UNIT_ASSERT_VALUES_EQUAL(backend.DumpKeyRanges(1),
            "[{0}, {16383}]@7 [{16384}, {32767}]@1 [{16384}, {32767}]@6 [{32768}, {49151}]@8 [{49152}, {65535}]@2 [{49152}, {65535}]@6 [{65536}, {81919}]@9");
    }

    Y_UNIT_TEST(CompactionGarbage) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            // special policy without nursery and 25% compaction triggers
            TCompactionPolicy policy;
            policy.ShardPolicy.SetMinSliceSize(0);
            policy.ShardPolicy.SetMinShardSize(0);
            policy.ShardPolicy.SetNewDataPercentToCompact(25);
            policy.ShardPolicy.SetNewRowsPercentToCompact(25);
            policy.ShardPolicy.SetMaxGarbagePercentToReuse(25);
            backend.DB.Alter().SetCompactionPolicy(1, policy);

            backend.Commit();
        }

        const ui64 rowsPerShard = 16 * 1024;

        // Create initial state with 2 shards
        TCompactionState initialState;
        {
            const ui64 splitKey = rowsPerShard;
            initialState.StateSnapshot[1] = TSerializedCellVec::Serialize({
                TCell::Make(splitKey)
            });
            NProto::TShardedStrategyStateInfo header;
            header.SetLastSplitKey(1);
            header.AddSplitKeys(1);
            header.SetLastShardId(2);
            header.AddShards(1);
            header.AddShards(2);
            Y_PROTOBUF_SUPPRESS_NODISCARD header.SerializeToString(&initialState.StateSnapshot[0]);
        }

        TShardedCompactionStrategy strategy(1, &backend, &broker, &logger, &time, "suffix");
        strategy.Start(std::move(initialState));

        // Create a large slice over both shards
        {
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < rowsPerShard * 2; ++seq) {
                db.Table<Schema::Data>().Key(seq).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(&strategy);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            backend.DumpKeyRanges(1),
            "[{0}, {16383}]@1 [{16384}, {32767}]@1");

        // Create a large slice in the first shard, to trigger its compaction
        {
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < rowsPerShard; ++seq) {
                db.Table<Schema::Data>().Key(seq).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(&strategy);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            backend.DumpKeyRanges(1, true),
            "[{0}, {16383}]@1/4 [{0}, {16383}]@2/7 [{16384}, {32767}]@1/4");

        // New slice should have triggered compaction in the left shard
        UNIT_ASSERT(broker.RunPending());
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(backend.SimpleTableCompaction(1, &broker, &strategy));

        UNIT_ASSERT_VALUES_EQUAL(
            backend.DumpKeyRanges(1, true),
            "[{0}, {16383}]@2/8 [{16384}, {32767}]@1/4");

        // Now we should have triggered garbage compaction in the right shard
        UNIT_ASSERT(broker.RunPending());
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(backend.SimpleTableCompaction(1, &broker, &strategy));

        UNIT_ASSERT_VALUES_EQUAL(
            backend.DumpKeyRanges(1, true),
            "[{0}, {16383}]@2/8 [{16384}, {32767}]@1/9");

        UNIT_ASSERT(!broker.HasPending());
    }

    Y_UNIT_TEST(CompactionGarbageNoReuse) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            // special policy without nursery, 25% compaction triggers
            TCompactionPolicy policy;
            policy.ShardPolicy.SetMinSliceSize(0);
            policy.ShardPolicy.SetMinShardSize(0);
            policy.ShardPolicy.SetMinSliceSizeToReuse(1);
            policy.ShardPolicy.SetNewDataPercentToCompact(25);
            policy.ShardPolicy.SetNewRowsPercentToCompact(25);
            policy.ShardPolicy.SetMaxGarbagePercentToReuse(25);
            backend.DB.Alter().SetCompactionPolicy(1, policy);

            backend.Commit();
        }

        TShardedCompactionStrategy strategy(1, &backend, &broker, &logger, &time, "suffix");
        strategy.Start({ });

        const ui64 rowsPerSlice = 16 * 1024;

        // Create two slices that largely overlap
        for (ui64 index = 0; index < 2; ++index) {
            const ui64 base = index * rowsPerSlice / 2;
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < rowsPerSlice; ++seq) {
                db.Table<Schema::Data>().Key(base + seq).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(&strategy);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            backend.DumpKeyRanges(1, true),
            "[{0}, {16383}]@1/4 [{8192}, {24575}]@2/7");

        // Run the triggered compaction
        UNIT_ASSERT(broker.RunPending());
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(backend.SimpleTableCompaction(1, &broker, &strategy));

        // We expect the middle to not be reused, as it would produce too much garbage
        UNIT_ASSERT_VALUES_EQUAL(
            backend.DumpKeyRanges(1, true),
            "[{0}, {24575}]@2/8");
    }

}

}
}
}
