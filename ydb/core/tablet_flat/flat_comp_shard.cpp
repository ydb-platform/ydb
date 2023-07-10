#include "flat_comp_shard.h"
#include "flat_part_charge.h"
#include "flat_part_iter_multi.h"
#include "flat_stat_part.h"
#include "util_fmt_cell.h"

#include <ydb/core/tablet_flat/protos/flat_table_shard.pb.h>
#include <ydb/core/util/pb.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/generic/cast.h>

#include <optional>

namespace NKikimr {
namespace NTable {
namespace NCompShard {

    using NKikimr::NTable::NProto::TShardedStrategyStateInfo;

    namespace {

        static constexpr ui32 PRIORITY_UPDATE_FACTOR = 20;

        using NFmt::TPrintableTypedCells;

        class TPrintableShardRange {
        public:
            TPrintableShardRange(const TTableShard& shard, const TTableInfo& table)
                : Shard(shard)
                , Table(table)
            { }

            friend IOutputStream& operator<<(IOutputStream& out, const TPrintableShardRange& v) {
                if (v.Shard.LeftKey == 0 && v.Shard.RightKey == 0) {
                    out << "all";
                    return out;
                }
                out << '[';
                if (v.Shard.LeftKey != 0) {
                    out << TPrintableTypedCells(v.Table.SplitKeys.at(v.Shard.LeftKey).GetCells(), v.Table.RowScheme->Keys->BasicTypes());
                } else {
                    out << "-inf";
                }
                out << ", ";
                if (v.Shard.RightKey != 0) {
                    out << TPrintableTypedCells(v.Table.SplitKeys.at(v.Shard.RightKey).GetCells(), v.Table.RowScheme->Keys->BasicTypes());
                } else {
                    out << "+inf";
                }
                out << ')';
                return out;
            }

        private:
            const TTableShard& Shard;
            const TTableInfo& Table;
        };

        TPartView MakePartView(TIntrusiveConstPtr<TPart> part, TIntrusiveConstPtr<TSlices> slices) noexcept {
            TPartView partView{ std::move(part), nullptr, std::move(slices) };
            partView.Screen = partView.Slices->ToScreen(); // TODO: remove screen from TPartView
            return partView;
        }

    }

    bool TSplitStatIterator::TCmpHeapByFirstKey::operator()(const TItemState* b, const TItemState* a) const noexcept {
        TCellsRef left = a->Slice.FirstKey.GetCells();
        TCellsRef right = b->Slice.FirstKey.GetCells();
        if (int cmp = ComparePartKeys(left, right, KeyCellDefaults)) {
            return cmp < 0;
        }
        return a->Slice.FirstInclusive && !b->Slice.FirstInclusive;
    }

    bool TSplitStatIterator::TCmpHeapByNextKey::operator()(const TItemState* b, const TItemState* a) const noexcept {
        TCellsRef left = a->NextKey;
        TCellsRef right = b->NextKey;
        return ComparePartKeys(left, right, KeyCellDefaults) < 0;
    }

    bool TSplitStatIterator::TCmpHeapByLastKey::operator()(const TItemState* b, const TItemState* a) const noexcept {
        TCellsRef left = a->Slice.LastKey.GetCells();
        TCellsRef right = b->Slice.LastKey.GetCells();
        if (int cmp = ComparePartKeys(left, right, KeyCellDefaults)) {
            return cmp < 0;
        }
        return !a->Slice.LastInclusive && b->Slice.LastInclusive;
    }

    bool TSplitStatIterator::HasStarted(const TItemState* item) const noexcept {
        TCellsRef left = item->Slice.FirstKey.GetCells();
        TCellsRef right = Key;
        return ComparePartKeys(left, right, KeyCellDefaults) < 0;
    }

    bool TSplitStatIterator::HasStopped(const TItemState* item) const noexcept {
        TCellsRef left = item->Slice.LastKey.GetCells();
        TCellsRef right = Key;
        if (int cmp = ComparePartKeys(left, right, KeyCellDefaults)) {
            return cmp < 0;
        }
        return !item->Slice.LastInclusive;
    }

    void TSplitStatIterator::TItemState::InitNextKey() noexcept {
        auto& columns = Part->Scheme->Groups[0].ColsKeyIdx;
        NextKey.resize(columns.size());
        for (size_t pos = 0; pos < columns.size(); ++pos) {
            NextKey[pos] = NextPage->Cell(columns[pos]);
        }
    }

    void TSplitStatIterator::TItemState::InitPageSize() noexcept {
        TRowId endRowId = NextPage ? NextPage->GetRowId() : Slice.EndRowId();
        LastPageSize = Helper.CalcSize(LastRowId, endRowId);
        LastPageRows = endRowId - LastRowId;
        LastRowId = endRowId;
    }

    void TSplitStatIterator::AddSlice(const TPart* part, const TSlice& slice, ui64 size) noexcept {
        auto* item = &Items.emplace_back(part, slice);
        InitQueue.push(item);
        RightSize_ += size;
        RightRows_ += slice.Rows();
    }

    bool TSplitStatIterator::Next() noexcept {
        if (!InitQueue && !NextQueue) {
            return false;
        }

        // Select the next split key
        TCellsRef selected;
        if (InitQueue && NextQueue) {
            TCellsRef left = InitQueue.top()->Slice.FirstKey.GetCells();
            TCellsRef right = NextQueue.top()->NextKey;
            if (ComparePartKeys(left, right, KeyCellDefaults) <= 0) {
                selected = left;
            } else {
                selected = right;
            }
        } else if (InitQueue) {
            selected = InitQueue.top()->Slice.FirstKey.GetCells();
        } else {
            selected = NextQueue.top()->NextKey;
        }

        Key.reserve(selected.size());
        Key.assign(selected.begin(), selected.end());
        TCellsRef splitKey = Key;

        // Take all matching items from init queue and initialize them
        while (InitQueue && ComparePartKeys(InitQueue.top()->Slice.FirstKey.GetCells(), splitKey, KeyCellDefaults) <= 0) {
            auto* item = InitQueue.top();
            InitQueue.pop();
            StartQueue.push(item);

            // Find the first page that is after the first slice row
            item->NextPage = item->Part->Index.LookupRow(item->Slice.BeginRowId());
            if (++item->NextPage && item->NextPage->GetRowId() < item->Slice.EndRowId()) {
                // That first page has a matching row, use it for key enumeration
                item->InitNextKey();
                NextQueue.push(item);
            }
        }

        // Take all slices from start queue that have crossed the boundary
        while (StartQueue && HasStarted(StartQueue.top())) {
            auto* item = StartQueue.top();
            StartQueue.pop();
            ActivationQueue.push_back(item);
        }

        // Everything in activation queue has crossed the boundary and now belongs on the left side
        for (auto* item : ActivationQueue) {
            item->InitPageSize();
            LeftSize_ += item->LastPageSize;
            LeftRows_ += item->LastPageRows;

            // Check if the last page is only partially on the left side
            if (item->LastPageRows <= 1) {
                item->IsPartial = false;
            } else if (item->NextPage) {
                item->IsPartial = ComparePartKeys(splitKey, item->NextKey, KeyCellDefaults) < 0;
            } else if (HasStopped(item)) {
                item->IsPartial = false;
            } else {
                item->IsPartial = true;
                StopQueue.push(item);
            }

            // The last page may not belong on the right side any more
            if (!item->IsPartial) {
                RightSize_ -= item->LastPageSize;
                RightRows_ -= item->LastPageRows;
            }
        }
        ActivationQueue.clear();

        // Take all slices that have next key matching the new boundary
        while (NextQueue && ComparePartKeys(NextQueue.top()->NextKey, splitKey, KeyCellDefaults) <= 0) {
            auto* item = NextQueue.top();
            NextQueue.pop();

            // The last page no longer belongs on the right side
            if (item->IsPartial) {
                RightSize_ -= item->LastPageSize;
                RightRows_ -= item->LastPageRows;
                item->IsPartial = false;
            }

            // Try moving to the next page
            if (++item->NextPage && item->NextPage->GetRowId() < item->Slice.EndRowId()) {
                item->InitNextKey();
                NextQueue.push(item);
            }

            // If there are any rows left the new page will activate on the next key
            if (item->LastRowId < item->Slice.EndRowId()) {
                ActivationQueue.push_back(item);
            }
        }

        // Take all slices that have their last row beyond the new boundary
        while (StopQueue && HasStopped(StopQueue.top())) {
            auto* item = StopQueue.top();
            StopQueue.pop();

            // Everything up to the end no longer belongs on the right side
            if (item->IsPartial) {
                RightSize_ -= item->LastPageSize;
                RightRows_ -= item->LastPageRows;
                item->IsPartial = false;
            }
        }

        return true;
    }

    bool TPageReuseBuilder::TItemState::InitNextKey() noexcept {
        if (NextPage && NextPage->GetRowId() < Slice.EndRowId()) {
            auto& columns = Part->Scheme->Groups[0].ColsKeyIdx;
            NextKey.resize(columns.size());
            for (size_t pos = 0; pos < columns.size(); ++pos) {
                NextKey[pos] = NextPage->Cell(columns[pos]);
            }
            First.Key = NextKey;
            First.RowId = NextPage->GetRowId();
            First.Inclusive = true;
            return true;
        } else {
            return false;
        }
    }

    TRowId TPageReuseBuilder::TItemState::GetNextRowId() const noexcept {
        return NextPage ? NextPage->GetRowId() : Part->Index.GetEndRowId();
    }

    void TPageReuseBuilder::AddSlice(const TPart* part, const TSlice& slice, bool reusable) noexcept {
        Items.emplace_back(part, slice, reusable);
    }

    TPageReuseBuilder::TResults TPageReuseBuilder::Build() noexcept {
        TResults results;
        TItemState* lastItem = nullptr;

        // Produces a min-heap by first key
        auto heapByFirstKeyMin = [this](const TItemState* b, const TItemState* a) noexcept -> bool {
            TCellsRef left = a->First.Key;
            TCellsRef right = b->First.Key;
            if (int cmp = ComparePartKeys(left, right, KeyCellDefaults)) {
                return cmp < 0;
            }
            return a->First.Inclusive && !b->First.Inclusive;
        };

        auto noIntersection = [this](const TRightBoundary& a, const TLeftBoundary& b) noexcept -> bool {
            TCellsRef left = a.Key;
            TCellsRef right = b.Key;
            if (int cmp = ComparePartKeys(left, right, KeyCellDefaults)) {
                return cmp < 0;
            }
            return !a.Inclusive || !b.Inclusive;
        };

        auto lessBoundary = [this](const TRightBoundary& a, const TRightBoundary& b) noexcept -> bool {
            TCellsRef left = a.Key;
            TCellsRef right = b.Key;
            if (int cmp = ComparePartKeys(left, right, KeyCellDefaults)) {
                return cmp < 0;
            }
            return !a.Inclusive && b.Inclusive;
        };

        TVector<TItemState*> enterQueue(Reserve(Items.size()));
        for (TItemState& item : Items) {
            item.NextPage = item.Part->Index.LookupRow(item.Slice.BeginRowId());
            Y_VERIFY(item.NextPage,
                    "Cannot find row %" PRIu64 " in the index for part %s",
                    item.Slice.BeginRowId(),
                    item.Part->Label.ToString().c_str());
            if (item.NextPage->GetRowId() != item.Slice.BeginRowId()) {
                ++item.NextPage;
            }
            item.First.Key = item.Slice.FirstKey.GetCells();
            item.First.RowId = item.Slice.FirstRowId;
            item.First.Inclusive = item.Slice.FirstInclusive;
            enterQueue.push_back(&item);
        }

        std::make_heap(enterQueue.begin(), enterQueue.end(), heapByFirstKeyMin);

        TVector<TCell> tmpStart;
        TVector<TCell> tmpEnd;

        bool reuseActive = true;
        while (enterQueue) {
            TRightBoundary end;

            // a separate scope to keep local variables
            {
                auto* item = enterQueue.front();
                std::pop_heap(enterQueue.begin(), enterQueue.end(), heapByFirstKeyMin);
                enterQueue.pop_back();

                TLeftBoundary start = item->First;
                bool viable = item->Reusable;

                // Protect start boundary against future overwrites
                tmpStart.assign(start.Key.begin(), start.Key.end());
                start.Key = tmpStart;

                if (item->NextPage && item->NextPage->GetRowId() == start.BeginRowId()) {
                    ++item->NextPage;
                } else {
                    // Sub-slice is not viable unless it starts on a page boundary
                    viable = false;
                }

                if (item->InitNextKey()) {
                    enterQueue.push_back(item);
                    std::push_heap(enterQueue.begin(), enterQueue.end(), heapByFirstKeyMin);

                    end.Key = item->NextKey;
                    end.RowId = item->NextPage->GetRowId();
                    end.Inclusive = false;
                } else {
                    end.Key = item->Slice.LastKey.GetCells();
                    end.RowId = item->Slice.LastRowId;
                    end.Inclusive = item->Slice.LastInclusive;
                }

                // Protect end boundary against future overwrites
                tmpEnd.assign(end.Key.begin(), end.Key.end());
                end.Key = tmpEnd;

                if (viable && end.EndRowId() != item->GetNextRowId()) {
                    // Sub-slice is not viable unless it ends on a page boundary
                    viable = false;
                }

                // Move on if current page has no intersection with others
                if (!enterQueue || item == enterQueue.front() || noIntersection(end, enterQueue.front()->First)) {
                    if (viable) {
                        if (reuseActive && lastItem == item && results.Reusable.back().Slice.EndRowId() == start.BeginRowId()) {
                            // Merge adjacent pages as long as it's from the same item
                            results.Reusable.back().Slice.LastKey = TSerializedCellVec(end.Key);
                            results.Reusable.back().Slice.LastRowId = end.RowId;
                            results.Reusable.back().Slice.LastInclusive = end.Inclusive;
                        } else {
                            auto& result = results.Reusable.emplace_back();
                            result.Part = item->Part;
                            result.Slice.FirstKey = TSerializedCellVec(start.Key);
                            result.Slice.FirstRowId = start.RowId;
                            result.Slice.FirstInclusive = start.Inclusive;
                            result.Slice.LastKey = TSerializedCellVec(end.Key);
                            result.Slice.LastRowId = end.RowId;
                            result.Slice.LastInclusive = end.Inclusive;
                            ++results.ExpectedSlices;
                            reuseActive = true;
                            lastItem = item;
                        }
                    } else if (reuseActive) {
                        ++results.ExpectedSlices;
                        reuseActive = false;
                        lastItem = nullptr;
                    }
                    continue;
                }
            }

            if (reuseActive) {
                ++results.ExpectedSlices;
                reuseActive = false;
                lastItem = nullptr;
            }

            // Now flush all items that have potential intersections, those cannot be reused
            while (enterQueue && !noIntersection(end, enterQueue.front()->First)) {
                auto* item = enterQueue.front();
                std::pop_heap(enterQueue.begin(), enterQueue.end(), heapByFirstKeyMin);
                enterQueue.pop_back();

                if (item->NextPage && item->NextPage->GetRowId() == item->First.BeginRowId()) {
                    ++item->NextPage;
                }

                TRightBoundary candidate;

                if (item->InitNextKey()) {
                    enterQueue.push_back(item);
                    std::push_heap(enterQueue.begin(), enterQueue.end(), heapByFirstKeyMin);

                    candidate.Key = item->NextKey;
                    candidate.RowId = item->NextPage->GetRowId();
                    candidate.Inclusive = false;
                } else {
                    candidate.Key = item->Slice.LastKey.GetCells();
                    candidate.RowId = item->Slice.LastRowId;
                    candidate.Inclusive = item->Slice.LastInclusive;
                }

                if (lessBoundary(end, candidate)) {
                    end = candidate;

                    // Protect end boundary against future overwrites
                    tmpEnd.assign(end.Key.begin(), end.Key.end());
                    end.Key = tmpEnd;
                }
            }
        }

        return results;
    }

    void TTableShard::RegisterItem(const TTablePart& info, TTablePart::TItem& item, bool isGarbage) noexcept {
        auto r = Levels->Add(info.Part, item.Slice);
        item.Level = r.Level;
        item.Position = r.Position;

        // register stats for the specific level
        if (PerLevelStats.size() <= item.Level->Index) {
            PerLevelStats.resize(item.Level->Index + 1);
        }
        PerLevelStats[item.Level->Index] += item.Stats;

        if (PerLevelGarbage.size() <= item.Level->Index) {
            PerLevelGarbage.resize(item.Level->Index + 1);
        }
        if (isGarbage) {
            PerLevelGarbage[item.Level->Index] += item.Stats.Size;
        }
    }

    bool TTableShard::FindSplitKey(TSerializedCellVec& foundKey, const TKeyCellDefaults& keyDefaults) const noexcept {
        TSplitStatIterator it(keyDefaults);

        for (const auto& kvInfo : Parts) {
            for (const auto& kvSlice : kvInfo.second.Slices) {
                it.AddSlice(kvInfo.second.Part.Get(), kvSlice.second.Slice, kvSlice.second.Stats.Size);
            }
        }

        TVector<TCell> bestSplitKey;
        ui64 bestLeftSize = it.LeftSize();
        ui64 bestRightSize = it.RightSize();
        Y_VERIFY(bestLeftSize == 0);
        Y_VERIFY(bestRightSize == Stats.Size, "Full stats size is out of sync!");

        bool stop = false;
        while (!stop && it.Next()) {
            TCellsRef splitKey = it.CurrentKey();
            auto leftSize = it.LeftSize();
            auto rightSize = it.RightSize();

            // left size increases, right size decreases
            // as soon as left becomes bigger than right it won't get any better
            if (leftSize > rightSize) {
                stop = true;
                auto currentDiff = bestRightSize - bestLeftSize;
                if (currentDiff <= leftSize - rightSize) {
                    // previous iteration has given us a better difference
                    break;
                }
            }

            bestSplitKey.assign(splitKey.begin(), splitKey.end());
            bestLeftSize = leftSize;
            bestRightSize = rightSize;
        }

        if (bestLeftSize < Stats.Size && bestRightSize < Stats.Size) {
            // We found a key that splits current shard somewhat evenly and
            // the size estimate of individual subshards actually decreses
            foundKey = TSerializedCellVec(bestSplitKey);
            return true;
        }

        // We cannot find split key for some reason
        // For example there's a single row-page that is way too big, or there
        // are lots of slices stacked on top of each other with no natural way
        // to divide them on a page boundary.
        return false;
    }

    bool TSliceSplitOp::Execute(IPages* env) {
        const TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults = Table->RowScheme->Keys;

        TVector<TCell> keyCellsBuffer(keyDefaults->Size());
        auto getKeyCells = [this, &keyDefaults, &keyCellsBuffer](ui64 keyId) {
            auto* key = Table->SplitKeys.FindPtr(keyId);
            Y_VERIFY(key, "Cannot find split key %" PRIu64, keyId);

            auto keyCells = key->GetCells();
            if (keyCells.size() < keyDefaults->Size()) {
                for (size_t i = 0; i < keyCells.size(); ++i) {
                    keyCellsBuffer[i] = keyCells[i];
                }
                for (size_t i = keyCells.size(); i < keyDefaults->Size(); ++i) {
                    keyCellsBuffer[i] = keyDefaults->Defs[i];
                }
                keyCells = keyCellsBuffer;
            }
            Y_VERIFY_DEBUG(keyCells.size() == keyDefaults->Size());

            return keyCells;
        };

        bool ok = true;
        TCharge charge(env, *Part, TTagsRef{ });
        for (size_t idx = 1; idx < Shards.size(); ++idx) {
            auto keyCells = getKeyCells(Shards[idx]->LeftKey);
            ok &= charge.SplitKey(keyCells, *keyDefaults, Slice.BeginRowId(), Slice.EndRowId());
        }

        if (!ok) {
            // Some pages are missing
            return false;
        }

        TSlice current = Slice;
        TVector<TSlice> results(Reserve(Shards.size()));
        TPartSimpleIt it(Part.Get(), { }, keyDefaults, env);
        for (size_t idx = 1; idx < Shards.size(); ++idx) {
            const TRowId currentBegin = current.BeginRowId();
            const TRowId currentEnd = current.EndRowId();
            if (currentBegin >= currentEnd) {
                break;
            }

            it.SetBounds(currentBegin, currentEnd);

            auto keyCells = getKeyCells(Shards[idx]->LeftKey);
            auto ready = it.Seek(keyCells, ESeek::Lower);
            Y_VERIFY(ready != EReady::Page, "Unexpected failure, precharge logic may be faulty");

            // This is row id of the first row >= split key
            const TRowId rowId = ready == EReady::Data ? it.GetRowId() : currentEnd;
            Y_VERIFY_DEBUG(currentBegin <= rowId);
            Y_VERIFY_DEBUG(rowId <= currentEnd);

            // This is the first key >= split key
            TSerializedCellVec rowKey;
            if (rowId != currentEnd) {
                rowKey = TSerializedCellVec(it.GetRawKey());
            } else if (Y_UNLIKELY(ComparePartKeys(current.LastKey.GetCells(), keyCells, *keyDefaults) < 0)) {
                // This shouldn't normally happen, but better safe than sorry
                // Current split key is actually out of bounds for the slice
                // Since there's no real intersection leave it as is
                break;
            }

            // Do we have any rows to the left of the split?
            if (currentBegin < rowId) {
                auto& left = results.emplace_back();
                left.FirstKey = std::move(current.FirstKey);
                left.FirstRowId = current.FirstRowId;
                left.FirstInclusive = current.FirstInclusive;
                if ((rowId - currentBegin) == 1 && left.FirstInclusive) {
                    // Single row, just reuse the exact same key
                    left.LastKey = left.FirstKey;
                    left.LastRowId = left.FirstRowId;
                    left.LastInclusive = true;
                } else {
                    // Seek to previous row
                    ready = it.Seek(rowId - 1);
                    Y_VERIFY(ready == EReady::Data, "Unexpected failure, precharge logic may be faulty");
                    left.LastKey = TSerializedCellVec(it.GetRawKey());
                    left.LastRowId = it.GetRowId();
                    left.LastInclusive = true;
                    if ((rowId - currentBegin) == 1) {
                        // Single row, just reuse the exact same key
                        Y_VERIFY_DEBUG(!left.FirstInclusive);
                        left.FirstKey = left.LastKey;
                        left.FirstRowId = left.LastRowId;
                        left.FirstInclusive = true;
                    }
                }

                Y_VERIFY_DEBUG(left.BeginRowId() == currentBegin);
                Y_VERIFY_DEBUG(left.EndRowId() == rowId);
            }

            current.FirstKey = std::move(rowKey);
            current.FirstRowId = rowId;
            current.FirstInclusive = true;
        }

        if (current.BeginRowId() < current.EndRowId()) {
            auto& left = results.emplace_back();
            if (current.Rows() == 1 && (current.FirstInclusive ^ current.LastInclusive)) {
                if (current.FirstInclusive) {
                    left.FirstKey = current.FirstKey;
                    left.FirstRowId = current.FirstRowId;
                    left.FirstInclusive = true;
                    left.LastKey = current.FirstKey;
                    left.LastRowId = current.FirstRowId;
                    left.LastInclusive = true;
                } else {
                    Y_VERIFY_DEBUG(current.LastInclusive);
                    left.FirstKey = current.LastKey;
                    left.FirstRowId = current.LastRowId;
                    left.FirstInclusive = true;
                    left.LastKey = current.LastKey;
                    left.LastRowId = current.LastRowId;
                    left.LastInclusive = true;
                }
            } else {
                left = std::move(current);
            }
        }

        TSliceSplitResult result;
        result.Part = Part;
        result.OldSlice = Slice;
        result.NewSlices = std::move(results);
        result.Shards = std::move(Shards);
        Consumer->OnSliceSplitResult(std::move(result));

        return true;
    }

    bool TUnderlayMask::HasKey(TCellsRef key) noexcept {
        while (Position != Bounds.end()) {
            // TODO: we could use binary search, but it's probably useless
            const auto& current = *Position;
            auto cmp = ComparePartKeys(key, current.LastKey.GetCells(), *RowScheme->Keys);
            if (cmp < 0 || (cmp == 0 && current.LastInclusive)) {
                break;
            }
            ++Position;
        }
        Y_VERIFY(ValidatePosition(key), "TUnderlayMask::HasKey called with out of order keys");
        if (Position != Bounds.end()) {
            const auto& current = *Position;
            auto cmp = ComparePartKeys(current.FirstKey.GetCells(), key, *RowScheme->Keys);
            return cmp < 0 || (cmp == 0 && current.FirstInclusive);
        }
        return false;
    }

    bool TUnderlayMask::ValidateOrder() const noexcept {
        auto it = Bounds.begin();
        if (it == Bounds.end()) {
            return true;
        }
        auto last = it++;
        while (it != Bounds.end()) {
            if (!TBounds::LessByKey(*last, *it, *RowScheme->Keys)) {
                return false;
            }
            last = it++;
        }
        return true;
    }

    bool TUnderlayMask::ValidatePosition(TCellsRef key) const noexcept {
        auto pos = Position;
        if (pos == Bounds.begin()) {
            return true;
        }
        auto& prev = *--pos;
        auto cmp = ComparePartKeys(prev.LastKey.GetCells(), key, *RowScheme->Keys);
        return cmp < 0 || (cmp == 0 && !prev.LastInclusive);
    }

    THolder<TUnderlayMask> TUnderlayMask::Build(
            TIntrusiveConstPtr<TRowScheme> rowScheme,
            TVector<const TBounds*>& input) noexcept
    {
        const TKeyCellDefaults& keyDefaults = *rowScheme->Keys;

        // Sorts heap by the first key (returns true when a->FirstKey < b->FirstKey)
        auto heapByFirstKeyMin = [&keyDefaults](const TBounds* b, const TBounds* a) noexcept -> bool {
            TCellsRef left = a->FirstKey.GetCells();
            TCellsRef right = b->FirstKey.GetCells();
            if (int cmp = ComparePartKeys(left, right, keyDefaults)) {
                return cmp < 0;
            }
            return a->FirstInclusive && !b->FirstInclusive;
        };

        // Returns true when a->LastKey < b->LastKey
        auto lastKeyLess = [&keyDefaults](const TBounds* a, const TBounds* b) noexcept -> bool {
            TCellsRef left = a->LastKey.GetCells();
            TCellsRef right = b->LastKey.GetCells();
            if (int cmp = ComparePartKeys(left, right, keyDefaults)) {
                return cmp < 0;
            }
            return !a->LastInclusive && b->LastInclusive;
        };

        // Returns true when a->LastKey may be stitched with b->FirstKey
        auto mayStitch = [&keyDefaults](const TBounds* a, const TBounds* b) noexcept -> bool {
            TCellsRef left = a->LastKey.GetCells();
            TCellsRef right = b->FirstKey.GetCells();
            int cmp = ComparePartKeys(left, right, keyDefaults);
            if (cmp < 0) {
                return false;
            }
            if (cmp == 0) {
                return a->LastInclusive || b->FirstInclusive;
            }
            return true;
        };

        TVector<TBounds> results(Reserve(input.size()));

        std::make_heap(input.begin(), input.end(), heapByFirstKeyMin);

        while (input) {
            const TBounds* first = input.front();
            std::pop_heap(input.begin(), input.end(), heapByFirstKeyMin);
            input.pop_back();

            const TBounds* last = first;

            while (input && mayStitch(last, input.front())) {
                auto* current = input.front();
                std::pop_heap(input.begin(), input.end(), heapByFirstKeyMin);
                input.pop_back();

                if (lastKeyLess(last, current)) {
                    last = current;
                }
            }

            results.emplace_back(
                    first->FirstKey,
                    last->LastKey,
                    first->FirstInclusive,
                    last->LastInclusive);
        }

        return MakeHolder<TUnderlayMask>(std::move(rowScheme), std::move(results));
    }

    TSplitKeys::TSplitKeys(TIntrusiveConstPtr<TRowScheme> rowScheme, TSplitKeys::TKeysVec keys)
        : RowScheme(std::move(rowScheme))
        , Keys(std::move(keys))
    {
        Y_VERIFY(ValidateOrder(), "TSplitKeys got keys in an invalid order");
        Reset();
    }

    TSplitKeys::TSplitKeys(TIntrusiveConstPtr<TRowScheme> rowScheme, TVector<const TBounds*> bounds)
        : RowScheme(std::move(rowScheme))
    {
        auto boundsLess = [this](const TBounds* a, const TBounds* b) noexcept -> bool {
            if (auto cmp = ComparePartKeys(a->FirstKey.GetCells(), b->FirstKey.GetCells(), *RowScheme->Keys)) {
                return cmp < 0;
            }
            return a->FirstInclusive && !b->FirstInclusive;
        };
        std::sort(bounds.begin(), bounds.end(), boundsLess);

        Keys.reserve(bounds.size());
        Inclusive.reserve(bounds.size());

        const TBounds* last = nullptr;
        for (const TBounds* edge : bounds) {
            if (!last || boundsLess(last, edge)) {
                Keys.emplace_back(edge->FirstKey);
                Inclusive.push_back(edge->FirstInclusive);
            }
            last = edge;
        }

        Y_VERIFY(ValidateOrder(), "TSplitKeys generated keys in an invalid order");
        Reset();
    }

    bool TSplitKeys::ShouldSplit(TCellsRef key) noexcept {
        bool splitFound = false;
        while (Position != Keys.end()) {
            // TODO: we could use binary search, but it's probably useless
            const auto& current = *Position;
            auto cmp = ComparePartKeys(key, current.GetCells(), *RowScheme->Keys);
            if (cmp < 0 || (cmp == 0 && !IsInclusive(Position))) {
                break;
            }
            splitFound = true;
            ++Position;
        }
        Y_VERIFY(ValidatePosition(key), "TSplitKeys::ShouldSplit called with out of order keys");
        return splitFound;
    }

    bool TSplitKeys::ValidateOrder() const noexcept {
        auto it = Keys.begin();
        if (it == Keys.end()) {
            return true;
        }
        auto posLess = [this](const TKeysVec::const_iterator& a, const TKeysVec::const_iterator& b) noexcept -> bool {
            if (auto cmp = ComparePartKeys(a->GetCells(), b->GetCells(), *RowScheme->Keys)) {
                return cmp < 0;
            }
            return IsInclusive(a) && !IsInclusive(b);
        };
        auto last = it++;
        while (it != Keys.end()) {
            if (!posLess(last, it)) {
                return false;
            }
            last = it++;
        }
        return true;
    }

    bool TSplitKeys::ValidatePosition(TCellsRef key) const noexcept {
        auto prev = Position;
        if (prev == Keys.begin()) {
            return true;
        }
        --prev;
        if (auto cmp = ComparePartKeys(prev->GetCells(), key, *RowScheme->Keys)) {
            return cmp < 0;
        }
        return IsInclusive(prev);
    }

    bool TSplitKeys::IsInclusive(TVector<TSerializedCellVec>::const_iterator pos) const noexcept {
        size_t index = pos - Keys.begin();
        return index >= Inclusive.size() || Inclusive[index];
    }

    void TShardedCompactionParams::Describe(IOutputStream& out) const noexcept {
        // TODO: display some additional info
        TCompactionParams::Describe(out);
    }

    void TShardedCompactionStrategy::Start(TCompactionState state) {
        Y_VERIFY(!Policy, "Strategy has already been started");

        const auto* scheme = Backend->TableScheme(Table);
        Policy = scheme->CompactionPolicy;
        TableInfo.RowScheme = Backend->RowScheme(Table);

        TShardedStrategyStateInfo header;
        TVector<ui64> splitKeyIds;
        TVector<ui64> shardIds;

        if (auto* pRawHeader = state.StateSnapshot.FindPtr(0)) {
            bool ok = ParseFromStringNoSizeLimit(header, *pRawHeader);
            Y_VERIFY(ok);

            TableInfo.LastSplitKey = header.GetLastSplitKey();
            TableInfo.LastShardId = header.GetLastShardId();

            for (ui64 keyId : header.GetSplitKeys()) {
                auto* pRawKey = state.StateSnapshot.FindPtr(keyId);
                Y_VERIFY(pRawKey);
                TableInfo.SplitKeys[keyId] = TSerializedCellVec(*pRawKey);
                splitKeyIds.emplace_back(keyId);
            }

            for (ui64 shardId : header.GetShards()) {
                shardIds.emplace_back(shardId);
            }
        }

        const auto& keyDefaults = *TableInfo.RowScheme->Keys;
        std::sort(splitKeyIds.begin(), splitKeyIds.end(), [&](ui64 a, ui64 b) -> bool {
            auto left = TableInfo.SplitKeys.at(a).GetCells();
            auto right = TableInfo.SplitKeys.at(b).GetCells();
            return ComparePartKeys(left, right, keyDefaults) < 0;
        });

        Shards.PushBack(new TTableShard());
        for (ui64 keyId : splitKeyIds) {
            auto* last = Shards.Back();
            Shards.PushBack(new TTableShard());
            auto* shard = Shards.Back();
            last->RightKey = keyId;
            shard->LeftKey = keyId;
        }

        if (shardIds) {
            Y_VERIFY(shardIds.size() == splitKeyIds.size() + 1,
                "Have %" PRISZT " shards with %" PRISZT " split keys",
                shardIds.size(), splitKeyIds.size());

            auto it = shardIds.begin();
            for (auto& shard : Shards) {
                Y_VERIFY(it != shardIds.end());
                shard.Id = *it++;
            }
            Y_VERIFY(it == shardIds.end());
        } else if (splitKeyIds) {
            // Assume we upgraded from some old dev code, renumber from 1
            for (auto& shard : Shards) {
                shard.Id = ++TableInfo.LastShardId;
            }
        }

        for (auto& shard : Shards) {
            // Initialize all shards while empty
            shard.Levels.Reset(new TLevels(TableInfo.RowScheme->Keys));
        }

        for (auto& part : Backend->TableColdParts(Table)) {
            ColdParts.emplace_back(std::move(part));
            Y_FAIL("Sharded compaction does not support cold parts");
        }

        auto parts = Backend->TableParts(Table);
        std::sort(parts.begin(), parts.end(),
            [](const TPartView& a, const TPartView& b) -> bool {
                if (a->Epoch != b->Epoch) {
                    return a->Epoch < b->Epoch;
                }
                return a->Label < b->Label;
            });

        while (parts) {
            const auto& partView = parts.back();
            TPartDataSizeHelper helper(partView.Part.Get());
            ui64 fullSize = helper.CalcSize(0, Max<TRowId>());
            if (fullSize >= Policy->ShardPolicy.GetMinSliceSize()) {
                break;
            }
            AllBackingSize += partView.Part->BackingSize();
            NurseryDataSize += fullSize;
            auto& nurseryItem = Nursery.emplace_back();
            nurseryItem.PartView = partView;
            nurseryItem.DataSize = fullSize;
            parts.pop_back();
        }

        for (const auto& partView : parts) {
            Y_VERIFY(partView.Slices && !partView.Slices->empty());
            EnsureGlobalPart(partView.Part, partView.Slices);
        }

        AddParts(std::move(parts));

        CheckCompactions();

        // TODO: schedule split/merge if necessary (performed in ApplyChanges)
    }

    void TShardedCompactionStrategy::Stop() {
        while (PendingSliceSplits) {
            auto* opWeak = PendingSliceSplits.Front();
            auto readId = opWeak->GetReadId();
            bool ok = Backend->CancelRead(readId);
            Y_VERIFY(ok, "Failed to cancel read with id %" PRIu64, readId);
        }

        CancelTask(ForcedCompactionTask);
        ForcedCompactionPending = false;

        for (auto& shard : Shards) {
            CancelTask(shard.Task);
        }

        if (MemCompactionId != 0) {
            Backend->CancelCompaction(MemCompactionId);
            MemCompactionId = 0;
            MemCompactionForced = false;
            NurseryTaken = 0;
        }

        // Make it possible to Start again
        TableInfo = { };
        Policy = nullptr;
        Shards.Clear();
        SliceSplitResults.clear();
        AllParts.clear();
        AllBackingSize = 0;
        Nursery.clear();
        NurseryDataSize = 0;
        ColdParts.clear();
    }

    void TShardedCompactionStrategy::ReflectSchema() {
        const auto* scheme = Backend->TableScheme(Table);

        TString err;
        bool ok = NLocalDb::ValidateCompactionPolicyChange(*Policy, *scheme->CompactionPolicy, err);
        Y_VERIFY(ok, "table %s id %u: %s", scheme->Name.c_str(), scheme->Id, err.c_str());

        Policy = scheme->CompactionPolicy;
        TableInfo.RowScheme = Backend->RowScheme(Table);

        CheckCompactions();
    }

    void TShardedCompactionStrategy::ReflectRemovedRowVersions() {
        // nothing
    }

    void TShardedCompactionStrategy::UpdateCompactions() {
        CheckCompactions();
    }

    float TShardedCompactionStrategy::GetOverloadFactor() {
        // TODO: implement overload factor
        return 0.0f;
    }

    ui64 TShardedCompactionStrategy::GetBackingSize() {
        return AllBackingSize;
    }

    ui64 TShardedCompactionStrategy::GetBackingSize(ui64 ownerTabletId) {
        // FIXME: maybe implement some day
        Y_UNUSED(ownerTabletId);
        return AllBackingSize;
    }

    ui64 TShardedCompactionStrategy::BeginMemCompaction(TTaskId taskId, TSnapEdge edge, ui64 forcedCompactionId) {
        auto params = MakeHolder<TShardedCompactionParams>();
        params->Table = Table;
        params->TaskId = taskId;
        params->Edge = edge;
        params->KeepInCache = true;

        NurseryTaken = 0;
        if (edge.Head == TEpoch::Max() && Nursery && forcedCompactionId == 0) {
            ui64 expectedSize = Backend->TableMemSize(Table, edge.Head);
            if (expectedSize > 0) {
                bool takeAll = (
                    (expectedSize + NurseryDataSize) >= (2 * Policy->ShardPolicy.GetMinSliceSize()) ||
                    (Nursery.size() >= Policy->ShardPolicy.GetMaxSlicesPerLevel()));
                while (NurseryTaken < Nursery.size()) {
                    auto& next = Nursery[NurseryTaken];
                    if (next.DataSize > expectedSize && !takeAll) {
                        break;
                    }
                    params->Parts.push_back(next.PartView);
                    expectedSize += next.DataSize;
                    ++NurseryTaken;
                }
            }
        }

        if (!Policy->KeepEraseMarkers) {
            params->IsFinal = NurseryTaken == Nursery.size() && AllParts.empty();
        } else {
            params->IsFinal = false;
        }

        if (!params->IsFinal && !Policy->KeepEraseMarkers) {
            TVector<const TBounds*> allBounds;
            for (size_t pos = NurseryTaken; pos < Nursery.size(); ++pos) {
                auto& item = Nursery[pos];
                for (const auto& slice : *item.PartView.Slices) {
                    allBounds.push_back(&slice);
                }
            }
            for (auto& kv : AllParts) {
                for (auto& slice : *kv.second.Slices) {
                    allBounds.push_back(&slice);
                }
            }
            Y_VERIFY(allBounds, "Unexpected lack of slice bounds");
            params->UnderlayMask = TUnderlayMask::Build(TableInfo.RowScheme, allBounds);
        }

        if (TableInfo.SplitKeys) {
            TVector<TSerializedCellVec> splitKeys;
            auto* last = Shards.Back();
            auto* shard = Shards.Front();
            while (shard != last) {
                splitKeys.push_back(TableInfo.SplitKeys.at(shard->RightKey));
                shard = shard->Next()->Node();
            }
            Y_VERIFY(splitKeys, "Unexpected lack of split keys");
            params->SplitKeys = MakeHolder<TSplitKeys>(TableInfo.RowScheme, std::move(splitKeys));
        }

        MemCompactionId = Backend->BeginCompaction(std::move(params));
        MemCompactionForced = forcedCompactionId != 0;
        return MemCompactionId;
    }

    TCompactionChanges TShardedCompactionStrategy::CompactionFinished(
            ui64 compactionId,
            THolder<TCompactionParams> paramsRaw,
            THolder<TCompactionResult> result)
    {
        auto* params = CheckedCast<TShardedCompactionParams*>(paramsRaw.Get());
        bool processNursery = false;
        bool doNurseryFlush = false;

        if (auto* shard = params->InputShard) {
            Y_VERIFY(shard->Task.State == EState::Compacting);
            Y_VERIFY(shard->Task.CompactionId == compactionId);
            shard->Task.State = EState::Free;
            shard->Task.TaskId = 0;
            shard->Task.CompactionId = 0;
            shard->FailingLevel = Max<size_t>();

            // Every compacted part should have the original from our state
            Y_VERIFY(params->Parts.size() == params->Original.size());
        } else if (compactionId == ForcedCompactionTask.CompactionId) {
            Y_VERIFY(ForcedCompactionTask.State == EState::Compacting);
            ForcedCompactionTask.State = EState::Free;
            ForcedCompactionTask.TaskId = 0;
            ForcedCompactionTask.CompactionId = 0;
        } else {
            Y_VERIFY(MemCompactionId == compactionId);
            MemCompactionId = 0;

            if (MemCompactionForced) {
                doNurseryFlush = true;
                MemCompactionForced = false;
                ForcedCompactionPending = true;
            } else {
                processNursery = true;
            }

            // Mem compaction only uses nursery, no reused or original parts expected
            Y_VERIFY(!params->Original);
            Y_VERIFY(!params->Reused);

            for (const auto& partView : params->Parts) {
                Y_VERIFY(NurseryTaken > 0);
                auto& item = Nursery.front();
                Y_VERIFY(partView.Part == item.PartView.Part);
                AllBackingSize -= partView.Part->BackingSize();
                NurseryDataSize -= item.DataSize;
                Nursery.pop_front();
                --NurseryTaken;
            }
            Y_VERIFY(NurseryTaken == 0);
            params->Parts.clear();
        }

        // These parts may have become garbage after compaction
        THashSet<TGlobalPart*> updateGarbageQueue;

        // Remove compacted slices from global parts
        for (const auto& partView : params->Parts) {
            auto label = partView->Label;
            auto* allInfo = AllParts.FindPtr(label);
            Y_VERIFY(allInfo, "Compacted part %s is not registered", label.ToString().c_str());

            allInfo->Slices = TSlices::Subtract(allInfo->Slices, partView.Slices);
            updateGarbageQueue.emplace(allInfo);
        }

        // Remove originally chosen slices from our state
        for (auto& partView : params->Original) {
            auto label = partView->Label;
            auto* allInfo = AllParts.FindPtr(label);
            Y_VERIFY(allInfo, "Compacted part %s is not registered", label.ToString().c_str());

            size_t removedCount = 0;

            // TODO: make it more efficient
            auto writePos = allInfo->Shards.begin();
            auto readPos = allInfo->Shards.begin();
            while (readPos != allInfo->Shards.end()) {
                auto* partShard = *readPos;
                auto* info = partShard->Parts.FindPtr(label);
                Y_VERIFY(info, "Compacted part %s cannot be found in a shard", label.ToString().c_str());
                for (const auto& input : *partView.Slices) {
                    auto pos = info->Slices.find(input);
                    if (pos != info->Slices.end()) {
                        auto& item = pos->second;
                        partShard->Levels.Reset();
                        partShard->PerLevelStats.clear();
                        partShard->PerLevelGarbage.clear();
                        partShard->Stats -= item.Stats;
                        info->Slices.erase(pos);
                        ++removedCount;
                    }
                }
                if (info->Slices.empty()) {
                    // This part no longer contributes to this shard
                    partShard->Parts.erase(label);
                    ++readPos;
                    continue;
                }
                if (writePos != readPos) {
                    *writePos = *readPos;
                }
                ++writePos;
                ++readPos;
            }
            if (writePos != allInfo->Shards.end()) {
                allInfo->Shards.erase(writePos, allInfo->Shards.end());
            }

            Y_VERIFY(removedCount == partView.Slices->size(), "Not all slices have been removed");

            if (allInfo->Shards.empty() && allInfo->SplitBlocks == 0 && allInfo->Slices->empty()) {
                updateGarbageQueue.erase(allInfo);
                TableInfo.GarbageParts.erase(label);
                AllBackingSize -= allInfo->Part->BackingSize();
                AllParts.erase(label);
            }
        }

        for (auto* allInfo : updateGarbageQueue) {
            UpdateGarbageStats(allInfo);
        }

        TVector<TPartView> parts;

        auto flushNursery = [this, &parts]() {
            while (Nursery) {
                // Go from oldest to newest
                auto& item = Nursery.back();
                auto& partView = item.PartView;
                AllBackingSize -= partView.Part->BackingSize();
                NurseryDataSize -= item.DataSize;
                parts.push_back(std::move(partView));
                Nursery.pop_back();
            }
        };

        if (doNurseryFlush) {
            flushNursery();
        }

        if (processNursery) {
            // We expect parts to be from the same epoch and in correct order
            for (auto& partView : result->Parts) {
                TPartDataSizeHelper helper(partView.Part.Get());
                ui64 fullSize = helper.CalcSize(0, Max<TRowId>());
                if (fullSize >= Policy->ShardPolicy.GetMinSliceSize()) {
                    flushNursery();
                    parts.push_back(std::move(partView));
                    continue;
                }
                // Add new item to front (it's the newest)
                AllBackingSize += partView.Part->BackingSize();
                NurseryDataSize += fullSize;
                auto& nurseryItem = Nursery.emplace_front();
                nurseryItem.PartView = std::move(partView);
                nurseryItem.DataSize = fullSize;
            }
        } else {
            parts.reserve(parts.size() + result->Parts.size());
            for (auto& partView : result->Parts) {
                parts.push_back(std::move(partView));
            }
        }
        result->Parts.clear();

        // Register all "new" parts in the global state
        for (const auto& partView : parts) {
            Y_VERIFY(partView.Slices && !partView.Slices->empty());
            EnsureGlobalPart(partView.Part, partView.Slices);
        }

        // Add any reused slices back to our state
        if (params->Reused) {
            parts.reserve(parts.size() + params->Reused.size());
            for (auto& partView : params->Reused) {
                parts.emplace_back(std::move(partView));
            }
            params->Reused.clear();
        }

        AddParts(std::move(parts));

        return ApplyChanges();
    }

    void TShardedCompactionStrategy::PartMerged(TPartView part, ui32 level) {
        Y_VERIFY(level == 255, "Unexpected level of the merged part");

        Y_VERIFY(part.Slices && !part.Slices->empty());
        EnsureGlobalPart(part.Part, part.Slices);

        TVector<TPartView> parts(Reserve(1));
        parts.emplace_back(std::move(part));
        AddParts(std::move(parts));
    }

    void TShardedCompactionStrategy::PartMerged(TIntrusiveConstPtr<TColdPart> part, ui32 level) {
        Y_VERIFY(level == 255, "Unexpected level of the merged part");

        ColdParts.emplace_back(std::move(part));
        Y_FAIL("Sharded compaction does not support cold parts");
    }

    TCompactionChanges TShardedCompactionStrategy::PartsRemoved(TArrayRef<const TLogoBlobID> parts) {
        Y_UNUSED(parts);

        // For simplicity just stop and start again
        auto state = SnapshotState();

        Stop();

        Start(std::move(state));

        // We don't keep per-part state, so no changes
        return { };
    }

    TCompactionChanges TShardedCompactionStrategy::ApplyChanges() {
        RequestChangesPending = false;

        TCompactionChanges changes{ };

        struct TState : public TIntrusiveListItem<TState> {
            size_t Index = Max<size_t>();
        };

        TIntrusiveList<TState> needSort;
        THashMap<TLogoBlobID, TState> byLabel;

        TVector<TPartView> parts(Reserve(SliceSplitResults.size()));

        for (auto& result : SliceSplitResults) {
            Y_VERIFY(result.Part, "Unexpected result without a part");

            TChangeSlices* change;

            auto& state = byLabel[result.Part->Label];
            if (state.Index != Max<size_t>()) {
                needSort.PushBack(&state);
                change = &changes.SliceChanges[state.Index];
            } else {
                state.Index = changes.SliceChanges.size();
                change = &changes.SliceChanges.emplace_back();
                change->Label = result.Part->Label;
            }

            // New slices are guaranteed to be already sorted
            change->NewSlices.insert(change->NewSlices.end(),
                    result.NewSlices.begin(), result.NewSlices.end());

            auto* allInfo = AllParts.FindPtr(result.Part->Label);
            Y_VERIFY(allInfo, "Cannot find a globally registered part %s", result.Part->Label.ToString().c_str());
            allInfo->SplitBlocks--;

            // Construct a fake TPartView so we may reuse AddParts method
            parts.emplace_back(TPartView{ std::move(result.Part), nullptr, new TSlices(std::move(result.NewSlices)) });

            // All affected shards are no longer blocked by this op
            for (auto* affectedShard : result.Shards) {
                affectedShard->SplitBlocks--;
            }
        }
        SliceSplitResults.clear();

        // We may need to sort some changes
        for (const auto& state : needSort) {
            auto cmpByRowId = [](const TSlice& a, const TSlice& b) noexcept -> bool {
                return a.EndRowId() <= b.BeginRowId();
            };
            auto& change = changes.SliceChanges[state.Index];
            std::sort(change.NewSlices.begin(), change.NewSlices.end(), cmpByRowId);
        }

        // Apply changes to our global state (matches the state after method returns)
        for (const auto& change : changes.SliceChanges) {
            auto* allInfo = AllParts.FindPtr(change.Label);
            Y_VERIFY(allInfo, "Generated changes to unregistered part %s", change.Label.ToString().c_str());
            allInfo->Slices = TSlices::Replace(allInfo->Slices, change.NewSlices);
        }

        // We couldn't see these slices during split, add them back
        AddParts(std::move(parts));

        // Check for split/merge unless forced compaction is currently running
        if (ForcedCompactionTask.State != EState::Compacting) {
            const auto& policy = Policy->ShardPolicy;

            auto isFreeShard = [](TTableShard* shard) -> bool {
                if (shard->SplitBlocks) {
                    return false;
                }
                return shard->Task.State == EState::Free || shard->Task.State == EState::Pending;
            };

            bool stateChanged = false;
            const auto minShardSize = policy.GetMinShardSize();
            const auto maxShardSize = policy.GetMaxShardSize();

            // Split shards that are too big
            if (maxShardSize > 0) {
                TVector<TTableShard*> bigShards;
                for (auto& shard : Shards) {
                    if (isFreeShard(&shard) && shard.Stats.Size > maxShardSize) {
                        if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                            logl << "Table " << Table << " has a big shard " << shard.Id << " with "
                                << shard.Stats.Size << " > " << maxShardSize << " bytes";
                        }

                        bigShards.push_back(&shard);
                    }
                }

                parts.clear();
                for (auto* shard : bigShards) {
                    TSerializedCellVec splitKey;
                    if (shard->FindSplitKey(splitKey, *TableInfo.RowScheme->Keys)) {
                        ui64 newKeyId = ++TableInfo.LastSplitKey;

                        if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                            logl << "Table " << Table << " adding shard key " << newKeyId
                                << ": " << TPrintableTypedCells(splitKey.GetCells(), TableInfo.RowScheme->Keys->BasicTypes());
                        }

                        changes.StateChanges[newKeyId] = splitKey.GetBuffer();
                        TableInfo.SplitKeys[newKeyId] = splitKey;

                        auto left = MakeHolder<TTableShard>();
                        auto right = MakeHolder<TTableShard>();
                        left->Id = ++TableInfo.LastShardId;
                        left->LeftKey = shard->LeftKey;
                        left->RightKey = newKeyId;
                        right->Id = ++TableInfo.LastShardId;
                        right->LeftKey = newKeyId;
                        right->RightKey = shard->RightKey;

                        for (auto& kvPart : shard->Parts) {
                            TIntrusiveConstPtr<TPart> part = kvPart.second.Part;
                            TVector<TSlice> slices(Reserve(kvPart.second.Slices.size()));
                            for (auto& kvSlice : kvPart.second.Slices) {
                                slices.emplace_back(kvSlice.second.Slice);
                            }

                            TGlobalPart* allInfo = AllParts.FindPtr(part->Label);
                            Y_VERIFY(allInfo);
                            auto end = std::remove(allInfo->Shards.begin(), allInfo->Shards.end(), shard);
                            allInfo->Shards.erase(end, allInfo->Shards.end());

                            // Construct a fake TPartView so we may reuse AddParts method
                            parts.emplace_back(TPartView{std::move(part), nullptr, new TSlices(std::move(slices))});
                        }

                        left.Release()->LinkBefore(shard);
                        right.Release()->LinkBefore(shard);

                        CancelTask(shard->Task);
                        delete shard;

                        stateChanged = true;
                    }
                }

                if (parts) {
                    // Add back all the slices we have taken from splits
                    AddParts(std::move(parts));
                }
            }

            // Merge shards that are too small
            if (Shards.Size() > 1) {
                using TShardList = TList<TTableShard*>;
                using TShardHash = THashMap<TTableShard*, TShardList::iterator>;
                TShardList smallShardList;
                TShardHash smallShardHash;
                for (auto& shard : Shards) {
                    if (isFreeShard(&shard) && shard.Stats.Size <= minShardSize) {
                        if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                            logl << "Table " << Table << " has a small shard " << shard.Id << " with "
                                << shard.Stats.Size << " > " << minShardSize << " bytes";
                        }

                        smallShardHash[&shard] = smallShardList.insert(smallShardList.end(), &shard);
                    }
                }

                // sort small shards by their data size
                smallShardList.sort([](TTableShard* a, TTableShard* b) -> bool {
                    return a->Stats.Size < b->Stats.Size;
                });

                parts.clear();
                while (!smallShardList.empty()) {
                    auto* first = smallShardList.front();
                    smallShardHash.erase(first);
                    smallShardList.pop_front();

                    // Empty shards are a special case, it's ok for them to just disappear
                    // This also doesn't change the state of their neighbors in any way
                    // The only side effect would be for those shards to become wider
                    if (first->Parts.empty()) {
                        auto* shard = first;
                        Y_VERIFY(!shard->SplitBlocks);

                        ui64 removedKey = 0;
                        if (shard != Shards.Back()) {
                            removedKey = shard->RightKey;
                            Y_VERIFY(removedKey != 0);
                            auto* target = shard->Next()->Node();
                            target->LeftKey = shard->LeftKey;
                        } else if (shard != Shards.Front()) {
                            removedKey = shard->LeftKey;
                            Y_VERIFY(removedKey != 0);
                            auto* target = shard->Prev()->Node();
                            target->RightKey = shard->RightKey;
                        }

                        if (removedKey) {
                            auto it = TableInfo.SplitKeys.find(removedKey);
                            Y_VERIFY(it != TableInfo.SplitKeys.end());

                            if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                                logl << "Table " << Table << " removing shard key " << it->first
                                    << ": " << TPrintableTypedCells(it->second.GetCells(), TableInfo.RowScheme->Keys->BasicTypes());
                            }

                            changes.StateChanges[removedKey] = { };
                            TableInfo.SplitKeys.erase(it);

                            CancelTask(shard->Task);
                            delete shard;

                            stateChanged = true;
                        }

                        continue;
                    }

                    auto* last = first;
                    ui64 mergedSize = first->Stats.Size;

                    while (true) {
                        auto* left = first != Shards.Front() ? first->Prev()->Node() : nullptr;
                        auto* right = last != Shards.Back() ? last->Next()->Node() : nullptr;
                        if (left && !smallShardHash.contains(left)) {
                            left = nullptr;
                        }
                        if (right && !smallShardHash.contains(right)) {
                            right = nullptr;
                        }
                        if (left && right) {
                            if (left->Stats.Size < right->Stats.Size) {
                                right = nullptr;
                            } else {
                                left = nullptr;
                            }
                        }
                        if (left) {
                            if (mergedSize + left->Stats.Size > maxShardSize && maxShardSize > 0) {
                                break;
                            }
                            first = left;
                            smallShardList.erase(smallShardHash.at(left));
                            smallShardHash.erase(left);
                            mergedSize += left->Stats.Size;
                        } else if (right) {
                            if (mergedSize + right->Stats.Size > maxShardSize && maxShardSize > 0) {
                                break;
                            }
                            last = right;
                            smallShardList.erase(smallShardHash.at(right));
                            smallShardHash.erase(right);
                            mergedSize += right->Stats.Size;
                        } else {
                            break;
                        }
                    }

                    if (first != last) {
                        auto merged = MakeHolder<TTableShard>();
                        merged->Id = ++TableInfo.LastShardId;
                        merged->LeftKey = first->LeftKey;
                        merged->RightKey = last->RightKey;
                        merged.Release()->LinkBefore(first);

                        TTableShard* next = first;
                        while (next) {
                            auto* shard = next;
                            next = (shard != last) ? shard->Next()->Node() : nullptr;

                            for (auto& kvPart : shard->Parts) {
                                TIntrusiveConstPtr<TPart> part = kvPart.second.Part;
                                TVector<TSlice> slices(Reserve(kvPart.second.Slices.size()));
                                for (auto& kvSlice : kvPart.second.Slices) {
                                    slices.emplace_back(kvSlice.second.Slice);
                                }

                                TGlobalPart* allInfo = AllParts.FindPtr(part->Label);
                                Y_VERIFY(allInfo);
                                auto end = std::remove(allInfo->Shards.begin(), allInfo->Shards.end(), shard);
                                allInfo->Shards.erase(end, allInfo->Shards.end());

                                // Construct a fake TPartView so we may reuse AddParts method
                                parts.emplace_back(TPartView{std::move(part), nullptr, new TSlices(std::move(slices))});
                            }

                            if (shard != first) {
                                auto it = TableInfo.SplitKeys.find(shard->LeftKey);
                                Y_VERIFY(it != TableInfo.SplitKeys.end());

                                if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
                                    logl << "Table " << Table << " removing shard key " << it->first
                                        << ": " << TPrintableTypedCells(it->second.GetCells(), TableInfo.RowScheme->Keys->BasicTypes());
                                }

                                changes.StateChanges[shard->LeftKey] = { };
                                TableInfo.SplitKeys.erase(it);
                            }

                            CancelTask(shard->Task);
                            delete shard;

                            stateChanged = true;
                        }
                    }
                }

                if (parts) {
                    // Add back all the slices we have taken from merges
                    AddParts(std::move(parts));
                }
            }

            if (stateChanged) {
                SerializeStateInfo(&changes.StateChanges[0]);
            }
        }

        CheckCompactions();

        return changes;
    }

    TCompactionState TShardedCompactionStrategy::SnapshotState() {
        TCompactionState state;

        SerializeStateInfo(&state.StateSnapshot[0]);
        for (auto& kv : TableInfo.SplitKeys) {
            state.StateSnapshot[kv.first] = kv.second.GetBuffer();
        }

        return state;
    }

    bool TShardedCompactionStrategy::AllowForcedCompaction() {
        return !MemCompactionForced && !ForcedCompactionPending && ForcedCompactionTask.State == EState::Free;
    }

    void TShardedCompactionStrategy::OutputHtml(IOutputStream &out) {
        HTML(out) {
            if (ForcedCompactionPending || ForcedCompactionTask.State != EState::Free) {
                DIV_CLASS("row") {
                    out << "Forced compaction";
                    if (ForcedCompactionPending) {
                        out << " waiting";
                    }
                    out << ", " << ForcedCompactionTask.State;
                    if (ForcedCompactionTask.State != EState::Free) {
                        out << ", Task #" << ForcedCompactionTask.TaskId;
                        out << " (priority " << ForcedCompactionTask.Priority << ")";
                        out << " submitted " << ForcedCompactionTask.SubmissionTimestamp.ToStringLocal();
                    }
                }
            }

            for (auto& shard : Shards) {
                DIV_CLASS("row") {
                    out << "Shard " << shard.Id << ' ';
                    out << TPrintableShardRange(shard, TableInfo);
                    out << ", " << shard.Parts.size() << " parts";
                    if (shard.Levels) {
                        out << ", " << shard.Levels->size() << " levels";
                    } else if (shard.SplitBlocks) {
                        out << ", " << shard.SplitBlocks << " slice splits";
                    }
                    out << ", " << shard.Stats.Size << " bytes";
                    out << ", " << shard.Stats.Rows << " rows";
                    out << ", " << shard.Task.State;
                    if (shard.Task.State != EState::Free) {
                        out << ", Task #" << shard.Task.TaskId;
                        out << " (priority " << shard.Task.Priority << ")";
                        out << " submitted " << shard.Task.SubmissionTimestamp.ToStringLocal();
                    }
                }
            }

            PRE() {
                for (const auto& item : Nursery) {
                    const auto* part = item.PartView.Part.Get();
                    const auto& label = part->Label;
                    out << "Genstep: " << label.Generation() << ":" << label.Step();
                    out << " epoch " << part->Epoch;
                    out << ", Backing size: " << part->BackingSize();
                    out << ", Base: " << label;
                    out << ", nursery";
                    out << Endl;
                }

                TVector<const TPart*> parts(Reserve(AllParts.size()));
                for (auto& kv : AllParts) {
                    parts.push_back(kv.second.Part.Get());
                }
                std::sort(parts.begin(), parts.end(), [](const TPart* a, const TPart* b) -> bool {
                    if (a->Epoch != b->Epoch) {
                        return a->Epoch > b->Epoch;
                    }
                    return a->Label > b->Label;
                });
                for (const TPart* part : parts) {
                    auto& label = part->Label;
                    out << "Genstep: " << label.Generation() << ":" << label.Step();
                    out << " epoch " << part->Epoch;
                    out << ", Backing size: " << part->BackingSize();
                    out << ", Base: " << label;
                    auto& info = AllParts.at(label);
                    out << ", " << info.Slices->size() << " slices";
                    if (info.Shards) {
                        out << ", " << info.Shards.size() << " shards";
                    }
                    if (info.SplitBlocks) {
                        out << ", " << info.SplitBlocks << " slice splits";
                    }
                    out << Endl;
                }
            }
        }
    }

    void TShardedCompactionStrategy::OnSliceSplitResult(TSliceSplitResult result) {
        SliceSplitResults.emplace_back(std::move(result));
        RequestChanges();
    }

    void TShardedCompactionStrategy::RequestChanges() noexcept {
        if (!RequestChangesPending) {
            RequestChangesPending = true;
            Backend->RequestChanges(Table);
        }
    }

    void TShardedCompactionStrategy::CheckCompactions() noexcept {
        CheckForcedCompaction();

        for (auto& shard : Shards) {
            CheckShardCompaction(&shard);
        }
    }

    void TShardedCompactionStrategy::AddParts(TVector<TPartView> parts) {
        // Types and default values for current table keys
        const auto& keyDefaults = *TableInfo.RowScheme->Keys;

        // A part/slice combination we scheduled to add
        struct TItem {
            TIntrusiveConstPtr<TPart> Part;
            TSlice Slice;

            TItem(TIntrusiveConstPtr<TPart> part, TSlice slice)
                : Part(std::move(part))
                , Slice(std::move(slice))
            { }
        };

        // Sorts items by their first key
        auto cmpByFirstKey = [&keyDefaults](const TItem& a, const TItem& b) noexcept -> bool {
            auto left = a.Slice.FirstKey.GetCells();
            auto right = b.Slice.FirstKey.GetCells();
            if (int cmp = ComparePartKeys(left, right, keyDefaults)) {
                return cmp < 0;
            }
            return a.Slice.FirstInclusive && !b.Slice.FirstInclusive;
        };

        // Returns true if the item is completely to the left of boundary
        auto cmpFullyInside = [&keyDefaults](const TItem& a, const TSerializedCellVec& boundary) noexcept -> bool {
            auto left = a.Slice.LastKey.GetCells();
            if (Y_UNLIKELY(!left)) {
                return false; // +inf
            }
            if (int cmp = ComparePartKeys(left, boundary.GetCells(), keyDefaults)) {
                return cmp < 0;
            }
            return !a.Slice.LastInclusive;
        };

        // Returns true if the item is completely to the right of boundary
        auto cmpFullyOutside = [&keyDefaults](const TItem& a, const TSerializedCellVec& boundary) noexcept -> bool {
            auto right = a.Slice.FirstKey.GetCells();
            if (Y_UNLIKELY(!right)) {
                return false; // -inf
            }
            return ComparePartKeys(boundary.GetCells(), right, keyDefaults) <= 0;
        };

        // A part/slice combination that crosses at least one shard boundary
        struct TSplitItem {
            TIntrusiveConstPtr<TPart> Part;
            TSlice Slice;
            TVector<TTableShard*> Shards;

            TSplitItem(TIntrusiveConstPtr<TPart> part, TSlice slice, TTableShard* firstShard)
                : Part(std::move(part))
                , Slice(std::move(slice))
            {
                Shards.push_back(firstShard);
            }
        };

        // Heap order of split items by their last key
        auto cmpHeapByLastKey = [&keyDefaults](const TSplitItem& a, const TSplitItem& b) noexcept -> bool {
            auto left = b.Slice.LastKey.GetCells();
            auto right = a.Slice.LastKey.GetCells();
            if (int cmp = ComparePartKeys(left, right, keyDefaults)) {
                return cmp < 0;
            }
            return !b.Slice.LastInclusive && a.Slice.LastInclusive;
        };

        // Returns true if item is completely to the left of boundary
        auto cmpSplitFullyInside = [&keyDefaults](const TSplitItem& a, const TSerializedCellVec& boundary) noexcept -> bool {
            auto left = a.Slice.LastKey.GetCells();
            if (Y_UNLIKELY(!left)) {
                return false; // +inf
            }
            if (int cmp = ComparePartKeys(left, boundary.GetCells(), keyDefaults)) {
                return cmp < 0;
            }
            return !a.Slice.LastInclusive;
        };

        struct TPendingState {
            TTableShard* const Shard;
            TTablePart* const Info;
            TPartDataSizeHelper SizeHelper;
            const bool IsGarbage;

            TPendingState(TTableShard* shard, TTablePart* info, bool isGarbage) noexcept
                : Shard(shard)
                , Info(info)
                , SizeHelper(info->Part.Get())
                , IsGarbage(isGarbage)
            {
            }

            void AddSlice(TSlice slice) noexcept {
                TTablePart::TSliceId sliceId(slice);
                auto r = Info->Slices.emplace(
                        std::piecewise_construct,
                        std::forward_as_tuple(sliceId),
                        std::forward_as_tuple());

                Y_VERIFY(r.second, "Duplicate slices for rows [%" PRIu64 ",%" PRIu64 ")",
                        sliceId.Begin, sliceId.End);

                auto& item = r.first->second;
                item.Slice = std::move(slice);
                item.Stats.Init(Info->Part.Get(), item.Slice, SizeHelper);

                Shard->Stats += item.Stats;

                if (Shard->Levels) {
                    if (Info->Part->Epoch >= Shard->Levels->GetMaxEpoch()) {
                        Shard->RegisterItem(*Info, item, IsGarbage);
                    } else {
                        // Adding new slices is no longer trivial
                        Shard->Levels.Reset();
                    }
                }
            }
        };

        size_t slicesCount = 0;
        for (auto& partView : parts) {
            Y_VERIFY(!partView.Slices->empty(), "Attempt to add part without slices");

            slicesCount += partView.Slices->size();

            auto* allInfo = AllParts.FindPtr(partView->Label);
            Y_VERIFY(allInfo, "Added part %s is not globally registered", partView->Label.ToString().c_str());
        }

        // We may need to split slices over multiple shards
        if (Shards.Size() > 1) {

            TVector<TItem> queue(Reserve(slicesCount));
            for (auto& partView : parts) {
                for (const auto& slice : *partView.Slices) {
                    queue.emplace_back(partView.Part, slice);
                }
            }
            std::sort(queue.begin(), queue.end(), cmpByFirstKey);

            TTableShard* shard = Shards.Front();
            TSerializedCellVec* shardRightKey = TableInfo.SplitKeys.FindPtr(shard->RightKey);
            Y_VERIFY(shardRightKey);

            TTableShard* lastShard = Shards.Back();

            TVector<TItem*> pending;
            auto flushPending = [&]() noexcept -> void {
                std::sort(pending.begin(), pending.end(), [](const TItem* a, const TItem* b) noexcept -> bool {
                    if (a->Part->Epoch != b->Part->Epoch) {
                        // Prefer older epochs first
                        return a->Part->Epoch < b->Part->Epoch;
                    }
                    if (a->Part.Get() != b->Part.Get()) {
                        // Prefer same-epoch parts in their natural order
                        return a->Part->Label < b->Part->Label;
                    }
                    // Prefer same-part slices in their natural order
                    return a->Slice.BeginRowId() < b->Slice.BeginRowId();
                });

                std::optional<TPendingState> state;

                for (auto* pItem : pending) {
                    if (!state || state->Info->Part.Get() != pItem->Part.Get()) {
                        state.reset();
                        auto label = pItem->Part->Label;
                        state.emplace(shard, EnsurePart(shard, std::move(pItem->Part)), TableInfo.GarbageParts.contains(label));
                    }
                    state->AddSlice(std::move(pItem->Slice));
                }

                state.reset();

                pending.clear();
            };

            TVector<TSplitItem> splitQueue;
            auto flushShard = [&]() noexcept -> void {
                Y_VERIFY(shard != lastShard);

                // Move to the next shard
                shard = shard->Next()->Node();

                if (shard != lastShard) {
                    shardRightKey = TableInfo.SplitKeys.FindPtr(shard->RightKey);
                    Y_VERIFY(shardRightKey);
                } else {
                    shardRightKey = nullptr;
                }

                if (splitQueue) {
                    // Everything left in split queue crosses into the next shard
                    for (auto& splitItem : splitQueue) {
                        splitItem.Shards.push_back(shard);
                    }
                    shard->SplitBlocks += splitQueue.size();

                    // Don't even try building levels for this shard
                    shard->Levels.Reset();
                    shard->PerLevelStats.clear();
                }

                while (!splitQueue.empty() && (shard == lastShard || cmpSplitFullyInside(splitQueue.front(), *shardRightKey))) {
                    std::pop_heap(splitQueue.begin(), splitQueue.end(), cmpHeapByLastKey);
                    auto& splitItem = splitQueue.back();

                    AllParts.at(splitItem.Part->Label).SplitBlocks++;

                    auto op = MakeHolder<TSliceSplitOp>(
                            this,
                            &TableInfo,
                            std::move(splitItem.Shards),
                            std::move(splitItem.Part),
                            std::move(splitItem.Slice));

                    auto* opWeak = op.Get();
                    PendingSliceSplits.PushBack(opWeak);

                    if (auto readId = Backend->BeginRead(std::move(op))) {
                        // The read operation is currently pending
                        // Remember its id so we may cancel it later
                        opWeak->SetReadId(readId);
                    }

                    splitQueue.pop_back();
                }
            };

            auto pos = queue.begin();
            while (pos != queue.end()) {
                auto& item = *pos;

                if (shard == lastShard || cmpFullyInside(item, *shardRightKey)) {
                    pending.emplace_back(&item);
                    ++pos;
                    continue;
                }

                if (cmpFullyOutside(item, *shardRightKey)) {
                    flushPending();
                    flushShard();
                    continue;
                }

                // This is the first shard where slice crosses into the next shard
                splitQueue.emplace_back(std::move(item.Part), std::move(item.Slice), shard);
                std::push_heap(splitQueue.begin(), splitQueue.end(), cmpHeapByLastKey);
                shard->SplitBlocks++;
                ++pos;

                // Don't even try building levels for this shard
                shard->Levels.Reset();
                shard->PerLevelStats.clear();
            }

            // Flush any pending slices for the last shard
            flushPending();

            while (splitQueue && shard != lastShard) {
                flushShard();
            }

            Y_VERIFY(pending.empty());
            Y_VERIFY(pos == queue.end());
            Y_VERIFY(splitQueue.empty());

        } else {

            // Avoid complex logic when there's a single shard
            auto* shard = Shards.Front();

            // Make sure new parts are sorted by epoch
            std::sort(parts.begin(), parts.end(), [](const TPartView& a, const TPartView& b) -> bool {
                if (a->Epoch != b->Epoch) {
                    return a->Epoch < b->Epoch;
                }
                return a->Label < b->Label;
            });

            for (auto& partView : parts) {
                auto label = partView->Label;

                TPendingState state(shard, EnsurePart(shard, std::move(partView.Part)), TableInfo.GarbageParts.contains(label));

                for (const auto& slice : *partView.Slices) {
                    state.AddSlice(slice);
                }
            }
        }
    }

    TShardedCompactionStrategy::TGlobalPart* TShardedCompactionStrategy::EnsureGlobalPart(
            const TIntrusiveConstPtr<TPart>& part,
            const TIntrusiveConstPtr<TSlices>& slices) noexcept
    {
        auto* allInfo = AllParts.FindPtr(part->Label);
        if (!allInfo) {
            TPartDataSizeHelper helper(part.Get());
            allInfo = &AllParts[part->Label];
            allInfo->Part = part;
            allInfo->TotalSize = helper.CalcSize(0, Max<TRowId>());
            allInfo->GarbageSize = 0;
            AllBackingSize += part->BackingSize();
        }
        allInfo->Slices = TSlices::Merge(allInfo->Slices, slices);
        UpdateGarbageStats(allInfo);
        return allInfo;
    }

    void TShardedCompactionStrategy::UpdateGarbageStats(TShardedCompactionStrategy::TGlobalPart* allInfo) noexcept {
        const auto& label = allInfo->Part->Label;

        TRowId last = 0;
        allInfo->GarbageSize = 0;
        TPartDataSizeHelper helper(allInfo->Part.Get());
        for (const auto& slice : *allInfo->Slices) {
            TRowId next = slice.BeginRowId();
            if (last < next) {
                allInfo->GarbageSize += helper.CalcSize(last, next);
            }
            last = slice.EndRowId();
        }
        allInfo->GarbageSize += helper.CalcSize(last, Max<TRowId>());

        ui32 garbagePercent = allInfo->GarbageSize * 100 / allInfo->TotalSize;
        if (garbagePercent >= Policy->ShardPolicy.GetMaxGarbagePercentToReuse()) {
            if (TableInfo.GarbageParts.emplace(label).second) {
                // This part just turned into garbage, update all shards
                for (auto* shard : allInfo->Shards) {
                    if (!shard->Levels) {
                        continue;
                    }
                    if (auto* partInfo = shard->Parts.FindPtr(label)) {
                        for (auto& kv : partInfo->Slices) {
                            size_t index = kv.second.Level->Index;
                            if (index < shard->PerLevelGarbage.size()) {
                                shard->PerLevelGarbage[index] += kv.second.Stats.Size;
                            }
                        }
                    }
                }
            }
        } else {
            if (TableInfo.GarbageParts.erase(label)) {
                // This part just stopped being garbage, update all shards
                for (auto* shard : allInfo->Shards) {
                    if (!shard->Levels) {
                        continue;
                    }
                    if (auto* partInfo = shard->Parts.FindPtr(label)) {
                        for (auto& kv : partInfo->Slices) {
                            size_t index = kv.second.Level->Index;
                            if (index < shard->PerLevelGarbage.size()) {
                                shard->PerLevelGarbage[index] -= kv.second.Stats.Size;
                            }
                        }
                    }
                }
            }
        }
    }

    TTablePart* TShardedCompactionStrategy::EnsurePart(TTableShard* shard, TIntrusiveConstPtr<TPart> part) noexcept {
        auto* info = shard->Parts.FindPtr(part->Label);
        if (!info) {
            AllParts.at(part->Label).Shards.emplace_back(shard);

            info = &shard->Parts[part->Label];
            info->Part = std::move(part);
        } else {
            Y_VERIFY(info->Part.Get() == part.Get(),
                     "Multiple parts with the same label %s",
                     part->Label.ToString().c_str());
        }
        return info;
    }

    void TShardedCompactionStrategy::RebuildLevels(TTableShard* shard) noexcept {
        Y_VERIFY(!shard->SplitBlocks, "Unexpected RebuildLevels while blocked by split");

        shard->Levels.Reset(new TLevels(TableInfo.RowScheme->Keys));
        shard->PerLevelStats.clear();

        TVector<TTablePart*> parts(Reserve(shard->Parts.size()));
        for (auto& kv : shard->Parts) {
            parts.emplace_back(&kv.second);
        }
        std::sort(parts.begin(), parts.end(), [](TTablePart* a, TTablePart* b) noexcept -> bool {
            if (a->Part->Epoch != b->Part->Epoch) {
                return a->Part->Epoch < b->Part->Epoch;
            }
            return a->Part->Label < b->Part->Label;
        });

        for (TTablePart* info : parts) {
            Y_VERIFY(info->Part->Epoch >= shard->Levels->GetMaxEpoch());

            bool isGarbage = TableInfo.GarbageParts.contains(info->Part->Label);

            for (auto& kv : info->Slices) {
                auto& item = kv.second;
                shard->RegisterItem(*info, item, isGarbage);
            }
        }
    }

    void TShardedCompactionStrategy::CancelTask(TCompactionTask& task) noexcept {
        switch (task.State) {
            case EState::Free: {
                // nothing
                break;
            }
            case EState::Pending: {
                Broker->CancelTask(task.TaskId);
                break;
            }
            case EState::Compacting: {
                Backend->CancelCompaction(task.CompactionId);
                break;
            }
        }
        task.State = EState::Free;
        task.TaskId = 0;
        task.CompactionId = 0;
    }

    bool TShardedCompactionStrategy::CheckShardCompaction(TTableShard* shard, bool schedule) noexcept {
        if (shard->SplitBlocks || shard->Task.State == EState::Compacting) {
            // cannot compact until we have the whole picture
            return false;
        }

        bool garbage = false;
        bool critical = false;
        shard->FailingLevel = Max<size_t>();

        if (!shard->Levels) {
            // build the overall picture about this shard
            RebuildLevels(shard);
        }

        const auto& policy = Policy->ShardPolicy;

        TVector<TStats> cumulative;
        cumulative.resize(shard->Levels->size());

        size_t idx = shard->Levels->size();
        for (auto level = shard->Levels->begin(); level != shard->Levels->end(); ++level) {
            bool failed = false;
            size_t nextIdx = idx--;
            auto& current = cumulative[idx];
            current += shard->PerLevelStats[idx];

            if (shard->PerLevelGarbage[idx] > 0) {
                garbage = true;
                failed = true;
            }

            if (nextIdx < cumulative.size()) {
                auto& next = cumulative[nextIdx];
                if (!failed &&
                    current.Size > 0 &&
                    policy.GetNewDataPercentToCompact() > 0 &&
                    next.Size * 100 > current.Size * policy.GetNewDataPercentToCompact())
                {
                    failed = true;
                }
                if (!failed &&
                    current.Rows > 0 &&
                    policy.GetNewRowsPercentToCompact() > 0 &&
                    next.Rows * 100 > current.Rows * policy.GetNewRowsPercentToCompact())
                {
                    failed = true;
                }
                current += next;
            }
            if (!failed &&
                !Policy->KeepEraseMarkers &&
                Policy->DroppedRowsPercentToCompact > 0 &&
                current.DroppedPercent() >= Policy->DroppedRowsPercentToCompact)
            {
                failed = true;
            }
            if (policy.GetMaxSlicesPerLevel() > 0 &&
                level->size() > policy.GetMaxSlicesPerLevel())
            {
                // We want to compact this even if it's just a single level
                critical = true;
                failed = true;
            }
            if (failed) {
                shard->FailingLevel = idx;
            }
        }

        if (critical && shard->FailingLevel > 0) {
            // Take an extra level below so we eventually escape from the
            // critical (e.g. by degenerating into full compaction of all
            // existing levels)
            shard->FailingLevel--;
        }

        if (shard->Levels->size() > policy.GetMaxTotalLevels()) {
            // We want MaxTotalLevels, compact everything above
            size_t level = policy.GetMaxTotalLevels() - 1;
            shard->FailingLevel = Min(shard->FailingLevel, level);
        }

        if (!critical && !garbage &&
            shard->FailingLevel != Max<size_t>() &&
            shard->Levels->size() - shard->FailingLevel < policy.GetMinLevelsToCompact())
        {
            // We don't have enough levels to compact
            shard->FailingLevel = Max<size_t>();
        }

        if (shard->FailingLevel == Max<size_t>()) {
            // We don't want to compact this shard at this time
            return false;
        }

        if (ForcedCompactionTask.State == EState::Compacting) {
            // No shard compactions while forced compaction is in progress
            return false;
        }

        if (schedule) {
            TString type = policy.GetResourceBrokerTask();

            // TODO: figure out how to assign priorities
            ui32 newPriority = policy.GetTaskPriorityBase();

            // For every N levels over min levels we increase priority
            if (policy.GetTaskPriorityLevelsBoost() > 0) {
                ui32 levelsBoost = shard->Levels->size() - shard->FailingLevel;
                levelsBoost -= Min(levelsBoost, policy.GetMinLevelsToCompact());
                levelsBoost /= policy.GetTaskPriorityLevelsBoost();
                newPriority -= Min(newPriority, levelsBoost);
            }

            // For every N bytes of input data we decrease priority
            if (policy.GetTaskPrioritySizePenalty() > 0) {
                ui32 sizePenalty = cumulative[shard->FailingLevel].Size;
                sizePenalty /= policy.GetTaskPrioritySizePenalty();
                newPriority += Min(sizePenalty, ui32(1000));
            }

            switch (shard->Task.State) {
                case EState::Free: {
                    // submit a new task
                    TString name;
                    {
                        TStringOutput out(name);
                        out << "shard-" << shard->Id;
                        out << "-table-" << Table;
                        out << "-" << TaskNameSuffix;
                    }
                    shard->Task.SubmissionTimestamp = Time->Now();
                    shard->Task.Priority = newPriority;
                    shard->Task.TaskId = Broker->SubmitTask(
                            std::move(name),
                            TResourceParams(std::move(type))
                                    .WithCPU(1)
                                    .WithPriority(shard->Task.Priority),
                            [this, shard](TTaskId taskId) {
                                BeginShardCompaction(shard, taskId);
                            });
                    shard->Task.State = EState::Pending;
                    break;
                }
                case EState::Pending: {
                    // update task priority
                    if (newPriority < shard->Task.Priority &&
                        (shard->Task.Priority - newPriority) >= shard->Task.Priority / PRIORITY_UPDATE_FACTOR) {
                        shard->Task.Priority = newPriority;
                        Broker->UpdateTask(
                                shard->Task.TaskId,
                                TResourceParams(std::move(type))
                                        .WithCPU(1)
                                        .WithPriority(shard->Task.Priority));
                    }
                    break;
                }
                default: {
                    Y_FAIL("Unexpected shard state");
                }
            }
        }

        return true;
    }

    bool TShardedCompactionStrategy::CheckForcedCompaction(bool schedule) noexcept {
        if (!ForcedCompactionPending ||
            ForcedCompactionTask.State == EState::Compacting ||
            PendingSliceSplits ||
            SliceSplitResults)
        {
            // Forced compaction cannot run at this time
            return false;
        }

        if (MemCompactionForced) {
            // Don't start big compaction during another forced mem compaction
            return false;
        }

        if (schedule) {
            const auto& policy = Policy->ShardPolicy;

            TString type = policy.GetResourceBrokerTask();

            // TODO: figure out how to assign priorities
            ui32 newPriority = policy.GetTaskPriorityBase();

            switch (ForcedCompactionTask.State) {
                case EState::Free: {
                    // submit a new task
                    TString name;
                    {
                        TStringOutput out(name);
                        out << "forced-table-" << Table;
                        out << "-" << TaskNameSuffix;
                    }
                    ForcedCompactionTask.SubmissionTimestamp = Time->Now();
                    ForcedCompactionTask.Priority = newPriority;
                    ForcedCompactionTask.TaskId = Broker->SubmitTask(
                            std::move(name),
                            TResourceParams(std::move(type))
                                    .WithCPU(1)
                                    .WithPriority(ForcedCompactionTask.Priority),
                            [this](TTaskId taskId) {
                                BeginForcedCompaction(taskId);
                            });
                    ForcedCompactionTask.State = EState::Pending;
                    break;
                }
                case EState::Pending: {
                    // update task priority
                    if (newPriority < ForcedCompactionTask.Priority &&
                        (ForcedCompactionTask.Priority - newPriority) >= ForcedCompactionTask.Priority / PRIORITY_UPDATE_FACTOR) {
                        ForcedCompactionTask.Priority = newPriority;
                        Broker->UpdateTask(
                                ForcedCompactionTask.TaskId,
                                TResourceParams(std::move(type))
                                        .WithCPU(1)
                                        .WithPriority(ForcedCompactionTask.Priority));
                    }
                    break;
                }
                default: {
                    Y_FAIL("Unexpected forced compaction state");
                }
            }
        }

        return true;
    }

    void TShardedCompactionStrategy::BeginShardCompaction(TTableShard* shard, TTaskId taskId) noexcept {
        Y_VERIFY(shard->Task.State == EState::Pending);
        Y_VERIFY(shard->Task.TaskId == taskId);
        if (!CheckShardCompaction(shard, false)) {
            // We no longer want to compact this shard for some reason
            Broker->FinishTask(taskId, EResourceStatus::Cancelled);
            shard->Task.State = EState::Free;
            shard->Task.TaskId = 0;
            return;
        }

        auto params = MakeHolder<TShardedCompactionParams>();
        params->Table = Table;
        params->TaskId = shard->Task.TaskId;
        params->InputShard = shard;

        struct TInput {
            TIntrusiveConstPtr<TPart> Part;
            TVector<TSlice> Slices;
            TVector<TSlice> Reused;
            TIntrusiveConstPtr<TSlices> ReusedRef;
        };

        TEpoch maxEpoch = TEpoch::Min();
        TVector<const TBounds*> edges;
        THashMap<const TPart*, TInput> inputs;

        // First gather all inputs
        for (auto& level : *shard->Levels) {
            if (level.Index < shard->FailingLevel) {
                continue;
            }
            for (auto& item : level) {
                auto& input = inputs[item.Part.Get()];
                if (!input.Part) {
                    maxEpoch = Max(maxEpoch, item.Part->Epoch);
                    input.Part = item.Part;
                }
                input.Slices.emplace_back(item.Slice);
            }
        }

        // Try to find slices that may be reused
        TPageReuseBuilder reuseBuilder(*TableInfo.RowScheme->Keys);
        for (auto& kv : inputs) {
            auto& input = kv.second;
            bool reusable = true;
            // Part is not reusable if it has any drops
            if (reusable && input.Part->Stat.Drops > 0) {
                reusable = false;
            }
            // Avoid reusing parts that are already considered garbage
            if (reusable && TableInfo.GarbageParts.contains(input.Part->Label)) {
                reusable = false;
            }
            for (const auto& slice : input.Slices) {
                reuseBuilder.AddSlice(kv.first, slice, reusable);
            }
        }
        auto reuseResults = reuseBuilder.Build();

        // Reuse all we can if not too many output slices are expected
        if (reuseResults.ExpectedSlices < Policy->ShardPolicy.GetMaxSlicesPerLevel()) {
            for (auto& reusable : reuseResults.Reusable) {
                TPartDataSizeHelper helper(reusable.Part);
                ui64 dataSize = helper.CalcSize(reusable.Slice.BeginRowId(), reusable.Slice.EndRowId());
                if (dataSize < Policy->ShardPolicy.GetMinSliceSizeToReuse()) {
                    // Avoid reusing slices that are too small, better to recompact
                    continue;
                }
                auto* input = inputs.FindPtr(reusable.Part);
                Y_VERIFY(input, "Cannot find reusable part %s in our inputs",
                        reusable.Part->Label.ToString().c_str());
                input->Reused.emplace_back(std::move(reusable.Slice));
            }
        }

        // Now turn those inputs into TPartView structures
        for (auto& kv : inputs) {
            auto& input = kv.second;
            std::sort(input.Slices.begin(), input.Slices.end(), [](const TSlice& a, const TSlice& b) noexcept -> bool {
                return a.BeginRowId() < b.BeginRowId();
            });
            TIntrusiveConstPtr<TSlices> original = new TSlices(std::move(input.Slices));
            TIntrusiveConstPtr<TSlices> reused = new TSlices(std::move(input.Reused));
            TIntrusiveConstPtr<TSlices> compacted = TSlices::Subtract(original, reused);

            // Everything we're compacting will become garbage soon
            if (reused && !reused->empty() && compacted && !compacted->empty()) {
                auto* allInfo = AllParts.FindPtr(input.Part->Label);
                Y_VERIFY(allInfo, "Reused part %s is not registered", input.Part->Label.ToString().c_str());

                TIntrusiveConstPtr<TSlices> afterCompaction = TSlices::Subtract(allInfo->Slices, compacted);

                // Calculate how much garbage this part would have after compaction
                TRowId last = 0;
                ui64 newGarbage = 0;
                TPartDataSizeHelper helper(input.Part.Get());
                for (const auto& slice : *afterCompaction) {
                    TRowId next = slice.BeginRowId();
                    if (last < next) {
                        newGarbage += helper.CalcSize(last, next);
                    }
                    last = slice.EndRowId();
                }
                newGarbage += helper.CalcSize(last, Max<TRowId>());
                ui64 newGarbagePercent = newGarbage * 100 / allInfo->TotalSize;

                if (newGarbagePercent >= Policy->ShardPolicy.GetMaxGarbagePercentToReuse()) {
                    compacted = original;
                    reused = nullptr;
                }
            }

            // Make sure compacted data would not interfere with reused slices
            if (reused && !reused->empty()) {
                input.ReusedRef = reused; // keep pointers alive
                for (const auto& slice : *reused) {
                    edges.emplace_back(&slice);
                }
            }

            // Create necessary records if there is anything to compact
            if (compacted && !compacted->empty()) {
                params->Parts.emplace_back(MakePartView(input.Part, std::move(compacted)));
                params->Original.emplace_back(MakePartView(input.Part, std::move(original)));
                if (reused && !reused->empty()) {
                    params->Reused.emplace_back(MakePartView(input.Part, std::move(reused)));
                }
            }
        }

        if (!Policy->KeepEraseMarkers) {
            params->IsFinal = shard->FailingLevel == 0;
        } else {
            params->IsFinal = false;
        }

        if (!params->IsFinal) {
            TVector<const TBounds*> allBounds;
            for (auto& level : *shard->Levels) {
                if (level.Index < shard->FailingLevel) {
                    for (auto& item : level) {
                        if (!Policy->KeepEraseMarkers) {
                            allBounds.push_back(&item.Slice);
                        }
                        if (item.Part->Epoch >= maxEpoch) {
                            // Prevent newer slices from bubbling up just because
                            // we have compacted older disjoint slices that happened
                            // to be higher in the hierarchy.
                            edges.push_back(&item.Slice);
                        }
                    }
                }
            }

            if (!Policy->KeepEraseMarkers) {
                Y_VERIFY(allBounds, "Unexpected lack of underlay slices");
                params->UnderlayMask = TUnderlayMask::Build(TableInfo.RowScheme, allBounds);
            }
        }

        if (edges) {
            params->SplitKeys = MakeHolder<TSplitKeys>(TableInfo.RowScheme, std::move(edges));
        }

        shard->Task.CompactionId = Backend->BeginCompaction(std::move(params));
        shard->Task.State = EState::Compacting;
    }

    void TShardedCompactionStrategy::BeginForcedCompaction(TTaskId taskId) noexcept {
        Y_VERIFY(ForcedCompactionTask.State == EState::Pending);
        Y_VERIFY(ForcedCompactionTask.TaskId == taskId);
        Y_VERIFY(ForcedCompactionPending);

        if (!CheckForcedCompaction(false)) {
            Broker->FinishTask(taskId, EResourceStatus::Cancelled);
            ForcedCompactionTask.State = EState::Free;
            ForcedCompactionTask.TaskId = 0;
            return;
        }

        // Avoid active splits changing state under us
        Y_VERIFY(!PendingSliceSplits);
        Y_VERIFY(!SliceSplitResults);

        // Cancel all running shard compactions
        for (auto& shard : Shards) {
            CancelTask(shard.Task);
        }

        auto params = MakeHolder<TShardedCompactionParams>();
        params->Table = Table;
        params->TaskId = taskId;
        params->IsFinal = !Policy->KeepEraseMarkers;

        // Take all non-nursery parts into a new giant compaction
        for (auto& kv : AllParts) {
            params->Parts.push_back(MakePartView(kv.second.Part, kv.second.Slices));
            params->Original.push_back(params->Parts.back());
        }

        if (TableInfo.SplitKeys) {
            TVector<TSerializedCellVec> splitKeys;
            auto* last = Shards.Back();
            auto* shard = Shards.Front();
            while (shard != last) {
                splitKeys.push_back(TableInfo.SplitKeys.at(shard->RightKey));
                shard = shard->Next()->Node();
            }
            Y_VERIFY(splitKeys, "Unexpected lack of split keys");
            params->SplitKeys = MakeHolder<TSplitKeys>(TableInfo.RowScheme, std::move(splitKeys));
        }

        ForcedCompactionTask.CompactionId = Backend->BeginCompaction(std::move(params));
        ForcedCompactionTask.State = EState::Compacting;
        ForcedCompactionPending = false;
    }

    void TShardedCompactionStrategy::SerializeStateInfo(TString* out) const noexcept {
        TShardedStrategyStateInfo header;
        header.SetLastSplitKey(TableInfo.LastSplitKey);
        header.SetLastShardId(TableInfo.LastShardId);
        for (auto& kv : TableInfo.SplitKeys) {
            header.AddSplitKeys(kv.first);
        }
        for (auto& shard : Shards) {
            header.AddShards(shard.Id);
        }
        Y_PROTOBUF_SUPPRESS_NODISCARD header.SerializeToString(out);
    }

}
}
}
