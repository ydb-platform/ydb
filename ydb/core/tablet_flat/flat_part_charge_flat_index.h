#pragma once

#include "flat_table_part.h"
#include "flat_part_iface.h"
#include "flat_part_index_iter_flat_index.h"
#include "flat_part_charge_iface.h"
#include "flat_page_data.h"

#include <util/generic/bitmap.h>

namespace NKikimr {
namespace NTable {

    class TChargeFlatIndex : public ICharge {
    public:
        using TIter = NPage::TFlatIndex::TIter;
        using TDataPage = NPage::TDataPage;
        using TGroupId = NPage::TGroupId;

        TChargeFlatIndex(IPages *env, const TPart &part, TTagsRef tags, bool includeHistory)
            : Env(env)
            , Part(&part)
            , Scheme(*Part->Scheme)
            , Index(Part, Env, TGroupId())
        {
            if (includeHistory && Part->HistoricGroupsCount) {
                HistoryIndex.emplace(Part, Env, TGroupId(0, true));
            }

            TDynBitMap seen;
            for (TTag tag : tags) {
                if (const auto* col = Scheme.FindColumnByTag(tag)) {
                    if (col->Group != 0 && !seen.Get(col->Group)) {
                        NPage::TGroupId groupId(col->Group);
                        Groups.emplace_back(TPartGroupFlatIndexIter(Part, Env, groupId), groupId);
                        if (HistoryIndex) {
                            NPage::TGroupId historyGroupId(col->Group, true);
                            HistoryGroups.emplace_back(TPartGroupFlatIndexIter(Part, Env, historyGroupId), historyGroupId);
                        }
                        seen.Set(col->Group);
                    }
                }
            }
        }

        TResult Do(const TCells key1, const TCells key2, TRowId row1, TRowId row2, 
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override
        {
            auto index = Index.TryLoadRaw();
            if (!index) {
                return { false, false };
            }

            auto startRow = row1;
            auto endRow = row2;
            bool overshot = !key2; // +inf is always on the next slice

            // First page to precharge (contains row1)
            auto first = index->LookupRow(row1);
            if (Y_UNLIKELY(!first)) {
                return { true, true }; // already out of bounds, nothing to precharge
            }

            // First extra page to precharge (when key placement is uncertain)
            auto firstExt = first;

            // Last page to precharge (contains row2)
            auto last = index->LookupRow(row2, first);
            if (Y_UNLIKELY(last < first)) {
                last = first; // will not go past the first page
                endRow = Min(endRow, index->GetLastRowId(last));
            }

            TIter key1Page;
            if (key1) {
                // First page to precharge (may contain key >= key1)
                key1Page = index->LookupKey(key1, Scheme.Groups[0], ESeek::Lower, &keyDefaults);
                if (!key1Page || key1Page > last) {
                    return { true, true }; // first key is outside of bounds
                }
                if (first <= key1Page) {
                    first = key1Page; // use the maximum
                    firstExt = key1Page + 1; // first key >= key1 might be on the next page
                    startRow = Max(startRow, key1Page->GetRowId());
                    if (!firstExt || last < firstExt) {
                        firstExt = last; // never precharge past the last page
                        overshot = true; // may have to touch the next slice
                    }
                } else {
                    key1Page = {};
                }
            }

            TIter key2Page;
            if (key2) {
                // Last page to precharge (may contain key >= key2)
                // We actually use the next page since lookup is not exact
                key2Page = index->LookupKey(key2, Scheme.Groups[0], ESeek::Lower, &keyDefaults);
                auto key2PageExt = key2Page + 1;
                if (key2PageExt && key2PageExt <= last) {
                    last = Max(key2PageExt, firstExt);
                    endRow = Min(endRow, last->GetRowId()); // may load the first row of key2PageExt
                } else {
                    overshot = true; // may find first key > key2 on row > row2
                }
            }

            bool ready = DoPrecharge(key1, key2, key1Page, key2Page, first, last, 
                startRow, endRow, keyDefaults, itemsLimit, bytesLimit);

            return { ready, overshot };
        }

        TResult DoReverse(const TCells key1, const TCells key2, TRowId row1, TRowId row2, 
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override
        {
            auto index = Index.TryLoadRaw();
            if (!index) {
                return { false, false };
            }

            auto startRow = row1;
            auto endRow = row2;
            bool overshot = !key2; // +inf is always on the next slice

            // First page to precharge (contains row1)
            auto first = index->LookupRow(row1);
            if (Y_UNLIKELY(!first)) {
                // Looks like row1 is out of bounds, start from the last row
                startRow = Min(row1, index->GetEndRowId() - 1);
                first = --(*index)->End();
                if (Y_UNLIKELY(!first)) {
                    return { true, true }; // empty index?
                }
            }

            // First extra page to precharge (when key placement is uncertain)
            auto firstExt = first;

            // Last page to precharge (contains row2)
            auto last = index->LookupRow(row2, first);
            if (Y_UNLIKELY(last > first)) {
                last = first; // will not go past the first page
                endRow = Max(endRow, last->GetRowId());
            }

            TIter key1Page;
            if (key1) {
                // First page to precharge (may contain key <= key1)
                key1Page = index->LookupKeyReverse(key1, Scheme.Groups[0], ESeek::Lower, &keyDefaults);
                if (!key1Page || key1Page < last) {
                    return { true, true }; // first key is outside of bounds
                }
                if (first >= key1Page) {
                    first = key1Page; // use the minimum
                    firstExt = key1Page - 1; // first key <= key1 might be on the next page
                    startRow = Min(startRow, index->GetLastRowId(first));
                    if (key1Page.Off() == 0 || last > firstExt) {
                        firstExt = last; // never precharge past the last page
                        overshot = true; // may have to touch the next slice
                    }
                } else {
                    key1Page = {};
                }
            }

            TIter key2Page;
            if (key2) {
                // Last page to precharge (may contain key <= key2)
                // We actually use the next page since lookup is not exact
                key2Page = index->LookupKeyReverse(key2, Scheme.Groups[0], ESeek::Lower, &keyDefaults);
                auto key2PageExt = key2Page - 1;
                if (key2Page && key2Page.Off() != 0 && key2PageExt >= last) {
                    last = Min(key2PageExt, firstExt);
                    endRow = Max(endRow, index->GetLastRowId(last)); // may load the last row of key2PageExt
                } else {
                    overshot = true; // may find first key < key2 on row < row2
                }
            }

            bool ready = DoPrechargeReverse(key1, key2, key1Page, key2Page, first, last, 
                startRow, endRow, keyDefaults, itemsLimit, bytesLimit);

            return { ready, overshot };
        }

    private:
        /**
         * Precharges data from first to last page inclusive
         *
         * Precharges data only in [ @param startRowId, @param endRowId ] range.
         *
         * If keys provided, precharges only foolproof needed pages between them.
         * 
         * If items limit specified also touches [@param startRowId + itemsLimit] row.
         *
         * If @param key1Page specified, @param first should be the same.
         */
        bool DoPrecharge(const TCells key1, const TCells key2, const TIter key1Page, const TIter key2Page,
                const TIter first, const TIter last, TRowId startRowId, TRowId endRowId,
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept
        {
            bool ready = true;

            if (first) {
                Y_DEBUG_ABORT_UNLESS(first <= last);
                Y_DEBUG_ABORT_UNLESS(!key1Page || key1Page == first);

                ui64 items = 0;
                ui64 bytes = 0;

                std::optional<std::pair<TRowId, TRowId>> prechargedRowsRange;
                bool needExactBounds = Groups || HistoryIndex;

                for (auto current = first; 
                        current && current <= last && !LimitExceeded(items, itemsLimit) && !LimitExceeded(bytes, bytesLimit); 
                        current++) {
                    auto currentExt = current + 1;
                    auto currentFirstRowId = current->GetRowId();
                    auto currentLastRowId = currentExt ? (currentExt->GetRowId() - 1) : Max<TRowId>();

                    auto page = Env->TryGetPage(Part, current->GetPageId(), {});
                    if (bytesLimit) {
                        bytes += Part->GetPageSize(current->GetPageId(), {});
                    }
                    ready &= bool(page);

                    auto prechargeCurrentFirstRowId = Max(currentFirstRowId, startRowId);
                    auto prechargeCurrentLastRowId = Min(currentLastRowId, endRowId);
                    
                    if (key1Page && key1Page == current) {
                        if (needExactBounds && page) {
                            auto key1RowId = LookupRowId(key1, page, Scheme.Groups[0], ESeek::Lower, keyDefaults);
                            prechargeCurrentFirstRowId = Max(prechargeCurrentFirstRowId, key1RowId);
                        } else {
                            prechargeCurrentFirstRowId = Max<TRowId>(); // no precharge
                        }
                    }
                    if (itemsLimit && prechargeCurrentFirstRowId <= prechargeCurrentLastRowId) {
                        ui64 left = itemsLimit - items; // we count only foolproof taken rows, so here we may precharge some extra rows
                        if (prechargeCurrentLastRowId - prechargeCurrentFirstRowId > left) {
                            prechargeCurrentLastRowId = prechargeCurrentFirstRowId + left;
                        }
                    }
                    if (prechargeCurrentFirstRowId <= prechargeCurrentLastRowId) {
                        items += prechargeCurrentLastRowId - prechargeCurrentFirstRowId + 1;
                    }
                    if (key2Page && key2Page <= current) {
                        if (key2Page == current) {
                            if (needExactBounds && page) {
                                auto key2RowId = LookupRowId(key2, page, Scheme.Groups[0], ESeek::Upper, keyDefaults);
                                if (key2RowId) {
                                    prechargeCurrentLastRowId = Min(prechargeCurrentLastRowId, key2RowId - 1);
                                } else {
                                    prechargeCurrentFirstRowId = Max<TRowId>(); // no precharge
                                }
                            } else {
                                prechargeCurrentFirstRowId = Max<TRowId>(); // no precharge
                            }
                        } else {
                            prechargeCurrentFirstRowId = Max<TRowId>(); // no precharge
                        }
                    }
                    if (prechargeCurrentFirstRowId <= prechargeCurrentLastRowId) {
                        if (prechargedRowsRange) {
                            prechargedRowsRange->second = prechargeCurrentLastRowId;
                        } else {
                            prechargedRowsRange.emplace(prechargeCurrentFirstRowId, prechargeCurrentLastRowId);
                        }
                        if (Groups) {
                            for (auto& g : Groups) {
                                ready &= DoPrechargeGroup(g, prechargeCurrentFirstRowId, prechargeCurrentLastRowId, bytes);
                            }
                        }
                    }
                }

                if (prechargedRowsRange && HistoryIndex) {
                    ready &= DoPrechargeHistory(prechargedRowsRange->first, prechargedRowsRange->second);
                }
            }

            return ready;
        }

        /**
         * Precharges data from first to last page inclusive in reverse
         *
         * Precharges data only in [ @param endRowId, @param startRowId ] range.
         *
         * If keys provided, precharges only foolproof needed pages between them.
         * 
         * If items limit specified also touches [@param startRowId + itemsLimit] row.
         *
         * If @param key1Page specified, @param first should be the same.
         */
        bool DoPrechargeReverse(const TCells key1, const TCells key2, const TIter key1Page, const TIter key2Page,
                TIter first, TIter last, TRowId startRowId, TRowId endRowId,
                const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept
        {
            bool ready = true;

            if (first) {
                Y_DEBUG_ABORT_UNLESS(first >= last);
                Y_DEBUG_ABORT_UNLESS(!key1Page || key1Page == first);

                ui64 items = 0;
                ui64 bytes = 0;

                std::optional<std::pair<TRowId, TRowId>> prechargedRowsRange;
                bool needExactBounds = Groups || HistoryIndex;

                for (auto current = first;
                        current >= last && !LimitExceeded(items, itemsLimit) && !LimitExceeded(bytes, bytesLimit);
                        current--) {
                    auto currentExt = current + 1;
                    auto currentFirstRowId = currentExt ? (currentExt->GetRowId() - 1) : Max<TRowId>();
                    auto currentLastRowId = current->GetRowId();

                    auto page = Env->TryGetPage(Part, current->GetPageId(), {});
                    if (bytesLimit) {
                        bytes += Part->GetPageSize(current->GetPageId(), {});
                    }
                    ready &= bool(page);

                    auto prechargeCurrentFirstRowId = Min(currentFirstRowId, startRowId);
                    auto prechargeCurrentLastRowId = Max(currentLastRowId, endRowId);

                    if (key1Page && key1Page == current) {
                        if (needExactBounds && page) {
                            auto key1RowId = LookupRowIdReverse(key1, page, Scheme.Groups[0], ESeek::Lower, keyDefaults);
                            if (key1RowId != Max<TRowId>()) { // Max<TRowId>() means that lower bound is before current page, so doesn't charge current page
                                prechargeCurrentFirstRowId = Min(prechargeCurrentFirstRowId, key1RowId);
                            } else {
                                prechargeCurrentLastRowId = Max<TRowId>(); // no precharge
                            }
                        } else {
                            prechargeCurrentLastRowId = Max<TRowId>(); // no precharge
                        }
                    }
                    if (itemsLimit && prechargeCurrentFirstRowId >= prechargeCurrentLastRowId) {
                        ui64 left = itemsLimit - items; // we count only foolproof taken rows, so here we may precharge some extra rows
                        if (prechargeCurrentFirstRowId - prechargeCurrentLastRowId > left) {
                            prechargeCurrentLastRowId = prechargeCurrentFirstRowId - left;
                        }
                    }
                    if (prechargeCurrentFirstRowId >= prechargeCurrentLastRowId) {
                        items += prechargeCurrentFirstRowId - prechargeCurrentLastRowId + 1;
                    }
                    if (key2Page && key2Page >= current) {
                        if (key2Page == current) {
                            if (needExactBounds && page) {
                                auto key2RowId = LookupRowIdReverse(key2, page, Scheme.Groups[0], ESeek::Upper, keyDefaults);
                                if (key2RowId != Max<TRowId>()) { // Max<TRowId>() means that upper bound is before current page, so doesn't limit current page
                                    prechargeCurrentLastRowId = Max(prechargeCurrentLastRowId, key2RowId + 1);
                                }
                            } else {
                                prechargeCurrentLastRowId = Max<TRowId>(); // no precharge
                            }
                        } else {
                            prechargeCurrentLastRowId = Max<TRowId>(); // no precharge
                        }
                    }
                    if (prechargeCurrentFirstRowId >= prechargeCurrentLastRowId) {
                        if (prechargedRowsRange) {
                            prechargedRowsRange->second = prechargeCurrentLastRowId;
                        } else {
                            prechargedRowsRange.emplace(prechargeCurrentFirstRowId, prechargeCurrentLastRowId);
                        }
                        if (Groups) {
                            for (auto& g : Groups) {
                                ready &= DoPrechargeGroupReverse(g, prechargeCurrentFirstRowId, prechargeCurrentLastRowId, bytes);
                            }
                        }
                    }

                    if (current.Off() == 0) {
                        break;
                    }
                }

                if (prechargedRowsRange && HistoryIndex) {
                    ready &= DoPrechargeHistory(prechargedRowsRange->first, prechargedRowsRange->second);
                }
            }

            return ready;
        }

    private:
        bool DoPrechargeHistory(TRowId startRowId, TRowId endRowId) const noexcept {
            auto index = HistoryIndex->TryLoadRaw();
            if (!index) {
                return false;
            }

            if (endRowId < startRowId) {
                using std::swap;
                swap(endRowId, startRowId);
            }

            // Minimum key is (startRowId, max, max)
            ui64 startStep = Max<ui64>();
            ui64 startTxId = Max<ui64>();
            TCell startKeyCells[3] = {
                TCell::Make(startRowId),
                TCell::Make(startStep),
                TCell::Make(startTxId),
            };
            TCells startKey{ startKeyCells, 3 };

            // Maximum key is (endRowId, 0, 0)
            ui64 endStep = 0;
            ui64 endTxId = 0;
            TCell endKeyCells[3] = {
                TCell::Make(endRowId),
                TCell::Make(endStep),
                TCell::Make(endTxId),
            };
            TCells endKey{ endKeyCells, 3 };

            // Directly use the history group scheme
            const auto& scheme = Part->Scheme->HistoryGroup;
            Y_DEBUG_ABORT_UNLESS(scheme.ColsKeyIdx.size() == 3);

            // Directly use the history key defaults with correct sort order
            const TKeyCellDefaults* keyDefaults = Part->Scheme->HistoryKeys.Get();

            auto first = index->LookupKey(startKey, scheme, ESeek::Lower, keyDefaults);
            if (!first) {
                return true;
            }

            auto last = index->LookupKey(endKey, scheme, ESeek::Lower, keyDefaults);

            bool ready = true;
            bool hasItems = false;
            TRowId prechargedFirstRowId, prechargedLastRowId;

            for (auto current = first; current && current <= last; current++) {
                auto page = Env->TryGetPage(Part, current->GetPageId(), NPage::TGroupId(0, true));
                ready &= bool(page);

                if (!HistoryGroups) {
                    // don't need to calculate prechargedFirstRowId/prechargedLastRowId
                    continue;
                }

                auto currentExt = current + 1;
                auto prechargeCurrentFirstRowId = current->GetRowId();
                auto prechargeCurrentLastRowId = currentExt ? (currentExt->GetRowId() - 1) : Max<TRowId>();

                if (first == current) {
                    if (page) {
                        auto startKeyRowId = LookupRowId(startKey, page, scheme, ESeek::Lower, *keyDefaults);
                        prechargeCurrentFirstRowId = Max(prechargeCurrentFirstRowId, startKeyRowId);
                    } else {
                        prechargeCurrentFirstRowId = Max<TRowId>(); // no precharge
                    }
                }
                if (last == current) {
                    if (page) {
                        auto endKeyRowId = LookupRowId(endKey, page, scheme, ESeek::Upper, *keyDefaults);
                        if (endKeyRowId) {
                            prechargeCurrentLastRowId = Min(prechargeCurrentLastRowId, endKeyRowId - 1);
                        } else {
                            prechargeCurrentFirstRowId = Max<TRowId>(); // no precharge
                        }
                    } else {
                        prechargeCurrentFirstRowId = Max<TRowId>(); // no precharge
                    }
                }

                if (prechargeCurrentFirstRowId <= prechargeCurrentLastRowId) {
                    if (!hasItems) {
                        prechargedFirstRowId = prechargeCurrentFirstRowId;
                        hasItems = true;
                    }
                    prechargedLastRowId = prechargeCurrentLastRowId;
                }
            }

            if (hasItems && HistoryGroups) {
                for (auto& g : HistoryGroups) {
                    ui64 bytes = 0;
                    ready &= DoPrechargeGroup(g, prechargedFirstRowId, prechargedLastRowId, bytes);
                }
            }

            return ready;
        }

    private:
        struct TGroupState {
            TPartGroupFlatIndexIter GroupIndex;
            TIter Index;
            TRowId LastRowId = Max<TRowId>();
            const NPage::TGroupId GroupId;

            TGroupState(TPartGroupFlatIndexIter&& groupIndex, NPage::TGroupId groupId)
                : GroupIndex(groupIndex)
                , GroupId(groupId)
            { }
        };

    private:
        /**
         * Precharges pages that contain row1 to row2 inclusive
         */
        bool DoPrechargeGroup(TGroupState& group, TRowId row1, TRowId row2, ui64& bytes) const noexcept {
            auto groupIndex = group.GroupIndex.TryLoadRaw();
            if (!groupIndex) {
                if (bytes) {
                    // Note: we can't continue if we have bytes limit
                    bytes = Max<ui64>();
                }
                return false;
            }

            bool ready = true;

            if (!group.Index || row1 < group.Index->GetRowId() || row1 > group.LastRowId) {
                group.Index = groupIndex->LookupRow(row1, group.Index);
                if (Y_UNLIKELY(!group.Index)) {
                    // Looks like row1 doesn't even exist
                    group.LastRowId = Max<TRowId>();
                    return ready;
                }
                group.LastRowId = groupIndex->GetLastRowId(group.Index);
                auto pageId = group.Index->GetPageId();
                ready &= bool(Env->TryGetPage(Part, pageId, group.GroupId));
                bytes += Part->GetPageSize(pageId, group.GroupId);
            }

            while (group.LastRowId < row2) {
                if (!++group.Index) {
                    // Looks like row2 doesn't even exist
                    group.LastRowId = Max<TRowId>();
                    return ready;
                }
                group.LastRowId = groupIndex->GetLastRowId(group.Index);
                auto pageId = group.Index->GetPageId();
                ready &= bool(Env->TryGetPage(Part, pageId, group.GroupId));
                bytes += Part->GetPageSize(pageId, group.GroupId);
            }

            return ready;
        }

        /**
         * Precharges pages that contain row1 to row2 inclusive in reverse
         */
        bool DoPrechargeGroupReverse(TGroupState& group, TRowId row1, TRowId row2, ui64& bytes) const noexcept {
            auto groupIndex = group.GroupIndex.TryLoadRaw();
            if (!groupIndex) {
                if (bytes) {
                    // Note: we can't continue if we have bytes limit
                    bytes = Max<ui64>();
                }
                return false;
            }

            bool ready = true;

            if (!group.Index || row1 < group.Index->GetRowId() || row1 > group.LastRowId) {
                group.Index = groupIndex->LookupRow(row1, group.Index);
                if (Y_UNLIKELY(!group.Index)) {
                    // Looks like row1 doesn't even exist
                    group.LastRowId = Max<TRowId>();
                    return ready;
                }
                group.LastRowId = groupIndex->GetLastRowId(group.Index);
                auto pageId = group.Index->GetPageId();
                ready &= bool(Env->TryGetPage(Part, pageId, group.GroupId));
                bytes += Part->GetPageSize(pageId, group.GroupId);
            }

            while (group.Index->GetRowId() > row2) {
                if (group.Index.Off() == 0) {
                    // This was the last page we could precharge
                    return ready;
                }
                group.LastRowId = group.Index->GetRowId() - 1;
                --group.Index;
                auto pageId = group.Index->GetPageId();
                ready &= bool(Env->TryGetPage(Part, pageId, group.GroupId));
                bytes += Part->GetPageSize(pageId, group.GroupId);
            }

            return ready;
        }

    private:
        TRowId LookupRowId(const TCells key, const TSharedData* page, const TPartScheme::TGroupInfo &group, ESeek seek, const TKeyCellDefaults &keyDefaults) const noexcept
        {
            auto data = TDataPage(page);
            auto lookup = data.LookupKey(key, group, seek, &keyDefaults);
            auto rowId = data.BaseRow() + lookup.Off();
            return rowId;
        }

    private:
        TRowId LookupRowIdReverse(const TCells key, const TSharedData* page, const TPartScheme::TGroupInfo &group, ESeek seek, const TKeyCellDefaults &keyDefaults) const noexcept
        {
            auto data = TDataPage(page);
            auto lookup = data.LookupKeyReverse(key, group, seek, &keyDefaults);
            auto rowId = lookup
                ? data.BaseRow() + lookup.Off()
                : Max<TRowId>();
            return rowId;
        }

    private:
        bool LimitExceeded(ui64 value, ui64 limit) const noexcept {
            return limit && value > limit;
        }

    private:
        IPages * const Env = nullptr;
        const TPart * const Part = nullptr;
        const TPartScheme &Scheme;
        mutable TPartGroupFlatIndexIter Index;
        mutable std::optional<TPartGroupFlatIndexIter> HistoryIndex;
        mutable TSmallVec<TGroupState> Groups;
        mutable TSmallVec<TGroupState> HistoryGroups;
    };

}
}
