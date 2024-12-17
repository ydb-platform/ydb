#pragma once

#include "flat_part_iface.h"
#include "flat_part_index_iter_iface.h"
#include "flat_table_part.h"
#include "flat_row_eggs.h"
#include "flat_row_state.h"
#include "flat_part_pinout.h"
#include "flat_part_slice.h"
#include "flat_table_committed.h"
#include "flat_page_data.h"

namespace NKikimr {
namespace NTable {

    /**
     * Part group iterator that may be positioned on a specific row id
     */
    class TPartGroupRowIter {
    public:
        TPartGroupRowIter(const TPart* part, IPages* env, NPage::TGroupId groupId)
            : Part(part)
            , Env(env) 
            , GroupId(groupId)
            , Index_(CreateIndexIter(part, env, groupId))
            , Index(*Index_.Get())
        {
        }

        EReady Seek(TRowId rowId) noexcept {
            // Fast path, check if we already have the needed data page
            if (Data) {
                TRowId minRowId = Page.BaseRow();
                if (minRowId <= rowId) {
                    TRowId current = minRowId + Data.Off();
                    if (current == rowId) {
                        return EReady::Data;
                    }
                    TRowId endRowId = minRowId + Data.End();
                    if (rowId < endRowId) {
                        Data = Page->Begin() + (rowId - minRowId);
                        Y_ABORT_UNLESS(Data, "Unexpected failure to find column group record for RowId=%lu", rowId);
                        return EReady::Data;
                    }
                }
            }

            Data = { };

            // Make sure we're at the correct index position first
            if (auto ready = Index.Seek(rowId); ready != EReady::Data) {
                return ready;
            }

            // Make sure we have the correct data page loaded
            if (!LoadPage(Index.GetPageId(), Index.GetRowId())) {
                return EReady::Page;
            }
            Y_DEBUG_ABORT_UNLESS(Page.BaseRow() <= rowId, "Index and row have an unexpected relation");

            Data = Page->Begin() + (rowId - Page.BaseRow());
            Y_ABORT_UNLESS(Data, "Unexpected failure to find record for RowId=%lu", rowId);
            return EReady::Data;
        }

        Y_FORCE_INLINE bool IsValid() const noexcept {
            return bool(Data);
        }

        Y_FORCE_INLINE TRowId GetRowId() const noexcept {
            return IsValid() ? (Page.BaseRow() + Data.Off()) : Max<TRowId>();
        }

        const NPage::TDataPage::TRecord* GetRecord() const noexcept {
            Y_DEBUG_ABORT_UNLESS(IsValid(), "Use of unpositioned iterator");
            return Data.GetRecord();
        }

    protected:
        bool LoadPage(TPageId pageId, TRowId baseRow) noexcept
        {
            if (PageId != pageId) {
                Data = { };
                if (!Page.Set(Env->TryGetPage(Part, pageId, GroupId))) {
                    PageId = Max<TPageId>();
                    return false;
                }
                PageId = pageId;
            }
            Y_DEBUG_ABORT_UNLESS(Page.BaseRow() == baseRow, "Index and data are out of sync");
            return true;
        }

    protected:
        const TPart* const Part;
        IPages* const Env;
        const NPage::TGroupId GroupId;

        TPageId PageId = Max<TPageId>();

        THolder<IPartGroupIndexIter> Index_;
        IPartGroupIndexIter& Index;
        NPage::TDataPage Page;
        NPage::TDataPage::TIter Data;
    };

    /**
     * Part group iterator that may be positioned on a specific key
     */
    class TPartGroupKeyIter : private TPartGroupRowIter
    {
    public:
        using TCells = NPage::TCells;

        TPartGroupKeyIter(const TPart* part, IPages* env, NPage::TGroupId groupId)
            : TPartGroupRowIter(part, env, groupId)
            , BeginRowId(0)
            , EndRowId(Index.GetEndRowId())
            , RowId(Max<TRowId>())
        {
        }

        void SetBounds(TRowId beginRowId, TRowId endRowId) noexcept
        {
            BeginRowId = beginRowId;
            EndRowId = Min(endRowId, Index.GetEndRowId());
            Y_DEBUG_ABORT_UNLESS(BeginRowId < EndRowId,
                "Trying to iterate over empty bounds=[%lu,%lu)", BeginRowId, EndRowId);
            RowId = Max<TRowId>();
        }

        EReady Seek(const TCells key, ESeek seek,
                const TPartScheme::TGroupInfo& scheme, const TKeyCellDefaults* keyDefaults) noexcept
        {
            Y_DEBUG_ABORT_UNLESS(seek == ESeek::Exact || seek == ESeek::Lower || seek == ESeek::Upper,
                    "Only ESeek{Exact, Upper, Lower} are currently supported here");

            if (auto ready = Index.Seek(seek, key, keyDefaults); ready != EReady::Data) {
                return Terminate(ready);
            }

            if (Index.GetRowId() >= EndRowId) {
                // Page is outside of bounds
                return Exhausted();
            }

            if (Index.GetRowId() < BeginRowId) {
                // Find an index record that has BeginRowId
                if (seek == ESeek::Exact) {
                    if (Index.GetNextRowId() <= BeginRowId) {
                        // We cannot return any other row, don't even try
                        return Exhausted();
                    }
                } else {
                    if (auto ready = SeekIndex(BeginRowId); ready != EReady::Data) {
                        return Terminate(ready);
                    }
                }
            }

            if (!LoadPage(Index.GetPageId(), Index.GetRowId())) {
                // Exact RowId unknown, don't allow Next until another Seek
                return Terminate(EReady::Page);
            }

            if (Data = Page.LookupKey(key, scheme, seek, keyDefaults)) {
                RowId = Page.BaseRow() + Data.Off();

                if (RowId >= EndRowId) {
                    // Row is outside of bounds
                    return Exhausted();
                }

                if (RowId < BeginRowId) {
                    if (seek == ESeek::Exact) {
                        // We cannot return any other row, don't even try
                        return Exhausted();
                    }

                    // Adjust to the first available row
                    Data += BeginRowId - RowId;
                    RowId = BeginRowId;
                    Y_DEBUG_ABORT_UNLESS(Data, "Unexpected failure to find BeginRowId=%lu", BeginRowId);
                }

                return EReady::Data;
            }

            if (seek != ESeek::Exact) {
                // The row we seek is on the next page

                if (auto ready = Index.Next(); ready != EReady::Data) {
                    return Terminate(ready);
                }

                RowId = Index.GetRowId();
                Y_DEBUG_ABORT_UNLESS(RowId > BeginRowId,
                    "Unexpected next page RowId=%lu (BeginRowId=%lu)",
                    RowId, BeginRowId);

                if (RowId < EndRowId) {
                    if (auto ready = LoadRowData(); ready != EReady::Data) {
                        return Terminate(ready);
                    }
                    return EReady::Data;
                }
            }

            return Exhausted();
        }

        EReady SeekReverse(const TCells key, ESeek seek,
                const TPartScheme::TGroupInfo& scheme, const TKeyCellDefaults* keyDefaults) noexcept
        {
            Y_DEBUG_ABORT_UNLESS(seek == ESeek::Exact || seek == ESeek::Lower || seek == ESeek::Upper,
                    "Only ESeek{Exact, Upper, Lower} are currently supported here");

            if (auto ready = Index.SeekReverse(seek, key, keyDefaults); ready != EReady::Data) {
                return Terminate(ready);
            }

            if (Index.GetRowId() < BeginRowId) {
                // Page may be outside of bounds
                if (Index.GetNextRowId() <= BeginRowId) {
                    // All rows are outside of bounds
                    return Exhausted();
                }
            }

            if (EndRowId <= Index.GetRowId()) {
                // Find an index record that has EndRowId - 1
                if (auto ready = SeekIndex(EndRowId - 1); ready != EReady::Data) {
                    return Terminate(ready);
                }
            }

            if (!LoadPage(Index.GetPageId(), Index.GetRowId())) {
                // Exact RowId unknown, don't allow Next until another Seek
                return Terminate(EReady::Page);
            }

            if (Data = Page.LookupKeyReverse(key, scheme, seek, keyDefaults)) {
                RowId = Page.BaseRow() + Data.Off();

                if (RowId < BeginRowId) {
                    // Row is outside of bounds
                    return Exhausted();
                }

                if (RowId >= EndRowId) {
                    // Adjust to the first available row
                    auto diff = RowId - (EndRowId - 1);
                    Y_DEBUG_ABORT_UNLESS(Data.Off() >= diff, "Unexpected failure to find LastRowId=%lu", EndRowId - 1);
                    Data -= diff;
                    RowId = EndRowId - 1;
                }

                return EReady::Data;
            }

            if (seek != ESeek::Exact) {
                // The row we seek is on the prev page

                RowId = Index.GetRowId() - 1;
                if (auto ready = Index.Prev(); ready != EReady::Data) {
                    return Terminate(ready);
                }

                Y_DEBUG_ABORT_UNLESS(RowId < EndRowId,
                    "Unexpected prev page RowId=%lu (EndRowId=%lu)",
                    RowId, EndRowId);
                if (RowId >= BeginRowId) {
                    if (auto ready = LoadRowData(); ready != EReady::Data) {
                        return Terminate(ready);
                    }
                    return EReady::Data;
                }
            }

            return Exhausted();
        }

        EReady Seek(TRowId rowId) noexcept
        {
            if (Y_UNLIKELY(rowId < BeginRowId || rowId >= EndRowId)) {
                return Exhausted();
            }

            if (auto ready = SeekIndex(rowId); ready != EReady::Data) {
                return Terminate(ready);
            }

            RowId = rowId;

            if (auto ready = LoadRowData(); ready != EReady::Data) {
                return Terminate(ready);
            }
            return EReady::Data;
        }

        EReady SeekToSliceFirstRow() noexcept
        {
            return Seek(BeginRowId);
        }

        EReady SeekToSliceLastRow() noexcept
        {
            return Seek(EndRowId - 1);
        }

        /**
         * If has Data, goes to the next row
         * 
         * If doesn't have Data, loads the current row
         */
        EReady Next() noexcept
        {
            if (Y_UNLIKELY(RowId == Max<TRowId>())) {
                return EReady::Gone;
            }

            Y_DEBUG_ABORT_UNLESS(RowId >= BeginRowId && RowId < EndRowId,
                "Unexpected RowId=%lu outside of iteration bounds [%lu,%lu)",
                RowId, BeginRowId, EndRowId);

            if (Data) {
                if (RowId + 1 >= EndRowId) {
                    return Exhausted();
                }

                if (Data + 1) {
                    ++RowId;
                    ++Data;
                    return EReady::Data;
                }

                // Go to the next data page
                // Keep Data in case we have page faulted on Index.Next and need to call it again
                auto ready = Index.Next();
                if (ready == EReady::Gone) {
                    return Exhausted();
                } else if (ready == EReady::Page) {
                    return ready;
                } else {
                    ++RowId;
                    Data = { };
                }
            }

            if (auto ready = LoadRowData(); ready != EReady::Gone) {
                return ready;
            }

            Y_ABORT_UNLESS(Index.Next() == EReady::Gone, "Unexpected failure to seek in a non-final data page");
            return Exhausted();
        }

        /**
         * If has Data, goes to the prev row
         * 
         * If doesn't have Data, loads the current row
         */
        EReady Prev() noexcept
        {
            if (Y_UNLIKELY(RowId == Max<TRowId>())) {
                return EReady::Gone;
            }

            Y_DEBUG_ABORT_UNLESS(RowId >= BeginRowId && RowId < EndRowId,
                "Unexpected RowId=%lu outside of iteration bounds [%lu,%lu)",
                RowId, BeginRowId, EndRowId);

            if (Data) {
                if (RowId <= BeginRowId) {
                    return Exhausted();
                }

                if (Data.Off() != 0) {
                    --RowId;
                    --Data;
                    return EReady::Data;
                }
                
                // Go to the prev data page
                // Keep Data in case we have page faulted on Index.Prev and need to call it again
                auto ready = Index.Prev();
                if (ready == EReady::Gone) {
                    return Exhausted();
                } else if (ready == EReady::Page) {
                    return ready;
                } else {
                    --RowId;
                    Data = { };
                }
            }

            if (auto ready = LoadRowData(); ready != EReady::Gone) {
                return ready;
            }

            Y_ABORT("Unexpected failure to seek in a non-final data page");
        }

        using TPartGroupRowIter::IsValid;
        using TPartGroupRowIter::GetRecord;

        Y_FORCE_INLINE TRowId GetRowId() const noexcept {
            // N.B. we don't check IsValid because screen-related code may
            // call GetRowId() even after EReady::Page or EReady::Gone.
            return RowId;
        }

        const TSharedData& GetPageData() const noexcept {
            return Page.GetData();
        }

    private:
        Y_FORCE_INLINE EReady Exhausted() noexcept
        {
            return Terminate(EReady::Gone);
        }
        
        Y_FORCE_INLINE EReady Terminate(EReady ready) noexcept
        {
            Data = { };
            RowId = Max<TRowId>();
            return ready;
        }

        EReady LoadRowData() noexcept
        {
            Y_DEBUG_ABORT_UNLESS(Index.IsValid() && Index.GetRowId() <= RowId,
                "Called without a valid index record");

            if (!LoadPage(Index.GetPageId(), Index.GetRowId())) {
                return EReady::Page;
            }

            if (Data = Page->Begin() + (RowId - Page.BaseRow())) {
                return EReady::Data;
            }

            return EReady::Gone;
        }

        EReady SeekIndex(TRowId rowId) noexcept
        {
            auto ready = Index.Seek(rowId);
            Y_DEBUG_ABORT_UNLESS(ready != EReady::Gone,
                "Unexpected failure to find index record for RowId=%lu, bounds=[%lu,%lu)",
                rowId, BeginRowId, EndRowId);
            return ready;
        }

    protected:
        TRowId BeginRowId;
        TRowId EndRowId;
        TRowId RowId;
    };

    /**
     * Part iterator over history data
     */
    class TPartHistoryIter : private TPartGroupRowIter
    {
    public:
        using TCells = NPage::TCells;

        explicit TPartHistoryIter(const TPart* part, IPages* env)
            : TPartGroupRowIter(part, env, NPage::TGroupId(0, /* historic */ true))
        { }

        EReady Seek(TRowId rowId, const TRowVersion& rowVersion) noexcept
        {
            Y_DEBUG_ABORT_UNLESS(rowId != Max<TRowId>());

            static_assert(sizeof(rowId) == sizeof(ui64), "sizes don't match");
            static_assert(sizeof(rowVersion.Step) == sizeof(ui64), "sizes don't match");
            static_assert(sizeof(rowVersion.TxId) == sizeof(ui64), "sizes don't match");

            // Construct a synthetic (rowId, step, txId) key on the stack
            TCell keyCells[3] = {
                TCell::Make(rowId),
                TCell::Make(rowVersion.Step),
                TCell::Make(rowVersion.TxId),
            };
            TCells key{ keyCells, 3 };

            // Directly use the history group scheme
            const auto& scheme = Part->Scheme->HistoryGroup;
            Y_DEBUG_ABORT_UNLESS(scheme.ColsKeyIdx.size() == 3);
            Y_DEBUG_ABORT_UNLESS(scheme.ColsKeyData.size() == 3);

            // Directly use the history key keyDefaults with correct sort order
            const TKeyCellDefaults* keyDefaults = Part->Scheme->HistoryKeys.Get();

            // Helper for loading row id and row version from the index
            auto checkIndex = [&]() -> bool {
                RowId = Index.GetKeyCell(0).AsValue<TRowId>();
                RowVersion.Step = Index.GetKeyCell(1).AsValue<ui64>();
                RowVersion.TxId = Index.GetKeyCell(2).AsValue<ui64>();
                return rowId == RowId;
            };

            // Helper for loading row id and row version from the data page
            auto checkData = [&]() -> bool {
                RowId = Data->Cell(scheme.ColsKeyData[0]).AsValue<TRowId>();
                RowVersion.Step = Data->Cell(scheme.ColsKeyData[1]).AsValue<ui64>();
                RowVersion.TxId = Data->Cell(scheme.ColsKeyData[2]).AsValue<ui64>();
                return rowId == RowId;
            };

            // Returns { } when current page is not sufficient
            auto seekDownToRowVersion = [&]() -> std::optional<EReady> {
                Y_DEBUG_ABORT_UNLESS(rowId == RowId && Data);
                Y_DEBUG_ABORT_UNLESS(rowVersion < RowVersion);

                // If we previously returned EReady::Data, then we are
                // potentially seeking to an older row version on the same
                // page. We want to optimize for two cases:
                //
                // 1. When the next row is very close, we dont want to perform
                //    expensive index seeks, page reloads and binary searches
                //
                // 2. When the next row is far away, we still attempt to find
                //    it on the current page, otherwise fallback to a binary
                //    search over the whole group.
                for (int linear = 4; linear >= 0; --linear) {
                    ++Data;

                    // Perform binary search on the last iteration
                    if (linear == 0 && Data) {
                        Data = Page.LookupKey(key, scheme, ESeek::Lower, keyDefaults);
                    }

                    if (!Data) {
                        // Row is not on current page, move to the next
                        switch (Index.Next()) {
                            case EReady::Page: {
                                // Fallback to full binary search (maybe we need some other index page)
                                RowId = Max<TRowId>();
                                return { };
                            };
                            case EReady::Gone: {
                                return Exhausted();
                            };
                            case EReady::Data: {
                                Y_DEBUG_ABORT_UNLESS(Index.GetKeyCellsCount(), "Non-first page is expected to have key cells");
                                if (Index.GetKeyCellsCount()) {
                                    if (!checkIndex()) {
                                        // First row for the next RowId
                                        MaxVersion = TRowVersion::Max();
                                        Y_DEBUG_ABORT_UNLESS(rowId < RowId);
                                        return EReady::Gone;
                                    }

                                    // Next page has the same row id
                                    // Save an estimate for MaxVersion
                                    MaxVersion = rowVersion;
                                    return { };
                                } else {
                                    // Fallback to full binary search
                                    RowId = Max<TRowId>();
                                    return { };
                                }
                            };
                        };
                    }

                    if (!checkData()) {
                        // First row for the new RowId
                        MaxVersion = TRowVersion::Max();
                        Y_DEBUG_ABORT_UNLESS(rowId < RowId);
                        return EReady::Gone;
                    }

                    if (RowVersion <= rowVersion) {
                        // Save an estimate for MaxVersion
                        MaxVersion = rowVersion;
                        return EReady::Data;
                    }
                }

                // This lambda has to be terminated
                Y_ABORT_UNLESS(false, "Data binary search bug");
            };

            // Special case when we already have data with the same row id
            if (rowId == RowId && Data) {
                // Check if the current row is correct.
                // This is mainly for a case when we returned EReady::Gone
                // and now we want the first version of the next row.
                if (RowVersion <= rowVersion && rowVersion <= MaxVersion) {
                    return EReady::Data;
                }

                // Check if we are descending below the current row
                if (Y_LIKELY(rowVersion < RowVersion)) {
                    if (std::optional<EReady> ready = seekDownToRowVersion(); ready.has_value()) {
                        return *ready;
                    }
                    Y_DEBUG_ABORT_UNLESS(!Data, "Shouldn't continue search with Data");
                } else  {
                    // High-level iterators never go up once descended to a
                    // particular version within a specific key. However a
                    // cached iterator may be reused by seeking to an earlier
                    // key. In that case history may be positioned very late in
                    // the version chain and we have to force a full seek.
                    Data = { };
                    RowId = Max<TRowId>();
                }
            }

            // Special case when we are following a previous Index estimate
            if (rowId == RowId && RowVersion <= rowVersion && rowVersion <= MaxVersion) {
                Y_DEBUG_ABORT_UNLESS(Index.IsValid());
                Y_DEBUG_ABORT_UNLESS(!Data);

                if (!LoadPage(Index.GetPageId(), Index.GetRowId())) {
                    return EReady::Page;
                }

                Data = Page->Begin();

                Y_ABORT_UNLESS(checkData() && RowVersion <= rowVersion, "Index and Data are out of sync");

                return EReady::Data;
            }

            // Full index binary search
            if (auto ready = Index.Seek(ESeek::Lower, key, keyDefaults); ready != EReady::Data) {
                return Terminate(ready);
            }

            // We need exact match on rowId, bail on larger values
            Y_DEBUG_ABORT_UNLESS(Index.GetRowId() == 0 || Index.GetKeyCellsCount(), "Non-first page is expected to have key cells");
            if (Index.GetKeyCellsCount()) {
                TRowId indexRowId = Index.GetKeyCell(0).AsValue<TRowId>();
                if (rowId < indexRowId) {
                    // We cannot compute MaxVersion anyway as indexRowId row may be presented on the previous page
                    // and last existing version for rowId is unknown
                    return Exhausted();
                }
            } else {
                // No information about the current index row
                RowId = Max<TRowId>();
            }

            if (!LoadPage(Index.GetPageId(), Index.GetRowId())) {
                // It's ok to repeat binary search on the next iteration,
                // since page faults take a long time and optimizing it
                // wouldn't be worth it.
                return Terminate(EReady::Page);
            }

            // Full data binary search
            if (Data = Page.LookupKey(key, scheme, ESeek::Lower, keyDefaults)) {
                if (!checkData()) {
                    // First row for the next RowId
                    MaxVersion = TRowVersion::Max();
                    Y_DEBUG_ABORT_UNLESS(rowId < RowId);
                    return EReady::Gone;
                }

                Y_ABORT_UNLESS(RowVersion <= rowVersion, "Data binary search bug");

                // Save an estimate for MaxVersion
                MaxVersion = rowVersion;
                return EReady::Data;
            }

            // Our key might be on the next page as lookups are not exact
            if (auto ready = Index.Next(); ready != EReady::Data) {
                // TODO: do not drop current index pages in case of a page fault
                // Will also repeat index binary search in case of a page fault on the next iteration
                return Terminate(ready);
            }

            Y_DEBUG_ABORT_UNLESS(Index.GetKeyCellsCount(), "Non-first page is expected to have key cells");
            if (Y_LIKELY(Index.GetKeyCellsCount())) {
                if (!checkIndex()) {
                    // First row for the nextRowId
                    MaxVersion = TRowVersion::Max();
                    Y_DEBUG_ABORT_UNLESS(rowId < RowId);
                    return EReady::Gone;
                }

                // The above binary search failed, but since we started with
                // an index search the first row must be the one we want.
                Y_ABORT_UNLESS(RowVersion <= rowVersion, "Index binary search bug");
            } else {
                // No information about the current index row
                RowId = Max<TRowId>();
            }

            if (!LoadPage(Index.GetPageId(), Index.GetRowId())) {
                // We don't want to repeat binary search on the next
                // iteration, as we already know the row is not on a
                // previous page, but index search would point to it
                // again and reloading would be very expensive.
                // Remember current MaxVersion estimate, so we just
                // quickly check the same page again after restart.
                MaxVersion = rowVersion;
                return EReady::Page;
            }

            Data = Page->Begin();
            Y_ABORT_UNLESS(Data);

            if (Index.GetKeyCellsCount()) {
                // Must have rowId as we have checked index
                Y_ABORT_UNLESS(checkData() && RowVersion <= rowVersion, "Index and Data are out of sync");

                // Save an estimate for MaxVersion
                MaxVersion = rowVersion;
                return EReady::Data;
            } else {
                if (checkData()) {
                    Y_ABORT_UNLESS(RowVersion <= rowVersion, "Index and Data are out of sync");

                    // Save an estimate for MaxVersion
                    MaxVersion = rowVersion;
                    return EReady::Data;
                } else {
                    // First row for the nextRowId
                    MaxVersion = TRowVersion::Max();
                    Y_DEBUG_ABORT_UNLESS(rowId < RowId);
                    return EReady::Gone;
                }
            }
        }

        using TPartGroupRowIter::IsValid;
        using TPartGroupRowIter::GetRecord;

        TRowId GetHistoryRowId() const noexcept {
            Y_DEBUG_ABORT_UNLESS(IsValid());
            return TPartGroupRowIter::GetRowId();
        }

        TRowId GetRowId() const noexcept {
            Y_DEBUG_ABORT_UNLESS(IsValid());
            return RowId;
        }

        const TRowVersion& GetRowVersion() const noexcept {
            Y_DEBUG_ABORT_UNLESS(IsValid());
            return RowVersion;
        }

    private:
        Y_FORCE_INLINE EReady Exhausted() noexcept
        {
            return Terminate(EReady::Gone);
        }

        Y_FORCE_INLINE EReady Terminate(EReady ready) noexcept
        {
            Data = { };
            RowId = Max<TRowId>();
            return ready;
        }

    private:
        TRowId RowId = Max<TRowId>();
        TRowVersion RowVersion;
        TRowVersion MaxVersion;
    };

    class TPartIter final {
    public:
        using TCells = NPage::TCells;
        using TGroupId = NPage::TGroupId;

        TPartIter(const TPart* part, TTagsRef tags, TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults, IPages* env)
            : Part(part)
            , Env(env)
            , Pinout(Part->Scheme->MakePinout(tags))
            , KeyCellDefaults(std::move(keyDefaults))
            , Main(Part, Env, TGroupId(0))
            , SkipMainDeltas(0)
            , SkipMainVersion(false)
            , SkipEraseVersion(false)
        {
            Groups.reserve(Pinout.AltGroups().size());
            GroupRemap.resize(Part->Scheme->Groups.size(), Max<ui32>());

            for (size_t idx : xrange(Pinout.AltGroups().size())) {
                ui32 group = Pinout.AltGroups()[idx];
                TGroupId groupId(group);
                Groups.emplace_back(Part, Env, groupId);
                GroupRemap[group] = idx;
            }

            size_t KeyCellDefaultsSize = KeyCellDefaults->Size();
            Key.resize(KeyCellDefaultsSize);

            size_t infoSize = Part->Scheme->Groups[0].ColsKeyData.size();
            for (size_t pos = infoSize; pos < KeyCellDefaultsSize; ++pos) {
                Key[pos] = (*KeyCellDefaults)[pos];
            }
        }

        void SetBounds(const TSlice& slice) noexcept
        {
            Main.SetBounds(slice.BeginRowId(), slice.EndRowId());
        }

        void SetBounds(TRowId beginRowId, TRowId endRowId) noexcept
        {
            Main.SetBounds(beginRowId, endRowId);
        }

        EReady Seek(const TCells key, ESeek seek) noexcept
        {
            ClearKey();
            return Main.Seek(key, seek, Part->Scheme->Groups[0], &*KeyCellDefaults);
        }

        EReady SeekReverse(const TCells key, ESeek seek) noexcept
        {
            ClearKey();
            return Main.SeekReverse(key, seek, Part->Scheme->Groups[0], &*KeyCellDefaults);
        }

        EReady Seek(TRowId rowId) noexcept
        {
            ClearKey();
            return Main.Seek(rowId);
        }

        EReady SeekToSliceFirstRow() noexcept
        {
            ClearKey();
            return Main.SeekToSliceFirstRow();
        }

        EReady SeekToSliceLastRow() noexcept
        {
            ClearKey();
            return Main.SeekToSliceLastRow();
        }

        EReady Next() noexcept
        {
            ClearKey();
            return Main.Next();
        }

        EReady Prev() noexcept
        {
            ClearKey();
            return Main.Prev();
        }

        Y_FORCE_INLINE bool IsValid() const noexcept
        {
            if (!SkipMainVersion) {
                return Main.IsValid();
            } else {
                Y_DEBUG_ABORT_UNLESS(HistoryState, "SkipMainVersion set, but no HistoryState");
                Y_DEBUG_ABORT_UNLESS(Main.IsValid());
                return HistoryState->History.IsValid() && HistoryState->History.GetRowId() == Main.GetRowId();
            }
        }

        Y_FORCE_INLINE TRowId GetRowId() const noexcept
        {
            return Main.GetRowId();
        }

        TDbTupleRef GetKey() const noexcept
        {
            InitKey();

            return TDbTupleRef(KeyCellDefaults->BasicTypes().begin(), Key.begin(), Key.size());
        }

        const TSharedData& GetKeyPage() const noexcept
        {
            return Main.GetPageData();
        }

        TCells GetRawKey() const noexcept
        {
            InitKey();

            return TCells(Key).Slice(0, Part->Scheme->Groups[0].ColsKeyData.size());
        }

        TRowVersion GetRowVersion() const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(IsValid(), "Attempt to get invalid row version");

            if (!SkipMainVersion) {
                const auto& info = Part->Scheme->Groups[0];
                const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);
                Y_ABORT_UNLESS(!data->IsDelta(), "GetRowVersion cannot be called on deltas");

                if (!SkipEraseVersion && data->IsErased()) {
                    return data->GetMaxVersion(info);
                }

                if (data->IsVersioned()) {
                    return data->GetMinVersion(info);
                } else {
                    return Part->MinRowVersion;
                }
            } else {
                const auto* data = HistoryState->History.GetRecord();
                const auto& info = Part->Scheme->HistoryGroup;

                if (!SkipEraseVersion && data->IsErased()) {
                    return data->GetMaxVersion(info);
                }

                // History group stores version as part of its key
                Y_DEBUG_ABORT_UNLESS(!data->IsVersioned());

                return HistoryState->History.GetRowVersion();
            }
        }

        EReady SkipToRowVersion(TRowVersion rowVersion, TIteratorStats& stats,
                                NTable::ITransactionMapSimplePtr committedTransactions,
                                NTable::ITransactionObserverSimplePtr transactionObserver,
                                const NTable::ITransactionSet& decidedTransactions) noexcept
        {
            Y_DEBUG_ABORT_UNLESS(Main.IsValid(), "Attempt to use an invalid iterator");

            // We cannot use min/max hints when part has uncommitted deltas
            if (!Part->TxIdStats) {
                if (Part->MaxRowVersion <= rowVersion) {
                    // Positioning to a known HEAD row version
                    return EReady::Data;
                }

                if (rowVersion < Part->MinRowVersion) {
                    // Don't bother seeking below the known first row version
                    if (!SkipMainVersion) {
                        const auto& info = Part->Scheme->Groups[0];
                        const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);
                        Y_ABORT_UNLESS(!data->IsDelta(), "Unexpected delta without TxIdStats");
                        if (!SkipEraseVersion && data->IsErased()) {
                            transactionObserver.OnSkipCommitted(data->GetMaxVersion(info));
                        } else if (data->IsVersioned()) {
                            transactionObserver.OnSkipCommitted(data->GetMinVersion(info));
                        } else {
                            transactionObserver.OnSkipCommitted(Part->MinRowVersion);
                        }
                        stats.InvisibleRowSkips++;
                        stats.UncertainErase = true;
                    }
                    return EReady::Gone;
                }
            }

            bool trustHistory;

            if (!SkipMainVersion) {
                const auto& info = Part->Scheme->Groups[0];
                const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);

                while (data->IsDelta()) {
                    ui64 txId = data->GetDeltaTxId(info);
                    const auto* commitVersion = committedTransactions.Find(txId);
                    if (commitVersion && *commitVersion <= rowVersion) {
                        // Already committed and correct version
                        if (!decidedTransactions.Contains(txId)) {
                            // This change may rollback and change the iteration result
                            stats.UncertainErase = true;
                        }
                        return EReady::Data;
                    }
                    if (commitVersion) {
                        // Skipping a newer committed delta
                        transactionObserver.OnSkipCommitted(*commitVersion, txId);
                        stats.InvisibleRowSkips++;
                        if (data->GetRop() != ERowOp::Erase) {
                            // Skipping non-erase delta, so any erase below cannot be trusted
                            stats.UncertainErase = true;
                        }
                    } else {
                        // Skipping an uncommitted delta
                        transactionObserver.OnSkipUncommitted(txId);
                        if (data->GetRop() != ERowOp::Erase && !decidedTransactions.Contains(txId)) {
                            // This change may commit and change the iteration result
                            stats.UncertainErase = true;
                        }
                    }
                    data = Main.GetRecord()->GetAltRecord(++SkipMainDeltas);
                    if (!data) {
                        // This was the last delta, nothing else for this key
                        SkipMainDeltas = 0;
                        return EReady::Gone;
                    }
                }

                if (!SkipEraseVersion && data->IsErased()) {
                    TRowVersion current = data->GetMaxVersion(info);
                    if (current <= rowVersion) {
                        // Already correct version
                        return EReady::Data;
                    }
                    SkipEraseVersion = true;
                    transactionObserver.OnSkipCommitted(current);
                    stats.InvisibleRowSkips++;
                    stats.UncertainErase = true;
                }

                TRowVersion current = data->IsVersioned() ? data->GetMinVersion(info) : Part->MinRowVersion;
                if (current <= rowVersion) {
                    // Already correct version
                    return EReady::Data;
                }

                transactionObserver.OnSkipCommitted(current);
                stats.InvisibleRowSkips++;
                stats.UncertainErase = true;

                if (!data->HasHistory()) {
                    // There is no history, reset
                    SkipEraseVersion = false;
                    return EReady::Gone;
                }

                if (!HistoryState) {
                    // Initialize history state once per iterator
                    HistoryState.ConstructInPlace(Part, Env, Pinout);
                }

                trustHistory = false;
                SkipMainVersion = true;
                SkipEraseVersion = false;
            } else {
                // This is not the first time SkipToRowVersion is called for
                // the current key, so we can be sure history iterator would
                // not need to backtrack.
                trustHistory = true;
            }

            EReady ready;
            if (trustHistory &&
                HistoryState->History.IsValid() &&
                HistoryState->History.GetRowId() == Main.GetRowId() &&
                HistoryState->History.GetRowVersion() <= rowVersion)
            {
                // We should have called Seek at least once for current row
                Y_DEBUG_ABORT_UNLESS(HistoryState->History.GetRowId() == Main.GetRowId());

                // History is already positioned on version that is old enough
                ready = EReady::Data;
            } else {
                // Seek might switch to a different row, must look at erase version
                SkipEraseVersion = false;

                // Find the first row that is as old as rowVersion
                ready = HistoryState->History.Seek(Main.GetRowId(), rowVersion);
            }

            switch (ready) {
                case EReady::Data: {
                    const auto* data = HistoryState->History.GetRecord();
                    const auto& info = Part->Scheme->HistoryGroup;

                    if (!SkipEraseVersion && data->IsErased()) {
                        TRowVersion current = data->GetMaxVersion(info);
                        if (current <= rowVersion) {
                            // Use the erase version
                            return EReady::Data;
                        }
                        SkipEraseVersion = true;
                    }

                    Y_DEBUG_ABORT_UNLESS(HistoryState->History.GetRowVersion() <= rowVersion);
                    return EReady::Data;
                }

                case EReady::Page: {
                    return EReady::Page;
                }

                case EReady::Gone: {
                    // End of history, reset
                    SkipMainVersion = false;
                    SkipEraseVersion = false;
                    return EReady::Gone;
                }
            }

            Y_UNREACHABLE();
        }

        std::optional<TRowVersion> SkipToCommitted(NTable::ITransactionMapSimplePtr committedTransactions,
                NTable::ITransactionObserverSimplePtr transactionObserver) noexcept
        {
            Y_DEBUG_ABORT_UNLESS(Main.IsValid(), "Attempt to use an invalid iterator");
            Y_ABORT_UNLESS(!SkipMainVersion, "Cannot use SkipToCommitted after positioning to history");

            const auto& info = Part->Scheme->Groups[0];
            const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);

            while (data->IsDelta()) {
                ui64 txId = data->GetDeltaTxId(info);
                const auto* commitVersion = committedTransactions.Find(txId);
                if (commitVersion) {
                    // Found a committed delta
                    return *commitVersion;
                }
                // Skip an uncommitted delta
                transactionObserver.OnSkipUncommitted(txId);
                data = Main.GetRecord()->GetAltRecord(++SkipMainDeltas);
                if (!data) {
                    // This was the last delta, nothing else for this key
                    SkipMainDeltas = 0;
                    return { };
                }
            }

            if (!SkipEraseVersion && data->IsErased()) {
                // Return erase version of this row
                return data->GetMaxVersion(info);
            }

            // Otherwise either row version or part version is the first committed version
            return data->IsVersioned() ? data->GetMinVersion(info) : Part->MinRowVersion;
        }

        bool IsDelta() const noexcept
        {
            if (SkipMainVersion) {
                return false;
            }

            Y_DEBUG_ABORT_UNLESS(Main.IsValid(), "Cannot use unpositioned iterators");

            const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);
            return data->IsDelta();
        }

        ui64 GetDeltaTxId() const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(!SkipMainVersion, "Current record is not a delta record");
            Y_DEBUG_ABORT_UNLESS(Main.IsValid(), "Cannot use unpositioned iterators");

            const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);
            Y_DEBUG_ABORT_UNLESS(data->IsDelta(), "Current record is not a delta record");

            const auto& info = Part->Scheme->Groups[0];
            return data->GetDeltaTxId(info);
        }

        void ApplyDelta(TRowState& row) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(!SkipMainVersion, "Current record is not a delta record");
            Y_DEBUG_ABORT_UNLESS(Main.IsValid(), "Cannot use unpositioned iterators");

            const ui32 index = SkipMainDeltas;
            const auto* data = Main.GetRecord()->GetAltRecord(index);
            Y_DEBUG_ABORT_UNLESS(data->IsDelta(), "Current record is not a delta record");

            if (row.Touch(data->GetRop())) {
                for (auto& pin : Pinout) {
                    if (!row.IsFinalized(pin.To)) {
                        Apply(row, pin, data, index);
                    }
                }
            }
        }

        EReady SkipDelta() noexcept
        {
            Y_DEBUG_ABORT_UNLESS(!SkipMainVersion, "Current record is not a delta record");
            Y_DEBUG_ABORT_UNLESS(Main.IsValid(), "Cannot use unpositioned iterators");

            const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);
            Y_DEBUG_ABORT_UNLESS(data->IsDelta(), "Current record is not a delta record");

            data = Main.GetRecord()->GetAltRecord(++SkipMainDeltas);
            if (!data) {
                SkipMainDeltas = 0;
                return EReady::Gone;
            }

            return EReady::Data;
        }

        void Apply(TRowState& row,
                   NTable::ITransactionMapSimplePtr committedTransactions,
                   NTable::ITransactionObserverSimplePtr transactionObserver) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(IsValid(), "Attempt to apply an invalid row");

            const auto* data = SkipMainVersion
                ? HistoryState->History.GetRecord()
                : Main.GetRecord()->GetAltRecord(SkipMainDeltas);
            ui32 index = SkipMainVersion ? 0 : SkipMainDeltas;

            if (!SkipMainVersion) {
                const auto& info = Part->Scheme->Groups[0];
                while (data->IsDelta()) {
                    ui64 txId = data->GetDeltaTxId(info);
                    const auto* commitVersion = committedTransactions.Find(txId);
                    // Apply committed deltas
                    if (commitVersion) {
                        transactionObserver.OnApplyCommitted(*commitVersion, txId);
                        if (row.Touch(data->GetRop())) {
                            for (auto& pin : Pinout) {
                                if (!row.IsFinalized(pin.To)) {
                                    Apply(row, pin, data, index);
                                }
                            }
                        }
                        if (row.IsFinalized()) {
                            return;
                        }
                    } else {
                        transactionObserver.OnSkipUncommitted(txId);
                    }
                    // Skip deltas until the row is finalized
                    data = Main.GetRecord()->GetAltRecord(++index);
                    if (!data) {
                        // This key doesn't have non-delta rows
                        return;
                    }
                }
            }

            if (!SkipEraseVersion && data->IsErased()) {
                if (!SkipMainVersion) {
                    const auto& info = Part->Scheme->Groups[0];
                    transactionObserver.OnApplyCommitted(data->GetMaxVersion(info));
                }
                row.Touch(ERowOp::Erase);
                return;
            }

            if (!SkipMainVersion) {
                const auto& info = Part->Scheme->Groups[0];
                transactionObserver.OnApplyCommitted(data->IsVersioned() ? data->GetMinVersion(info) : Part->MinRowVersion);
            }
            if (row.Touch(data->GetRop())) {
                for (auto& pin: Pinout) {
                    if (!row.IsFinalized(pin.To)) {
                        Apply(row, pin, data, index);
                    }
                }
            }
        }

    private:
        Y_FORCE_INLINE void ClearKey() noexcept
        {
            KeyInitialized = false;
            SkipMainDeltas = 0;
            SkipMainVersion = false;
            SkipEraseVersion = false;
        }

        Y_FORCE_INLINE void InitKey() const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(Main.IsValid(), "Attempt to get an invalid key");
            if (KeyInitialized)
                return;

            KeyInitialized = true;

            auto it = Key.begin();
            auto* record = Main.GetRecord();
            for (auto & info : Part->Scheme->Groups[0].ColsKeyData) {
                *it = record->Cell(info);
                ++it;
            }
        }

        void Apply(TRowState& row, TPinout::TPin pin, const NPage::TDataPage::TRecord* mainRecord, ui32 altIndex) const noexcept
        {
            auto& col = SkipMainVersion ? Part->Scheme->HistoryColumns[pin.From] : Part->Scheme->AllColumns[pin.From];

            const NPage::TDataPage::TRecord* data;
            if (col.Group == 0) {
                data = mainRecord;
            } else {
                size_t altIdx = GroupRemap.at(col.Group);
                auto& g = SkipMainVersion ? HistoryState->Groups.at(altIdx) : Groups.at(altIdx);
                TRowId altRowId = SkipMainVersion ? HistoryState->History.GetHistoryRowId() : Main.GetRowId();

                switch (g.Seek(altRowId)) {
                    case EReady::Data:
                        data = g.GetRecord()->GetAltRecord(altIndex);
                        break;
                    case EReady::Page:
                        // Data page is not available (page fault)
                        // Handling it during Seek/Next makes code too complicated,
                        // so we currently cheat by pretending this column has data
                        // in some outer blob that is waiting to be loaded.
                        row.Set(pin.To, TCellOp(ECellOp::Null, ELargeObj::Outer), { } /* no useful data */);
                        return;
                    case EReady::Gone:
                        Y_ABORT("Unexpected failure to find RowId=%" PRIu64 " in group %" PRIu32 "%s",
                                altRowId, col.Group, SkipMainVersion ? "/history" : "");
                }
            }

            Apply(row, pin, data, col);
        }

        void Apply(TRowState& row, TPinout::TPin pin, const NPage::TDataPage::TRecord* data,
                const TPartScheme::TColumn& info) const noexcept
        {
            auto op = data->GetCellOp(info);

            if (op == ECellOp::Empty) {
                Y_ABORT_UNLESS(!info.IsKey(), "Got an absent key cell");
            } else if (op == ELargeObj::Inline) {
                row.Set(pin.To, op, data->Cell(info));
            } else if (op == ELargeObj::Extern || op == ELargeObj::Outer) {
                const auto ref = data->Cell(info).AsValue<ui64>();

                if (ref >> (sizeof(ui32) * 8))
                    Y_ABORT("Upper bits of ELargeObj ref now isn't used");

                if (auto blob = Env->Locate(Part, ref, op)) {
                    const auto got = NPage::TLabelWrapper().Read(**blob);

                    Y_ABORT_UNLESS(got == NPage::ECodec::Plain && got.Version == 0);

                    row.Set(pin.To, { ECellOp(op), ELargeObj::Inline }, TCell(*got));
                } else if (op == ELargeObj::Outer) {
                    op = TCellOp(blob.Need ? ECellOp::Null : ECellOp(op), ELargeObj::Outer);

                    row.Set(pin.To, op, { } /* cannot put some useful data */);
                } else {
                    Y_ABORT_UNLESS(ref < (*Part->Blobs)->size(), "out of blobs catalog");

                    op = TCellOp(blob.Need ? ECellOp::Null : ECellOp(op), ELargeObj::GlobId);

                    /* Have to preserve reference to memory with TGlobId until
                        of next iterator alteration method invocation. This is
                        why here direct array of TGlobId is used.
                    */
                    row.Set(pin.To, op, TCell::Make((**Part->Blobs)[ref]));
                }
            } else {
                Y_ABORT("Got an unknown blob placement reference type");
            }
        }

    public:
        const TPart* const Part;
        IPages* const Env;

    private:
        const TPinout Pinout;
        const TIntrusiveConstPtr<TKeyCellDefaults> KeyCellDefaults;

        TPartGroupKeyIter Main;

        // Groups are lazily positioned so we need them mutable
        TSmallVec<ui32> GroupRemap;
        mutable TSmallVec<TPartGroupRowIter> Groups;

        // Key is lazily initialized so needs to be mutable
        mutable bool KeyInitialized = false;
        mutable TSmallVec<TCell> Key;

        // History state is used when we need to position into history data
        struct THistoryState {
            TPartHistoryIter History;
            mutable TSmallVec<TPartGroupRowIter> Groups;

            THistoryState(const TPart* part, IPages* env, const TPinout& pinout)
                : History(part, env)
            {
                Groups.reserve(pinout.AltGroups().size());

                for (size_t idx : xrange(pinout.AltGroups().size())) {
                    ui32 group = pinout.AltGroups()[idx];
                    TGroupId groupId(group, /* historic */ true);
                    Groups.emplace_back(part, env, groupId);
                }
            }
        };

        TMaybeFail<THistoryState> HistoryState;
        ui32 SkipMainDeltas = 0;
        ui8 SkipMainVersion : 1;
        ui8 SkipEraseVersion : 1;
    };

    class TRunIter final {
    public:
        using TCells = NPage::TCells;

        TRunIter(const TRun& run, TTagsRef tags, TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults, IPages* env)
            : Run(run)
            , Tags(tags)
            , KeyCellDefaults(std::move(keyDefaults))
            , Env(env)
            , Current(Run.end())
        {
            Y_DEBUG_ABORT_UNLESS(!Run.empty(), "Cannot iterate over an empty run");
        }

        EReady Seek(const TCells key, ESeek seek) noexcept
        {
            bool seekToSliceFirstRow = false;
            TRun::const_iterator pos;

            switch (seek) {
                case ESeek::Exact:
                    pos = Run.Find(key);
                    break;

                case ESeek::Lower:
                    if (!key) {
                        pos = Run.begin();
                        seekToSliceFirstRow = true;
                        break;
                    }

                    pos = Run.LowerBound(key);
                    if (pos != Run.end() &&
                        TSlice::CompareSearchKeyFirstKey(key, pos->Slice, *KeyCellDefaults) <= 0)
                    {
                        // key <= FirstKey
                        seekToSliceFirstRow = true;
                    }
                    break;

                case ESeek::Upper:
                    if (!key) {
                        pos = Run.end();
                        break;
                    }

                    pos = Run.UpperBound(key);
                    if (pos != Run.end() &&
                        TSlice::CompareSearchKeyFirstKey(key, pos->Slice, *KeyCellDefaults) < 0)
                    {
                        // key < FirstKey
                        seekToSliceFirstRow = true;
                    }
                    break;

                default:
                    Y_ABORT("Unsupported iterator seek mode");
            }

            if (pos == Run.end()) {
                // Mark iterator as exhausted
                Current = Run.end();
                return EReady::Gone;
            }

            if (Current != pos) {
                Current = pos;
                UpdateCurrent();
            }

            if (!seekToSliceFirstRow) {
                auto ready = CurrentIt->Seek(key, seek);
                if (ready != EReady::Gone) {
                    return ready;
                }

                if (seek == ESeek::Exact) {
                    // Mark iterator as exhausted
                    Current = Run.end();
                    return EReady::Gone;
                }

                if (++Current == Run.end()) {
                    return EReady::Gone;
                }

                // Take the first row of the next slice
                UpdateCurrent();
            }

            return SeekToSliceFirstRow();
        }

        EReady SeekReverse(const TCells key, ESeek seek) noexcept
        {
            bool seekToSliceLastRow = false;
            TRun::const_iterator pos;

            switch (seek) {
                case ESeek::Exact:
                    pos = Run.Find(key);
                    break;

                case ESeek::Lower:
                    if (!key) {
                        seekToSliceLastRow = true;
                        pos = Run.end();
                        --pos;
                        break;
                    }

                    pos = Run.LowerBoundReverse(key);
                    if (pos != Run.end() &&
                        TSlice::CompareLastKeySearchKey(pos->Slice, key, *KeyCellDefaults) <= 0)
                    {
                        // LastKey <= key 
                        seekToSliceLastRow = true;
                    }
                    break;

                case ESeek::Upper:
                    if (!key) {
                        pos = Run.end();
                        break;
                    }

                    pos = Run.UpperBoundReverse(key);
                    if (pos != Run.end() &&
                        TSlice::CompareLastKeySearchKey(pos->Slice, key, *KeyCellDefaults) < 0)
                    {
                        // LastKey < key
                        seekToSliceLastRow = true;
                    }
                    break;

                default:
                    Y_ABORT("Unsupported iterator seek mode");
            }

            if (pos == Run.end()) {
                // Mark iterator as exhausted
                Current = Run.end();
                return EReady::Gone;
            }

            if (Current != pos) {
                Current = pos;
                UpdateCurrent();
            }

            if (!seekToSliceLastRow) {
                auto ready = CurrentIt->SeekReverse(key, seek);
                if (ready != EReady::Gone) {
                    return ready;
                }

                if (seek == ESeek::Exact) {
                    // Mark iterator as exhausted
                    Current = Run.end();
                    return EReady::Gone;
                }

                if (Current == Run.begin()) {
                    Current = Run.end();
                    return EReady::Gone;
                }

                // Take the first row of the previous slice
                --Current;
                UpdateCurrent();
            }

            return SeekToSliceLastRow();
        }

        EReady Next() noexcept
        {
            if (Y_UNLIKELY(Current == Run.end())) {
                // Calling Next on an exhausted iterator (e.g. from tests)
                return EReady::Gone;
            }

            Y_DEBUG_ABORT_UNLESS(CurrentIt, "Unexpected missing iterator for the current slice");

            auto ready = CurrentIt->Next();
            if (ready != EReady::Gone) {
                return ready;
            }

            if (++Current == Run.end()) {
                return EReady::Gone;
            }

            UpdateCurrent();

            ready = SeekToSliceFirstRow();
            if (ready == EReady::Page) {
                // we haven't sought start, will do it again later  
                Current--;
                UpdateCurrent();
            }

            return ready;
        }

        EReady Prev() noexcept
        {
            if (Y_UNLIKELY(Current == Run.end())) {
                // Calling Prev on an exhausted iterator (e.g. from tests)
                return EReady::Gone;
            }

            Y_DEBUG_ABORT_UNLESS(CurrentIt, "Unexpected missing iterator for the current slice");

            auto ready = CurrentIt->Prev();
            if (ready != EReady::Gone) {
                return ready;
            }

            if (Current == Run.begin()) {
                Current = Run.end();
                return EReady::Gone;
            }

            --Current;
            UpdateCurrent();

            ready = SeekToSliceLastRow();
            if (ready == EReady::Page) {
                // we haven't sought end, will do it again later  
                Current++;
                UpdateCurrent();
            }

            return ready;
        }

        bool IsValid() const noexcept
        {
            if (Current != Run.end()) {
                Y_DEBUG_ABORT_UNLESS(CurrentIt, "Unexpected missing iterator for the current slice");
                return CurrentIt->IsValid();
            }
            return false;
        }

        const TPart* Part() const noexcept
        {
            return CurrentIt ? CurrentIt->Part : nullptr;
        }

        TEpoch Epoch() const noexcept
        {
            return CurrentIt ? CurrentIt->Part->Epoch : TEpoch::Max();
        }

        TRowId GetRowId() const noexcept
        {
            return CurrentIt ? CurrentIt->GetRowId() : Max<TRowId>();
        }

        TDbTupleRef GetKey() const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt);
            return CurrentIt->GetKey();
        }

        const TSharedData& GetKeyPage() const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt);
            return CurrentIt->GetKeyPage();
        }

        void Apply(TRowState& row,
                   NTable::ITransactionMapSimplePtr committedTransactions,
                   NTable::ITransactionObserverSimplePtr transactionObserver) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt);
            CurrentIt->Apply(row, committedTransactions, transactionObserver);
        }

        TRowVersion GetRowVersion() const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt);
            return CurrentIt->GetRowVersion();
        }

        EReady SkipToRowVersion(TRowVersion rowVersion, TIteratorStats& stats,
                                NTable::ITransactionMapSimplePtr committedTransactions,
                                NTable::ITransactionObserverSimplePtr transactionObserver,
                                const NTable::ITransactionSet& decidedTransactions) noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt);
            auto ready = CurrentIt->SkipToRowVersion(rowVersion, stats, committedTransactions, transactionObserver, decidedTransactions);
            return ready;
        }

        bool IsDelta() const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt);
            return CurrentIt->IsDelta();
        }

        ui64 GetDeltaTxId() const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt);
            return CurrentIt->GetDeltaTxId();
        }

        void ApplyDelta(TRowState& row) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt);
            return CurrentIt->ApplyDelta(row);
        }

        EReady SkipDelta() noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt);
            return CurrentIt->SkipDelta();
        }

    private:
        Y_FORCE_INLINE void InitCurrent() noexcept
        {
            const auto* part = Current->Part.Get();
            auto it = Cache.find(part);
            if (it != Cache.end()) {
                CurrentIt = std::move(it->second);
                Cache.erase(it);
            } else {
                CurrentIt = MakeHolder<TPartIter>(part, Tags, KeyCellDefaults, Env);
            }
            CurrentIt->SetBounds(Current->Slice);
        }

        Y_FORCE_INLINE void DropCurrent() noexcept
        {
            Y_DEBUG_ABORT_UNLESS(CurrentIt, "Dropping non-existant current iterator");
            const auto* part = CurrentIt->Part;
            Cache[part] = std::move(CurrentIt);
        }

        Y_FORCE_INLINE void UpdateCurrent() noexcept
        {
            if (CurrentIt) {
                if (CurrentIt->Part == Current->Part.Get()) {
                    // Avoid expensive hash map operations
                    CurrentIt->SetBounds(Current->Slice);
                    return;
                }

                DropCurrent();
            }

            InitCurrent();
        }

        Y_FORCE_INLINE EReady SeekToSliceFirstRow() noexcept
        {
            auto ready = CurrentIt->SeekToSliceFirstRow();
            Y_ABORT_UNLESS(ready != EReady::Gone,
                "Unexpected slice without the first row");
            return ready;
        }

        Y_FORCE_INLINE EReady SeekToSliceLastRow() noexcept
        {
            auto ready = CurrentIt->SeekToSliceLastRow();
            Y_ABORT_UNLESS(ready != EReady::Gone,
                "Unexpected slice without the last row");
            return ready;
        }

    public:
        const TRun& Run;
        TTagsRef const Tags;
        TIntrusiveConstPtr<TKeyCellDefaults> const KeyCellDefaults;
        IPages* const Env;

    private:
        TRun::const_iterator Current;
        THolder<TPartIter> CurrentIt;
        THashMap<const TPart*, THolder<TPartIter>> Cache;
    };

}
}
