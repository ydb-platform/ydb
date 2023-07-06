#pragma once

#include "flat_part_iface.h"
#include "flat_table_part.h"
#include "flat_row_eggs.h"
#include "flat_row_state.h"
#include "flat_part_pinout.h"
#include "flat_part_slice.h"
#include "flat_table_committed.h"

namespace NKikimr {
namespace NTable {

    /**
     * Part group iterator that may be positioned on a specific row id
     */
    class TPartGroupRowIt {
    public:
        TPartGroupRowIt(const NPage::TIndex& indexRef, NPage::TGroupId groupId)
            : IndexRef(indexRef)
            , GroupId(groupId)
            , Index(IndexRef->Begin())
        {
            Y_VERIFY(Index, "Unexpected failure to find the first index record");
        }

        TPartGroupRowIt(const TPart* part, NPage::TGroupId groupId)
            : TPartGroupRowIt(part->GetGroupIndex(groupId), groupId)
        { }

        EReady Seek(TRowId rowId, const TPart* part, IPages* env) noexcept {
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
                        Y_VERIFY(Data, "Unexpected failure to find column group record for RowId=%lu", rowId);
                        return EReady::Data;
                    }
                }
            }

            Data = { };

            // Make sure we're at the correct index position first
            if (!(Index = IndexRef.LookupRow(rowId, Index))) {
                return EReady::Gone;
            }

            // Make sure we have the correct data page loaded
            if (!LoadPage(Index->GetPageId(), Index->GetRowId(), part, env)) {
                return EReady::Page;
            }
            Y_VERIFY_DEBUG(Page.BaseRow() <= rowId, "Index and row have an unexpected relation");

            Data = Page->Begin() + (rowId - Page.BaseRow());
            Y_VERIFY(Data, "Unexpected failure to find record for RowId=%lu", rowId);
            return EReady::Data;
        }

        Y_FORCE_INLINE bool IsValid() const noexcept {
            return bool(Data);
        }

        Y_FORCE_INLINE TRowId GetRowId() const noexcept {
            return IsValid() ? (Page.BaseRow() + Data.Off()) : Max<TRowId>();
        }

        const NPage::TDataPage::TRecord* GetRecord() const noexcept {
            Y_VERIFY_DEBUG(IsValid(), "Use of unpositioned iterator");
            return Data.GetRecord();
        }

    protected:
        bool LoadPage(TPageId pageId, TRowId baseRow, const TPart* part, IPages* env) noexcept
        {
            if (PageId != pageId) {
                Data = { };
                if (!Page.Set(env->TryGetPage(part, pageId, GroupId))) {
                    PageId = Max<TPageId>();
                    return false;
                }
                PageId = pageId;
            }
            Y_VERIFY_DEBUG(Page.BaseRow() == baseRow, "Index and data are out of sync");
            return true;
        }

    protected:
        const NPage::TIndex& IndexRef;
        const NPage::TGroupId GroupId;
        TPageId PageId = Max<TPageId>();

        NPage::TIndex::TIter Index;
        NPage::TDataPage Page;
        NPage::TDataPage::TIter Data;
    };

    /**
     * Part group iterator that may be positioned on a specific key
     */
    class TPartGroupKeyIt
        : private TPartGroupRowIt
    {
    public:
        using TCells = NPage::TCells;

        TPartGroupKeyIt(const NPage::TIndex& indexRef, NPage::TGroupId groupId)
            : TPartGroupRowIt(indexRef, groupId)
        {
            ClearBounds();
        }

        void ClearBounds() noexcept
        {
            BeginRowId = 0;
            EndRowId = IndexRef.GetEndRowId();
            RowId = Max<TRowId>();
        }

        void SetBounds(TRowId beginRowId, TRowId endRowId) noexcept
        {
            BeginRowId = beginRowId;
            EndRowId = Min(endRowId, IndexRef.GetEndRowId());
            Y_VERIFY_DEBUG(BeginRowId < EndRowId,
                "Trying to iterate over empty bounds=[%lu,%lu)", BeginRowId, EndRowId);
            RowId = Max<TRowId>();
        }

        EReady Seek(
                const TCells key, ESeek seek,
                const TPart* part, IPages* env,
                const TPartScheme::TGroupInfo& scheme, const TKeyCellDefaults* keyDefaults) noexcept
        {
            Y_VERIFY_DEBUG(seek == ESeek::Exact || seek == ESeek::Lower || seek == ESeek::Upper,
                    "Only ESeek{Exact, Upper, Lower} are currently supported here");

            if (Index = IndexRef.LookupKey(key, scheme, seek, keyDefaults)) {
                if (Index->GetRowId() >= EndRowId) {
                    // Page is outside of bounds
                    return Exhausted();
                }

                if (Index->GetRowId() < BeginRowId) {
                    // Find an index record that has BeginRowId
                    if (seek == ESeek::Exact) {
                        auto next = Index + 1;
                        if (next && next->GetRowId() <= BeginRowId) {
                            // We cannot return any other row, don't even try
                            return Exhausted();
                        }
                    } else {
                        SeekIndex(BeginRowId);
                    }
                }

                if (!LoadPage(Index->GetPageId(), Index->GetRowId(), part, env)) {
                    // Exact RowId unknown, don't allow Next until another Seek
                    RowId = Max<TRowId>();
                    return EReady::Page;
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
                        Y_VERIFY_DEBUG(Data, "Unexpected failure to find BeginRowId=%lu", BeginRowId);
                    }

                    return EReady::Data;
                }

                if (seek != ESeek::Exact && ++Index) {
                    // The row we seek is on the next page
                    RowId = Index->GetRowId();
                    Y_VERIFY_DEBUG(RowId > BeginRowId,
                        "Unexpected next page RowId=%lu (BeginRowId=%lu)",
                        RowId, BeginRowId);

                    if (RowId < EndRowId) {
                        return Next(part, env);
                    }
                }
            }

            return Exhausted();
        }

        EReady SeekReverse(
                const TCells key, ESeek seek,
                const TPart* part, IPages* env,
                const TPartScheme::TGroupInfo& scheme, const TKeyCellDefaults* keyDefaults) noexcept
        {
            Y_VERIFY_DEBUG(seek == ESeek::Exact || seek == ESeek::Lower || seek == ESeek::Upper,
                    "Only ESeek{Exact, Upper, Lower} are currently supported here");

            if (Index = IndexRef.LookupKeyReverse(key, scheme, seek, keyDefaults)) {
                if (Index->GetRowId() < BeginRowId) {
                    // Page may be outside of bounds
                    auto next = Index + 1;
                    if (next && next->GetRowId() <= BeginRowId) {
                        // All rows are outside of bounds
                        return Exhausted();
                    }
                }

                if (EndRowId <= Index->GetRowId()) {
                    // Find an index record that has EndRowId - 1
                    SeekIndex(EndRowId - 1);
                }

                if (!LoadPage(Index->GetPageId(), Index->GetRowId(), part, env)) {
                    // Exact RowId unknown, don't allow Next until another Seek
                    RowId = Max<TRowId>();
                    return EReady::Page;
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
                        Y_VERIFY_DEBUG(Data.Off() >= diff, "Unexpected failure to find LastRowId=%lu", EndRowId - 1);
                        Data -= diff;
                        RowId = EndRowId - 1;
                    }

                    return EReady::Data;
                }

                if (seek != ESeek::Exact && Index.Off() > 0) {
                    // The row we seek is on the previous page
                    // N.B. actually this should never be triggered,
                    // since reverse search should always have exact==true
                    RowId = Index->GetRowId() - 1;
                    --Index;
                    Y_VERIFY_DEBUG(RowId < EndRowId,
                        "Unexpected prev page RowId=%lu (EndRowId=%lu)",
                        RowId, EndRowId);
                    if (RowId >= BeginRowId) {
                        return Prev(part, env);
                    }
                }
            }

            return Exhausted();
        }

        EReady Seek(TRowId rowId, const TPart* part, IPages* env) noexcept
        {
            if (Y_UNLIKELY(rowId < BeginRowId || rowId >= EndRowId)) {
                return Exhausted();
            }

            SeekIndex(rowId);
            RowId = rowId;
            Data = { };

            return Next(part, env);
        }

        EReady SeekToStart(const TPart* part, IPages* env) noexcept
        {
            return Seek(BeginRowId, part, env);
        }

        EReady SeekReverse(TRowId rowId, const TPart* part, IPages* env) noexcept
        {
            if (Y_UNLIKELY(rowId < BeginRowId || rowId >= EndRowId)) {
                return Exhausted();
            }

            SeekIndex(rowId);
            RowId = rowId;
            Data = { };

            return Prev(part, env);
        }

        EReady SeekToEnd(const TPart* part, IPages* env) noexcept
        {
            return SeekReverse(EndRowId - 1, part, env);
        }

        EReady Next(const TPart* part, IPages* env) noexcept
        {
            if (Y_UNLIKELY(RowId == Max<TRowId>())) {
                return EReady::Gone;
            }

            Y_VERIFY_DEBUG(RowId >= BeginRowId && RowId < EndRowId,
                "Unexpected RowId=%lu outside of iteration bounds [%lu,%lu)",
                RowId, BeginRowId, EndRowId);

            if (Data) {
                if (++RowId >= EndRowId) {
                    return Exhausted();
                }

                if (++Data) {
                    return EReady::Data;
                } else if (!++Index) {
                    return Exhausted();
                }
            }

            Y_VERIFY_DEBUG(Index && Index->GetRowId() <= RowId,
                "Next called without a valid index record");

            if (!LoadPage(Index->GetPageId(), Index->GetRowId(), part, env)) {
                return EReady::Page;
            }

            if (Data = Page->Begin() + (RowId - Page.BaseRow())) {
                return EReady::Data;
            }

            Y_VERIFY(!++Index, "Unexpected failure to seek in a non-final data page");
            return Exhausted();
        }

        EReady Prev(const TPart* part, IPages* env) noexcept
        {
            if (Y_UNLIKELY(RowId == Max<TRowId>())) {
                return EReady::Gone;
            }

            Y_VERIFY_DEBUG(RowId >= BeginRowId && RowId < EndRowId,
                "Unexpected RowId=%lu outside of iteration bounds [%lu,%lu)",
                RowId, BeginRowId, EndRowId);

            if (Data) {
                if (RowId-- <= BeginRowId) {
                    return Exhausted();
                }

                if (Data.Off() == 0) {
                    Data = { };
                    if (Index.Off() == 0) {
                        Index = { };
                        return Exhausted();
                    }
                    --Index;
                } else {
                    --Data;
                    return EReady::Data;
                }
            }

            Y_VERIFY_DEBUG(Index && Index->GetRowId() <= RowId,
                "Prev called without a valid index record");

            if (!LoadPage(Index->GetPageId(), Index->GetRowId(), part, env)) {
                return EReady::Page;
            }

            if (Data = Page->Begin() + (RowId - Page.BaseRow())) {
                return EReady::Data;
            }

            Y_FAIL("Unexpected failure to seek in a non-final data page");
        }

        using TPartGroupRowIt::IsValid;
        using TPartGroupRowIt::GetRecord;

        Y_FORCE_INLINE TRowId GetRowId() const noexcept {
            // N.B. we don't check IsValid because screen-related code may
            // call GetRowId() even after EReady::Page or EReady::Gone.
            return RowId;
        }

    private:
        Y_FORCE_INLINE EReady Exhausted() noexcept
        {
            Data = { };
            RowId = Max<TRowId>();
            return EReady::Gone;
        }

        void SeekIndex(TRowId rowId) noexcept
        {
            Index = IndexRef.LookupRow(rowId, Index);
            Y_VERIFY_DEBUG(Index,
                "Unexpected failure to find index record for RowId=%lu, bounds=[%lu,%lu)",
                rowId, BeginRowId, EndRowId);
        }

    protected:
        TRowId BeginRowId;
        TRowId EndRowId;
        TRowId RowId;
    };

    /**
     * Part group iterator over history data
     */
    class TPartGroupHistoryIt
        : private TPartGroupRowIt
    {
    public:
        using TCells = NPage::TCells;

        explicit TPartGroupHistoryIt(const TPart* part)
            : TPartGroupRowIt(part, NPage::TGroupId(0, /* historic */ true))
        { }

        EReady Seek(
            TRowId rowId, const TRowVersion& rowVersion,
            const TPart* part, IPages* env) noexcept
        {
            Y_VERIFY_DEBUG(rowId != Max<TRowId>());

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
            const auto& scheme = part->Scheme->HistoryGroup;
            Y_VERIFY_DEBUG(scheme.ColsKeyIdx.size() == 3);
            Y_VERIFY_DEBUG(scheme.ColsKeyData.size() == 3);

            // Directly use the histroy key keyDefaults with correct sort order
            const TKeyCellDefaults* keyDefaults = part->Scheme->HistoryKeys.Get();

            // Helper for loading row id and row version from the index
            auto checkIndex = [&]() -> bool {
                RowId = Index->Cell(scheme.ColsKeyIdx[0]).AsValue<TRowId>();
                RowVersion.Step = Index->Cell(scheme.ColsKeyIdx[1]).AsValue<ui64>();
                RowVersion.TxId = Index->Cell(scheme.ColsKeyIdx[2]).AsValue<ui64>();
                return rowId == RowId;
            };

            // Helper for loading row id and row version from the data page
            auto checkData = [&]() -> bool {
                RowId = Data->Cell(scheme.ColsKeyData[0]).AsValue<TRowId>();
                RowVersion.Step = Data->Cell(scheme.ColsKeyData[1]).AsValue<ui64>();
                RowVersion.TxId = Data->Cell(scheme.ColsKeyData[2]).AsValue<ui64>();
                return rowId == RowId;
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
                            if (!++Index) {
                                return Exhausted();
                            } else if (!checkIndex()) {
                                // First row for the next RowId
                                MaxVersion = TRowVersion::Max();
                                Y_VERIFY_DEBUG(rowId < RowId);
                                return EReady::Gone;
                            }

                            // Next page has the same row id
                            break;
                        }

                        if (!checkData()) {
                            // First row for the new RowId
                            MaxVersion = TRowVersion::Max();
                            Y_VERIFY_DEBUG(rowId < RowId);
                            return EReady::Gone;
                        }

                        if (RowVersion <= rowVersion) {
                            // Save an estimate for MaxVersion
                            MaxVersion = rowVersion;
                            return EReady::Data;
                        }

                        // This loop may only terminate with a break
                        Y_VERIFY(linear > 0, "Data binary search bug");
                    }

                    // Save an estimate for MaxVersion
                    MaxVersion = rowVersion;
                } else {
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
                Y_VERIFY_DEBUG(Index);
                Y_VERIFY_DEBUG(!Data);

                if (!LoadPage(Index->GetPageId(), Index->GetRowId(), part, env)) {
                    return EReady::Page;
                }

                Data = Page->Begin();

                Y_VERIFY(checkData() && RowVersion <= rowVersion, "Index and Data are out of sync");

                return EReady::Data;
            }

            // Full binary search
            if (Index = IndexRef.LookupKey(key, scheme, ESeek::Lower, keyDefaults)) {
                // We need exact match on rowId, bail on larger values
                TRowId indexRowId = Index->Cell(scheme.ColsKeyIdx[0]).AsValue<TRowId>();
                if (rowId < indexRowId) {
                    // We cannot compute MaxVersion anyway
                    return Exhausted();
                }

                if (!LoadPage(Index->GetPageId(), Index->GetRowId(), part, env)) {
                    // It's ok to repeat binary search on the next iteration,
                    // since page faults take a long time and optimizing it
                    // wouldn't be worth it.
                    Data = { };
                    RowId = Max<TRowId>();
                    return EReady::Page;
                }

                if (Data = Page.LookupKey(key, scheme, ESeek::Lower, keyDefaults)) {
                    if (!checkData()) {
                        // First row for the next RowId
                        MaxVersion = TRowVersion::Max();
                        Y_VERIFY_DEBUG(rowId < RowId);
                        return EReady::Gone;
                    }

                    Y_VERIFY(RowVersion <= rowVersion, "Data binary search bug");

                    // Save an estimate for MaxVersion
                    MaxVersion = rowVersion;
                    return EReady::Data;
                }

                if (!++Index) {
                    return Exhausted();
                }

                if (!checkIndex()) {
                    // First row for the nextRowId
                    MaxVersion = TRowVersion::Max();
                    Y_VERIFY_DEBUG(rowId < RowId);
                    return EReady::Gone;
                }

                // The above binary search failed, but since we started with
                // an index search the first row must be the one we want.
                Y_VERIFY(RowVersion <= rowVersion, "Index binary search bug");

                if (!LoadPage(Index->GetPageId(), Index->GetRowId(), part, env)) {
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

                Y_VERIFY(Data && checkData() && RowVersion <= rowVersion, "Index and Data are out of sync");

                // Save an estimate for MaxVersion
                MaxVersion = rowVersion;
                return EReady::Data;
            }

            return Exhausted();
        }

        using TPartGroupRowIt::IsValid;
        using TPartGroupRowIt::GetRecord;

        TRowId GetHistoryRowId() const noexcept {
            Y_VERIFY_DEBUG(IsValid());
            return TPartGroupRowIt::GetRowId();
        }

        TRowId GetRowId() const noexcept {
            Y_VERIFY_DEBUG(IsValid());
            return RowId;
        }

        const TRowVersion& GetRowVersion() const noexcept {
            Y_VERIFY_DEBUG(IsValid());
            return RowVersion;
        }

    private:
        Y_FORCE_INLINE EReady Exhausted() {
            Data = { };
            RowId = Max<TRowId>();
            return EReady::Gone;
        }

    private:
        TRowId RowId = Max<TRowId>();
        TRowVersion RowVersion;
        TRowVersion MaxVersion;
    };

    class TPartSimpleIt final {
    public:
        using TCells = NPage::TCells;
        using TGroupId = NPage::TGroupId;

        TPartSimpleIt(const TPart* part, TTagsRef tags, TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults, IPages* env)
            : Part(part)
            , Env(env)
            , Pinout(Part->Scheme->MakePinout(tags))
            , KeyCellDefaults(std::move(keyDefaults))
            , Main(Part->Index, TGroupId(0))
            , SkipMainDeltas(0)
            , SkipMainVersion(false)
            , SkipEraseVersion(false)
        {
            Groups.reserve(Pinout.AltGroups().size());
            GroupRemap.resize(Part->Scheme->Groups.size(), Max<ui32>());

            for (size_t idx : xrange(Pinout.AltGroups().size())) {
                ui32 group = Pinout.AltGroups()[idx];
                TGroupId groupId(group);
                Groups.emplace_back(Part->GetGroupIndex(groupId), groupId);
                GroupRemap[group] = idx;
            }

            size_t KeyCellDefaultsSize = KeyCellDefaults->Size();
            Key.resize(KeyCellDefaultsSize);

            size_t infoSize = Part->Scheme->Groups[0].ColsKeyData.size();
            for (size_t pos = infoSize; pos < KeyCellDefaultsSize; ++pos) {
                Key[pos] = (*KeyCellDefaults)[pos];
            }
        }

        void ClearBounds() noexcept
        {
            Main.ClearBounds();
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
            return Main.Seek(key, seek, Part, Env, Part->Scheme->Groups[0], &*KeyCellDefaults);
        }

        EReady SeekReverse(const TCells key, ESeek seek) noexcept
        {
            ClearKey();
            return Main.SeekReverse(key, seek, Part, Env, Part->Scheme->Groups[0], &*KeyCellDefaults);
        }

        EReady Seek(TRowId rowId) noexcept
        {
            ClearKey();
            return Main.Seek(rowId, Part, Env);
        }

        EReady SeekToStart() noexcept
        {
            ClearKey();
            return Main.SeekToStart(Part, Env);
        }

        EReady SeekReverse(TRowId rowId) noexcept
        {
            ClearKey();
            return Main.SeekReverse(rowId, Part, Env);
        }

        EReady SeekToEnd() noexcept
        {
            ClearKey();
            return Main.SeekToEnd(Part, Env);
        }

        EReady Next() noexcept
        {
            ClearKey();
            return Main.Next(Part, Env);
        }

        EReady Prev() noexcept
        {
            ClearKey();
            return Main.Prev(Part, Env);
        }

        Y_FORCE_INLINE bool IsValid() const noexcept
        {
            if (!SkipMainVersion) {
                return Main.IsValid();
            } else {
                Y_VERIFY_DEBUG(HistoryState, "SkipMainVersion set, but no HistoryState");
                Y_VERIFY_DEBUG(Main.IsValid());
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

        TCells GetRawKey() const noexcept
        {
            InitKey();

            return TCells(Key).Slice(0, Part->Scheme->Groups[0].ColsKeyData.size());
        }

        TRowVersion GetRowVersion() const noexcept
        {
            Y_VERIFY_DEBUG(IsValid(), "Attempt to get invalid row version");

            if (!SkipMainVersion) {
                const auto& info = Part->Scheme->Groups[0];
                const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);
                Y_VERIFY(!data->IsDelta(), "GetRowVersion cannot be called on deltas");

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
                Y_VERIFY_DEBUG(!data->IsVersioned());

                return HistoryState->History.GetRowVersion();
            }
        }

        EReady SkipToRowVersion(TRowVersion rowVersion, TIteratorStats& stats,
                                NTable::ITransactionMapSimplePtr committedTransactions,
                                NTable::ITransactionObserverSimplePtr transactionObserver) noexcept
        {
            Y_VERIFY_DEBUG(Main.IsValid(), "Attempt to use an invalid iterator");

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
                        Y_VERIFY(!data->IsDelta(), "Unexpected delta without TxIdStats");
                        if (!SkipEraseVersion && data->IsErased()) {
                            transactionObserver.OnSkipCommitted(data->GetMaxVersion(info));
                        } else if (data->IsVersioned()) {
                            transactionObserver.OnSkipCommitted(data->GetMinVersion(info));
                        } else {
                            transactionObserver.OnSkipCommitted(Part->MinRowVersion);
                        }
                        stats.InvisibleRowSkips++;
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
                        return EReady::Data;
                    }
                    if (commitVersion) {
                        // Skipping a newer committed delta
                        transactionObserver.OnSkipCommitted(*commitVersion, txId);
                        stats.InvisibleRowSkips++;
                    } else {
                        // Skipping an uncommitted delta
                        transactionObserver.OnSkipUncommitted(txId);
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
                }

                TRowVersion current = data->IsVersioned() ? data->GetMinVersion(info) : Part->MinRowVersion;
                if (current <= rowVersion) {
                    // Already correct version
                    return EReady::Data;
                }

                transactionObserver.OnSkipCommitted(current);
                stats.InvisibleRowSkips++;

                if (!data->HasHistory()) {
                    // There is no history, reset
                    SkipEraseVersion = false;
                    return EReady::Gone;
                }

                if (!HistoryState) {
                    // Initialize history state once per iterator
                    HistoryState.ConstructInPlace(Part, Pinout);
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
                Y_VERIFY_DEBUG(HistoryState->History.GetRowId() == Main.GetRowId());

                // History is already positioned on version that is old enough
                ready = EReady::Data;
            } else {
                // Seek might switch to a different row, must look at erase version
                SkipEraseVersion = false;

                // Find the first row that is as old as rowVersion
                ready = HistoryState->History.Seek(Main.GetRowId(), rowVersion, Part, Env);
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

                    Y_VERIFY_DEBUG(HistoryState->History.GetRowVersion() <= rowVersion);
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

        std::optional<TRowVersion> SkipToCommitted(
                NTable::ITransactionMapSimplePtr committedTransactions,
                NTable::ITransactionObserverSimplePtr transactionObserver) noexcept
        {
            Y_VERIFY_DEBUG(Main.IsValid(), "Attempt to use an invalid iterator");
            Y_VERIFY(!SkipMainVersion, "Cannot use SkipToCommitted after positioning to history");

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

            Y_VERIFY_DEBUG(Main.IsValid(), "Cannot use unpositioned iterators");

            const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);
            return data->IsDelta();
        }

        ui64 GetDeltaTxId() const noexcept
        {
            Y_VERIFY_DEBUG(!SkipMainVersion, "Current record is not a delta record");
            Y_VERIFY_DEBUG(Main.IsValid(), "Cannot use unpositioned iterators");

            const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);
            Y_VERIFY_DEBUG(data->IsDelta(), "Current record is not a delta record");

            const auto& info = Part->Scheme->Groups[0];
            return data->GetDeltaTxId(info);
        }

        void ApplyDelta(TRowState& row) const noexcept
        {
            Y_VERIFY_DEBUG(!SkipMainVersion, "Current record is not a delta record");
            Y_VERIFY_DEBUG(Main.IsValid(), "Cannot use unpositioned iterators");

            const ui32 index = SkipMainDeltas;
            const auto* data = Main.GetRecord()->GetAltRecord(index);
            Y_VERIFY_DEBUG(data->IsDelta(), "Current record is not a delta record");

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
            Y_VERIFY_DEBUG(!SkipMainVersion, "Current record is not a delta record");
            Y_VERIFY_DEBUG(Main.IsValid(), "Cannot use unpositioned iterators");

            const auto* data = Main.GetRecord()->GetAltRecord(SkipMainDeltas);
            Y_VERIFY_DEBUG(data->IsDelta(), "Current record is not a delta record");

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
            Y_VERIFY_DEBUG(IsValid(), "Attempt to apply an invalid row");

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
            Y_VERIFY_DEBUG(Main.IsValid(), "Attempt to get an invalid key");
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

                switch (g.Seek(altRowId, Part, Env)) {
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
                        Y_FAIL("Unexpected failure to find RowId=%" PRIu64 " in group %" PRIu32 "%s",
                                altRowId, col.Group, SkipMainVersion ? "/history" : "");
                }
            }

            Apply(row, pin, data, col);
        }

        void Apply(
                TRowState& row,
                TPinout::TPin pin,
                const NPage::TDataPage::TRecord* data,
                const TPartScheme::TColumn& info) const noexcept
        {
            auto op = data->GetCellOp(info);

            if (op == ECellOp::Empty) {
                Y_VERIFY(!info.IsKey(), "Got an absent key cell");
            } else if (op == ELargeObj::Inline) {
                row.Set(pin.To, op, data->Cell(info));
            } else if (op == ELargeObj::Extern || op == ELargeObj::Outer) {
                const auto ref = data->Cell(info).AsValue<ui64>();

                if (ref >> (sizeof(ui32) * 8))
                    Y_FAIL("Upper bits of ELargeObj ref now isn't used");
                if (auto blob = Env->Locate(Part, ref, op)) {
                    const auto got = NPage::TLabelWrapper().Read(**blob);

                    Y_VERIFY(got == NPage::ECodec::Plain && got.Version == 0);

                    row.Set(pin.To, { ECellOp(op), ELargeObj::Inline }, TCell(*got));
                } else if (op == ELargeObj::Outer) {
                    op = TCellOp(blob.Need ? ECellOp::Null : ECellOp(op), ELargeObj::Outer);

                    row.Set(pin.To, op, { } /* cannot put some useful data */);
                } else {
                    Y_VERIFY(ref < (*Part->Blobs)->size(), "out of blobs catalog");

                    /* Have to preserve reference to memory with TGlobId until
                        of next iterator alteration method invocation. This is
                        why here direct array of TGlobId is used.
                    */

                    op = TCellOp(blob.Need ? ECellOp::Null : ECellOp(op), ELargeObj::GlobId);

                    row.Set(pin.To, op, TCell::Make((**Part->Blobs)[ref]));
                }
            } else {
                Y_FAIL("Got an unknown blob placement reference type");
            }
        }

    public:
        const TPart* const Part;
        IPages* const Env;

    private:
        const TPinout Pinout;
        const TIntrusiveConstPtr<TKeyCellDefaults> KeyCellDefaults;

        TPartGroupKeyIt Main;

        // Groups are lazily positioned so we need them mutable
        TSmallVec<ui32> GroupRemap;
        mutable TSmallVec<TPartGroupRowIt> Groups;

        // Key is lazily initialized so needs to be mutable
        mutable bool KeyInitialized = false;
        mutable TSmallVec<TCell> Key;

        // History state is used when we need to position into history data
        struct THistoryState {
            TPartGroupHistoryIt History;
            mutable TSmallVec<TPartGroupRowIt> Groups;

            THistoryState(const TPart* part, const TPinout& pinout)
                : History(part)
            {
                Groups.reserve(pinout.AltGroups().size());

                for (size_t idx : xrange(pinout.AltGroups().size())) {
                    ui32 group = pinout.AltGroups()[idx];
                    TGroupId groupId(group, /* historic */ true);
                    Groups.emplace_back(part, groupId);
                }
            }
        };

        TMaybeFail<THistoryState> HistoryState;
        ui32 SkipMainDeltas = 0;
        ui8 SkipMainVersion : 1;
        ui8 SkipEraseVersion : 1;
    };

    class TRunIt final {
    public:
        using TCells = NPage::TCells;

        TRunIt(const TRun& run, TTagsRef tags, TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults, IPages* env)
            : Run(run)
            , Tags(tags)
            , KeyCellDefaults(std::move(keyDefaults))
            , Env(env)
            , Current(Run.end())
        {
            Y_VERIFY_DEBUG(!Run.empty(), "Cannot iterate over an empty run");
        }

        EReady Seek(const TCells key, ESeek seek) noexcept
        {
            if (Run.size() == 1) {
                // Avoid overhead of extra key comparisons in a single slice
                Current = Run.begin();

                if (!CurrentIt) {
                    InitCurrent();
                }

                return CurrentIt->Seek(key, seek);
            }

            bool seekToStart = false;
            TRun::const_iterator pos;

            switch (seek) {
                case ESeek::Exact:
                    pos = Run.Find(key);
                    break;

                case ESeek::Lower:
                    if (!key) {
                        pos = Run.begin();
                        seekToStart = true;
                        break;
                    }

                    pos = Run.LowerBound(key);
                    if (pos != Run.end() &&
                        TSlice::CompareSearchKeyFirstKey(key, pos->Slice, *KeyCellDefaults) <= 0)
                    {
                        // Key is at the start of the slice
                        seekToStart = true;
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
                        // Key is at the start of the slice
                        seekToStart = true;
                    }
                    break;

                default:
                    Y_FAIL("Unsupported iterator seek mode");
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

            if (!seekToStart) {
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

            return SeekToStart();
        }

        EReady SeekReverse(const TCells key, ESeek seek) noexcept
        {
            if (Run.size() == 1) {
                // Avoid overhead of extra key comparisons in a single slice
                Current = Run.begin();

                if (!CurrentIt) {
                    InitCurrent();
                }

                return CurrentIt->SeekReverse(key, seek);
            }

            bool seekToEnd = false;
            TRun::const_iterator pos;

            switch (seek) {
                case ESeek::Exact:
                    pos = Run.Find(key);
                    break;

                case ESeek::Lower:
                    if (!key) {
                        seekToEnd = true;
                        pos = Run.end();
                        --pos;
                        break;
                    }

                    pos = Run.LowerBoundReverse(key);
                    if (pos != Run.end() &&
                        TSlice::CompareLastKeySearchKey(pos->Slice, key, *KeyCellDefaults) <= 0)
                    {
                        seekToEnd = true;
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
                        seekToEnd = true;
                    }
                    break;

                default:
                    Y_FAIL("Unsupported iterator seek mode");
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

            if (!seekToEnd) {
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

            return SeekToEnd();
        }

        EReady Next() noexcept
        {
            if (Y_UNLIKELY(Current == Run.end())) {
                // Calling Next on an exhausted iterator (e.g. from tests)
                return EReady::Gone;
            }

            Y_VERIFY_DEBUG(CurrentIt, "Unexpected missing iterator for the current slice");

            auto ready = CurrentIt->Next();
            if (ready != EReady::Gone) {
                return ready;
            }

            if (++Current == Run.end()) {
                return EReady::Gone;
            }

            UpdateCurrent();

            return SeekToStart();
        }

        EReady Prev() noexcept
        {
            if (Y_UNLIKELY(Current == Run.end())) {
                // Calling Prev on an exhausted iterator (e.g. from tests)
                return EReady::Gone;
            }

            Y_VERIFY_DEBUG(CurrentIt, "Unexpected missing iterator for the current slice");

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

            return SeekToEnd();
        }

        bool IsValid() const noexcept
        {
            if (Current != Run.end()) {
                Y_VERIFY_DEBUG(CurrentIt, "Unexpected missing iterator for the current slice");
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
            Y_VERIFY_DEBUG(CurrentIt);
            return CurrentIt->GetKey();
        }

        void Apply(TRowState& row,
                   NTable::ITransactionMapSimplePtr committedTransactions,
                   NTable::ITransactionObserverSimplePtr transactionObserver) const noexcept
        {
            Y_VERIFY_DEBUG(CurrentIt);
            CurrentIt->Apply(row, committedTransactions, transactionObserver);
        }

        TRowVersion GetRowVersion() const noexcept
        {
            Y_VERIFY_DEBUG(CurrentIt);
            return CurrentIt->GetRowVersion();
        }

        EReady SkipToRowVersion(TRowVersion rowVersion, TIteratorStats& stats,
                                NTable::ITransactionMapSimplePtr committedTransactions,
                                NTable::ITransactionObserverSimplePtr transactionObserver) noexcept
        {
            Y_VERIFY_DEBUG(CurrentIt);
            auto ready = CurrentIt->SkipToRowVersion(rowVersion, stats, committedTransactions, transactionObserver);
            return ready;
        }

        bool IsDelta() const noexcept
        {
            Y_VERIFY_DEBUG(CurrentIt);
            return CurrentIt->IsDelta();
        }

        ui64 GetDeltaTxId() const noexcept
        {
            Y_VERIFY_DEBUG(CurrentIt);
            return CurrentIt->GetDeltaTxId();
        }

        void ApplyDelta(TRowState& row) const noexcept
        {
            Y_VERIFY_DEBUG(CurrentIt);
            return CurrentIt->ApplyDelta(row);
        }

        EReady SkipDelta() noexcept
        {
            Y_VERIFY_DEBUG(CurrentIt);
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
                CurrentIt = MakeHolder<TPartSimpleIt>(part, Tags, KeyCellDefaults, Env);
            }
            CurrentIt->SetBounds(Current->Slice);
        }

        Y_FORCE_INLINE void DropCurrent() noexcept
        {
            Y_VERIFY_DEBUG(CurrentIt, "Dropping non-existant current iterator");
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

        Y_FORCE_INLINE EReady SeekToStart() noexcept
        {
            auto ready = CurrentIt->SeekToStart();
            Y_VERIFY(ready != EReady::Gone,
                "Unexpected slice without the first row");
            return ready;
        }

        Y_FORCE_INLINE EReady SeekToEnd() noexcept
        {
            auto ready = CurrentIt->SeekToEnd();
            Y_VERIFY(ready != EReady::Gone,
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
        THolder<TPartSimpleIt> CurrentIt;
        THashMap<const TPart*, THolder<TPartSimpleIt>> Cache;
    };

}
}
