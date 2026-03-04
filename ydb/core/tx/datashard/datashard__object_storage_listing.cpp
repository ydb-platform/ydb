#include "datashard_impl.h"
#include <util/string/vector.h>

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

class TDataShard::TTxObjectStorageListing : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    // This is the same timeout as in RpcObjectStorage's handler.
    // We don't need to run this Tx for a longer time than Rpc handler waits.
    static constexpr TDuration MAX_TIMEOUT = TDuration::Minutes(5);
    static constexpr ui64 PRECHARGE_ITEMS = Max<ui64>(); // no limit on items to precharge in order to skip big hunks of erased rows, limit only by bytes
    static constexpr ui64 PRECHARGE_BYTES = 50_MB; // we want to avoid precharging too much, but object storages usually have lots of rows, so we need to precharge at least some reasonable amount of data

    TEvDataShard::TEvObjectStorageListingRequest::TPtr Ev;
    TAutoPtr<TEvDataShard::TEvObjectStorageListingResponse> Result;
    NWilson::TSpan ListingSpan;

    // Used to continue iteration from last known position instead of restarting from the beginning
    // This greatly improves performance for the cases with many deletion markers but sacrifices
    // consitency within the shard. This in not a big deal because listings are not consistent across shards.
    TString LastPath;
    TString LastCommonPath;
    bool LastProcessedKeyErased = false;
    ui32 RestartCount;
    bool StartTsInitialized = false;
    TMonotonic StartTs;

    // Stats for current Execute() call.
    struct TIterationStats {
        ui64 ScannedRows = 0;
        ui64 LeafRows = 0;
        ui64 ErasedRows = 0;
        ui64 FilteredOutRows = 0;
        ui64 SkipToCount = 0;
        bool SkipToFailed = false;
        ui64 DeletedRowSkips = 0;
        bool Advanced = false;
    };

public:
    TTxObjectStorageListing(TDataShard* ds, TEvDataShard::TEvObjectStorageListingRequest::TPtr ev, NWilson::TSpan &&listingSpan)
        : TBase(ds, listingSpan.GetTraceId())
        , Ev(ev)
        , ListingSpan(std::move(listingSpan))
        , RestartCount(0)
    {}

    TTxType GetTxType() const override { return TXTYPE_S3_LISTING; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        ++RestartCount;

        NWilson::TSpan execSpan;
        if (txc.TransactionExecutionSpan) {
            execSpan = NWilson::TSpan(
                TWilsonTablet::TabletDetailed,
                txc.TransactionExecutionSpan.GetTraceId(),
                "Datashard.ObjectStorageListing.Execute",
                NWilson::EFlags::AUTO_END);
            if (execSpan && RestartCount > 1) {
                execSpan.Attribute("Restart", std::to_string(RestartCount));
            }
        }

        TIterationStats stats;

        const auto now = AppData(ctx)->MonotonicTimeProvider->Now();
        if (!StartTsInitialized) {
            StartTsInitialized = true;
            StartTs = now;
        } else if (now - StartTs >= MAX_TIMEOUT) {
            SetError(execSpan, stats, NKikimrTxDataShard::TError::EXECUTION_CANCELLED, "Request timed out");
            return true;
        }

        if (!Result) {
            Result = new TEvDataShard::TEvObjectStorageListingResponse(Self->TabletID());
        }

        if (Self->State != TShardState::Ready &&
            Self->State != TShardState::Readonly &&
            Self->State != TShardState::SplitSrcWaitForNoTxInFlight &&
            Self->State != TShardState::Frozen) {
            TString errorReason = Sprintf("Wrong shard state: %" PRIu32 " tablet id: %" PRIu64, Self->State, Self->TabletID());
            SetError(execSpan, stats, NKikimrTxDataShard::TError::WRONG_SHARD_STATE, errorReason);
            return true;
        }

        const ui64 tableId = Ev->Get()->Record.GetTableId();
        const ui64 maxKeys = Ev->Get()->Record.GetMaxKeys();

        if (!Self->TableInfos.contains(tableId)) {
            TString errorReason = Sprintf("Unknown table id %" PRIu64, tableId);
            SetError(execSpan, stats, NKikimrTxDataShard::TError::SCHEME_ERROR, errorReason);
            return true;
        }

        const TUserTable& tableInfo = *Self->TableInfos[tableId];
        if (tableInfo.IsBackup) {
            SetError(execSpan, stats, NKikimrTxDataShard::TError::SCHEME_ERROR, "Cannot read from a backup table");
            return true;
        }

        const ui32 localTableId = tableInfo.LocalTid;

        TVector<TRawTypeValue> key;
        TVector<TRawTypeValue> endKey;
        bool endKeyInclusive = true;

        // TODO: check prefix column count against key column count
        const TSerializedCellVec prefixColumns(Ev->Get()->Record.GetSerializedKeyPrefix());
        for (ui32 ki = 0; ki < prefixColumns.GetCells().size(); ++ki) {
            // TODO: check prefix column type
            auto &cell = prefixColumns.GetCells()[ki];
            NScheme::TTypeId type = tableInfo.KeyColumnTypes[ki].GetTypeId();
            key.emplace_back(cell.Data(), cell.Size(), type);
            endKey.emplace_back(cell.Data(), cell.Size(), type);
        }
        const ui32 pathColPos = prefixColumns.GetCells().size();

        size_t columnCount = txc.DB.GetScheme().GetTableInfo(localTableId)->KeyColumns.size();

        // TODO: check path column is present in schema and has Utf8 type
        const TString pathPrefix = Ev->Get()->Record.GetPathColumnPrefix();
        const TString pathSeparator = Ev->Get()->Record.GetPathColumnDelimiter();

        TString startAfterPath;
        bool minKeyInclusive = false;
        TSerializedCellVec suffixColumns;
        if (Ev->Get()->Record.GetSerializedStartAfterKeySuffix().empty()) {
            if (Ev->Get()->Record.HasLastPath()) {
                TString reqLastPath = Ev->Get()->Record.GetLastPath();

                key.emplace_back(reqLastPath, NScheme::NTypeIds::Utf8);

                startAfterPath = reqLastPath;
            } else {
                minKeyInclusive = true;
                key.emplace_back(pathPrefix.data(), pathPrefix.size(), NScheme::NTypeIds::Utf8);
                key.resize(columnCount);
            }
        } else {
            suffixColumns.Parse(Ev->Get()->Record.GetSerializedStartAfterKeySuffix());
            size_t prefixSize = prefixColumns.GetCells().size();

            if (Ev->Get()->Record.HasLastPath()) {
                TString reqLastPath = Ev->Get()->Record.GetLastPath();
                
                key.emplace_back(reqLastPath, tableInfo.KeyColumnTypes[prefixSize].GetTypeId());

                for (size_t i = 1; i < suffixColumns.GetCells().size(); ++i) {
                    size_t ki = prefixSize + i;
                    key.emplace_back(suffixColumns.GetCells()[i].Data(), suffixColumns.GetCells()[i].Size(), tableInfo.KeyColumnTypes[ki].GetTypeId());
                }
                
                startAfterPath = reqLastPath;
            } else {
                for (size_t i = 0; i < suffixColumns.GetCells().size(); ++i) {
                    size_t ki = prefixSize + i;
                    key.emplace_back(suffixColumns.GetCells()[i].Data(), suffixColumns.GetCells()[i].Size(), tableInfo.KeyColumnTypes[ki].GetTypeId());
                }
                startAfterPath = TString(suffixColumns.GetCells()[0].Data(), suffixColumns.GetCells()[0].Size());
            }
        }

        TString lastCommonPath; // we will skip a common prefix iff it has been already returned from the prevoius shard
        if (Ev->Get()->Record.HasLastCommonPrefix()) {
            lastCommonPath = Ev->Get()->Record.GetLastCommonPrefix();
        }

        // If this trasaction has restarted we want to continue from the last seen key
        if (LastPath) {
            const size_t pathColIdx =  prefixColumns.GetCells().size();
            key.resize(pathColIdx);
            key.emplace_back(LastPath.data(), LastPath.size(), NScheme::NTypeIds::Utf8);
            key.resize(columnCount);

            lastCommonPath = LastCommonPath;
        } else {
            LastCommonPath = lastCommonPath;
        }

        const TString pathEndPrefix = NextPrefix(pathPrefix);
        if (pathEndPrefix) {
            endKey.emplace_back(pathEndPrefix.data(), pathEndPrefix.size(), NScheme::NTypeIds::Utf8);
            while (endKey.size() < tableInfo.KeyColumnTypes.size()) {
                endKey.emplace_back();
            }
            endKeyInclusive = false;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " S3 Listing: start at key ("
            << JoinVectorIntoString(key, " ") << "), end at key (" << JoinVectorIntoString(endKey, " ") << ")"
            << " restarted: " << RestartCount-1 << " last path: \"" << LastPath << "\""
            << " contents: " << Result->Record.ContentsRowsSize()
            << " common prefixes: " << Result->Record.CommonPrefixesRowsSize());

        Result->Record.SetMoreRows(!IsKeyInRange(endKey, tableInfo));

        if (!maxKeys) {
            // Nothing to return, don't bother searching
            FillSpan(execSpan, stats);
            return true;
        }

        // Select path column and all user-requested columns
        const TVector<ui32> columnsToReturn(Ev->Get()->Record.GetColumnsToReturn().begin(), Ev->Get()->Record.GetColumnsToReturn().end());

        NTable::TKeyRange keyRange;
        keyRange.MinKey = key;
        keyRange.MinInclusive = minKeyInclusive;
        keyRange.MaxKey = endKey;
        keyRange.MaxInclusive = endKeyInclusive;

        if (LastPath) {
            // Don't include the last key in case of restart
            // Include last key if it was erased to allow erase cache to extend across restarts
            keyRange.MinInclusive = LastProcessedKeyErased;
        }

        bool hasFilter = Ev->Get()->Record.has_filter();
        TSerializedCellVec filterColumnValues;
        TVector<ui32> filterColumnIds;
        TVector<NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType> matchTypes;

        if (hasFilter) {
            const auto& filter = Ev->Get()->Record.filter();

            filterColumnValues.Parse(filter.values());
            for (const auto& colId : filter.columns()) {
                filterColumnIds.push_back(colId);
            }
            
            for (const auto& matchType : filter.matchtypes()) {
                if (!NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType_IsValid(matchType)) {
                    TString errorReason = Sprintf("Unknown match type %" PRIu32, matchType);
                    SetError(execSpan, stats, NKikimrTxDataShard::TError::BAD_ARGUMENT, errorReason);
                    return true;
                }
                matchTypes.push_back(static_cast<NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType>(matchType));
            }
        }

        TAutoPtr<NTable::TTableIter> iter = txc.DB.IterateRange(localTableId, keyRange, columnsToReturn);

        ui64 foundKeys = Result->Record.ContentsRowsSize() + Result->Record.CommonPrefixesRowsSize();
        while (iter->Next(NTable::ENext::Data) == NTable::EReady::Data) {
            // NOTE: We intentionally iterate with ENext::Data instead of ENext::All.
            // This means the iterator only returns visible (non-erased) rows; erased rows are
            // not surfaced as separate items but are accounted for via DeletedRowSkips stats
            // (see the logic below that updates stats.ErasedRows).
            if (iter->Stats.DeletedRowSkips != stats.DeletedRowSkips) {
                stats.ErasedRows += iter->Stats.DeletedRowSkips - stats.DeletedRowSkips;
                stats.DeletedRowSkips = iter->Stats.DeletedRowSkips;
            }
            ++stats.ScannedRows;
            stats.Advanced = true;
            TDbTupleRef currentKey = iter->GetKey();

            // Check all columns that prefix columns are in the current key are equal to the specified values
            Y_VERIFY(currentKey.Cells().size() > prefixColumns.GetCells().size());
            Y_VERIFY_DEBUG(
                0 == CompareTypedCellVectors(
                        prefixColumns.GetCells().data(),
                        currentKey.Cells().data(),
                        currentKey.Types,
                        prefixColumns.GetCells().size()),
                "Unexpected out of range key returned from iterator");

            Y_VERIFY(currentKey.Types[pathColPos].GetTypeId() == NScheme::NTypeIds::Utf8);
            const TCell& pathCell = currentKey.Cells()[pathColPos];
            TString path = TString((const char*)pathCell.Data(), pathCell.Size());

            LastPath = path;
            LastProcessedKeyErased = false;

            // Check that path begins with the specified prefix
            Y_VERIFY_DEBUG(path.StartsWith(pathPrefix),
                "Unexpected out of range key returned from iterator");

            bool isLeafPath = true;
            if (!pathSeparator.empty()) {
                size_t separatorPos = path.find_first_of(pathSeparator, pathPrefix.length());
                if (separatorPos != TString::npos) {
                    path.resize(separatorPos + pathSeparator.length());
                    isLeafPath = false;
                }
            }

            TDbTupleRef value = iter->GetValues();
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " S3 Listing: "
                "\"" << path << "\"" << (isLeafPath ? " -> " + DbgPrintTuple(value, *AppData(ctx)->TypeRegistry) : TString()));

            if (isLeafPath) {
                ++stats.LeafRows;
                Y_VERIFY(value.Cells()[0].Size() >= 1);
                Y_VERIFY(path == TStringBuf((const char*)value.Cells()[0].Data(), value.Cells()[0].Size()),
                    "Path column must be requested at pos 0");

                TString newContentsRow = TSerializedCellVec::Serialize(value.Cells());

                if (Result->Record.GetContentsRows().empty() ||
                    *Result->Record.GetContentsRows().rbegin() != newContentsRow) {

                    if (hasFilter) {
                        bool matches = true;

                        for (size_t i = 0; i < filterColumnIds.size(); i++) {
                            auto &columnId = filterColumnIds[i];

                            Y_VERIFY(columnId < value.Cells().size());

                            NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType matchType = matchTypes[i];

                            switch (matchType) {
                                case NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType_EQUAL:
                                    if (CompareTypedCells(value.Cells()[columnId], filterColumnValues.GetCells()[i], value.Types[columnId]) != 0) {
                                        matches = false;
                                    }
                                    break;
                                case NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType_NOT_EQUAL: {
                                    int cmp = CompareTypedCells(value.Cells()[columnId], filterColumnValues.GetCells()[i], value.Types[columnId]);

                                    if (cmp == 0) {
                                        matches = false;
                                    }

                                    break;
                                }
                            }

                            if (!matches) {
                                break;
                            }
                        }

                        if (!matches) {
                            ++stats.FilteredOutRows;
                            continue;
                        }
                    }
                    
                    // Add a row with path column and all columns requested by user
                    Result->Record.AddContentsRows(newContentsRow);
                    if (++foundKeys >= maxKeys) {
                        break;
                    }
                }
            } else {
                if (hasFilter) {
                    bool matches = true;

                    for (size_t i = 0; i < filterColumnIds.size(); i++) {
                        auto &columnId = filterColumnIds[i];

                        Y_VERIFY(columnId < value.Cells().size());

                        NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType matchType;
                        
                        if (matchTypes.size() == filterColumnIds.size()) {
                            matchType = matchTypes[i];
                        } else {
                            matchType = NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType_EQUAL;
                        }

                        switch (matchType) {
                            case NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType_EQUAL:
                                if (CompareTypedCells(value.Cells()[columnId], filterColumnValues.GetCells()[i], value.Types[columnId]) != 0) {
                                    matches = false;
                                }
                                break;
                            case NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType_NOT_EQUAL: {
                                int cmp = CompareTypedCells(value.Cells()[columnId], filterColumnValues.GetCells()[i], value.Types[columnId]);

                                if (cmp == 0) {
                                    matches = false;
                                }

                                break;
                            }
                        }

                        if (!matches) {
                            break;
                        }
                    }

                    if (!matches) {
                        ++stats.FilteredOutRows;
                        continue;
                    }
                }
                
                // For prefix save only path
                if (path > startAfterPath && path != lastCommonPath) {
                    LastCommonPath = path;
                    Result->Record.AddCommonPrefixesRows(path);
                    if (++foundKeys >= maxKeys)
                        break;
                }

                TString lookup = NextPrefix(path);
                if (!lookup) {
                    // May only happen if path is equal to separator, which consists of only '\xff'
                    // This would imply separator is not a valid UTF-8 string, but in any case no
                    // other path exists after the current prefix.
                    break;
                }

                // Skip to the next key after path+separator
                key.resize(prefixColumns.GetCells().size());
                key.emplace_back(lookup.data(), lookup.size(), NScheme::NTypeIds::Utf8);
                key.resize(columnCount);

                ++stats.SkipToCount;
                if (!iter->SkipTo(key, /* inclusive = */ true)) {
                    stats.SkipToFailed = true;
                    FillSpan(execSpan, stats);
                    return false;
                }
            }
        }

        if (iter->Stats.DeletedRowSkips != stats.DeletedRowSkips) {
            stats.ErasedRows += iter->Stats.DeletedRowSkips - stats.DeletedRowSkips;
            stats.DeletedRowSkips = iter->Stats.DeletedRowSkips;
        }

        if (iter->Last() == NTable::EReady::Page) {
            auto lastKeyCells = iter->GetKey().Cells();
            // Same as in DataShard ReadIterator.
            if (lastKeyCells && (stats.Advanced || iter->Stats.DeletedRowSkips >= 4)) {
                Y_VERIFY(lastKeyCells.size() > pathColPos);
                Y_VERIFY(iter->GetKey().Types[pathColPos].GetTypeId() == NScheme::NTypeIds::Utf8);
                const TCell& pathCell = lastKeyCells[pathColPos];
                LastPath = TString((const char*)pathCell.Data(), pathCell.Size());
                LastProcessedKeyErased = (iter->GetKeyState() == NTable::ERowOp::Erase);
            }

            if (lastKeyCells) {
                TVector<TRawTypeValue> prechargeMinKey = ToRawTypeValue(lastKeyCells, tableInfo, false);
                txc.DB.Precharge(localTableId, prechargeMinKey, keyRange.MaxKey,
                    columnsToReturn, 0, PRECHARGE_ITEMS, PRECHARGE_BYTES);
            } else {
                txc.DB.Precharge(localTableId, keyRange.MinKey, keyRange.MaxKey,
                    columnsToReturn, 0, PRECHARGE_ITEMS, PRECHARGE_BYTES);
            }
        }

        FillSpan(execSpan, stats);
        return iter->Last() != NTable::EReady::Page;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " S3 Listing: finished "
                    << " status: " << Result->Record.GetStatus()
                    << " description: \"" << Result->Record.GetErrorDescription() << "\""
                    << " contents: " << Result->Record.ContentsRowsSize()
                    << " common prefixes: " << Result->Record.CommonPrefixesRowsSize());
        ctx.Send(Ev->Sender, Result.Release());

        if (ListingSpan) {
            // If there was an error, it was already reported in SetError.
            ListingSpan.EndOk();
        }
    }

private:
    static TVector<TRawTypeValue> ToRawTypeValue(
        TArrayRef<const TCell> keyCells,
        const TUserTable& tableInfo,
        bool addNulls)
    {
        TVector<TRawTypeValue> result;
        result.reserve(keyCells.size());

        for (ui32 i = 0; i < keyCells.size(); ++i) {
            result.emplace_back(keyCells[i].AsRef(), tableInfo.KeyColumnTypes[i].GetTypeId());
        }

        if (addNulls) {
            result.resize(tableInfo.KeyColumnTypes.size());
        }

        return result;
    }

    void FillSpan(NWilson::TSpan& execSpan, TIterationStats& stats, const TString& errorReason = "") {
        if (!execSpan) {
            return;
        }
        execSpan.Attribute("Shard", std::to_string(this->Self->TabletID()));
        execSpan.Attribute("ScannedRows", std::to_string(stats.ScannedRows));
        execSpan.Attribute("LeafRows", std::to_string(stats.LeafRows));
        execSpan.Attribute("ErasedRows", std::to_string(stats.ErasedRows));
        execSpan.Attribute("FilteredOutRows", std::to_string(stats.FilteredOutRows));
        execSpan.Attribute("SkipToCount", std::to_string(stats.SkipToCount));
        execSpan.Attribute("SkipToFailed", std::to_string(stats.SkipToFailed));
        if (errorReason) {
            execSpan.EndError(errorReason);
        } else {
            execSpan.EndOk();
        }
    }

    void SetError(NWilson::TSpan& execSpan, TIterationStats& stats, ui32 status, TString descr) {
        Result = new TEvDataShard::TEvObjectStorageListingResponse(Self->TabletID());

        Result->Record.SetStatus(status);
        Result->Record.SetErrorDescription(descr);

        FillSpan(execSpan, stats, descr);

        if (ListingSpan) {
            ListingSpan.EndError(descr);
        }
    }

    static bool IsKeyInRange(TArrayRef<const TRawTypeValue> key, const TUserTable& tableInfo) {
        if (!key) {
            return false;
        }
        auto range = tableInfo.GetTableRange();
        size_t prefixSize = Min(key.size(), range.To.size());
        for (size_t pos = 0; pos < prefixSize; ++pos) {
            if (int cmp = CompareTypedCells(TCell(&key[pos]), range.To[pos], tableInfo.KeyColumnTypes[pos])) {
                return cmp < 0;
            }
        }
        if (key.size() != range.To.size()) {
            return key.size() > range.To.size();
        }
        return range.InclusiveTo;
    }

    /**
     * Given a prefix p will return the first prefix p' that is
     * lexicographically after all strings that have prefix p.
     * Will return an empty string if prefix p' does not exist.
     */
    static TString NextPrefix(TString p) {
        while (p) {
            if (char next = (char)(((unsigned char)p.back()) + 1)) {
                p.back() = next;
                break;
            } else {
                p.pop_back(); // overflow, move to the next character
            }
        }

        return p;
    }
};

void TDataShard::Handle(TEvDataShard::TEvObjectStorageListingRequest::TPtr& ev, const TActorContext& ctx) {
    NWilson::TSpan listingSpan;

    if (ev->TraceId) {
        listingSpan = NWilson::TSpan(TWilsonTablet::TabletTopLevel, std::move(ev->TraceId), "Datashard.ObjectStorageListing", NWilson::EFlags::AUTO_END);
        if (listingSpan) {
            listingSpan.Attribute("Shard", std::to_string(TabletID()));
        }
    }

    Executor()->Execute(new TTxObjectStorageListing(this, ev, std::move(listingSpan)), ctx);
}

} // namespace NDataShard
} // namespace NKikimr
