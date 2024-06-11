#include "minikql_engine_host.h"

#include <ydb/core/tablet_flat/flat_dbase_sz_env.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tablet_flat/flat_table_stats.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>
#include <ydb/core/tx/locks/sys_tables.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {
namespace NMiniKQL {

using TScheme = NTable::TScheme;

void ConvertTableKeys(const TScheme& scheme, const TScheme::TTableInfo* tableInfo,
    const TArrayRef<const TCell>& row,  TSmallVec<TRawTypeValue>& key, ui64* keyDataBytes)
{
    ui64 bytes = 0;
    key.reserve(row.size());
    for (size_t keyIdx = 0; keyIdx < row.size(); keyIdx++) {
        const TCell& cell = row[keyIdx];
        ui32 keyCol = tableInfo->KeyColumns[keyIdx];
        NScheme::TTypeInfo vtypeInfo = scheme.GetColumnInfo(tableInfo, keyCol)->PType;
        if (cell.IsNull()) {
            key.emplace_back();
            bytes += 1;
        } else {
            key.emplace_back(cell.Data(), cell.Size(), vtypeInfo);
            bytes += cell.Size();
        }
    }
    if (keyDataBytes)
        *keyDataBytes = bytes;
}

void ConvertTableValues(const TScheme& scheme, const TScheme::TTableInfo* tableInfo, const TArrayRef<const IEngineFlatHost::TUpdateCommand>& commands, TSmallVec<NTable::TUpdateOp>& ops, ui64* valueBytes) {
    ui64 bytes = 0;
    ops.reserve(commands.size());
    for (size_t i = 0; i < commands.size(); i++) {
        const IEngineFlatHost::TUpdateCommand& upd = commands[i];
        Y_ABORT_UNLESS(upd.Operation == TKeyDesc::EColumnOperation::Set);
        auto vtypeinfo = scheme.GetColumnInfo(tableInfo, upd.Column)->PType;
        ops.emplace_back(upd.Column, NTable::ECellOp::Set, upd.Value.IsNull() ? TRawTypeValue() : TRawTypeValue(upd.Value.Data(), upd.Value.Size(), vtypeinfo));
        bytes += upd.Value.IsNull() ? 1 : upd.Value.Size();
    }
    if (valueBytes)
        *valueBytes = bytes;
}

TEngineHost::TEngineHost(NTable::TDatabase& db, TEngineHostCounters& counters, const TEngineHostSettings& settings)
    : Db(db)
    , Scheme(db.GetScheme())
    , Settings(settings)
    , Counters(counters)
{}

ui64 TEngineHost::GetShardId() const {
    return Settings.ShardId;
}

const TScheme::TTableInfo* TEngineHost::GetTableInfo(const TTableId& tableId) const {
    return Scheme.GetTableInfo(LocalTableId(tableId));
}

bool TEngineHost::IsReadonly() const {
    return Settings.IsReadonly;
}
bool TEngineHost::IsValidKey(TKeyDesc& key) const {
    ui64 localTableId = LocalTableId(key.TableId);
    return NMiniKQL::IsValidKey(Scheme, localTableId, key);
}
ui64 TEngineHost::CalculateReadSize(const TVector<const TKeyDesc*>& keys) const {
    auto env = Db.CreateSizeEnv();

    for (const TKeyDesc* ki : keys) {
        DoCalculateReadSize(*ki, env);
    }

    return env.GetSize();
}

void TEngineHost::DoCalculateReadSize(const TKeyDesc& key, NTable::TSizeEnv& env) const {
    if (TSysTables::IsSystemTable(key.TableId))
        return;
    if (key.RowOperation != TKeyDesc::ERowOperation::Read)
        return;
    ui64 localTid = LocalTableId(key.TableId);
    Y_ABORT_UNLESS(localTid, "table not exist");
    const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(localTid);
    TSmallVec<TRawTypeValue> keyFrom;
    TSmallVec<TRawTypeValue> keyTo;
    ConvertKeys(tableInfo, key.Range.From, keyFrom);
    ConvertKeys(tableInfo, key.Range.To, keyTo);

    TSmallVec<NTable::TTag> tags;
    for (const auto& column : key.Columns) {
        if (Y_LIKELY(column.Operation == TKeyDesc::EColumnOperation::Read)) {
            tags.push_back(column.Column);
        }
    }

    Db.CalculateReadSize(env, localTid,
                               keyFrom,
                               keyTo,
                               tags,
                               0,
                               key.RangeLimits.ItemsLimit, key.RangeLimits.BytesLimit,
                               key.Reverse ? NTable::EDirection::Reverse : NTable::EDirection::Forward);
}

ui64 TEngineHost::CalculateResultSize(const TKeyDesc& key) const {
    if (TSysTables::IsSystemTable(key.TableId))
        return 0;

    ui64 localTid = LocalTableId(key.TableId);
    Y_ABORT_UNLESS(localTid, "table not exist");
    if (key.Range.Point) {
        return Db.EstimateRowSize(localTid);
    } else {
        auto env = Db.CreateSizeEnv();
        DoCalculateReadSize(key, env);
        ui64 size = env.GetSize();

        if (key.RangeLimits.ItemsLimit != 0) {
            ui64 rowSize = Db.EstimateRowSize(localTid);
            size = Min(size, rowSize*key.RangeLimits.ItemsLimit);
        }
        if (key.RangeLimits.BytesLimit != 0) {
            size = Min(size, key.RangeLimits.BytesLimit);
        }
        return size;
    }
}

void TEngineHost::PinPages(const TVector<THolder<TKeyDesc>>& keys, ui64 pageFaultCount) {
    ui64 limitMultiplier = 1;
    if (pageFaultCount >= 2) {
        if (pageFaultCount <= 63) {
            limitMultiplier <<= pageFaultCount - 1;
        } else {
            limitMultiplier = Max<ui64>();
        }
    }

    auto adjustLimit = [limitMultiplier](ui64 limit) -> ui64 {
        if (limit >= Max<ui64>() / limitMultiplier) {
            return Max<ui64>();
        } else {
            return limit * limitMultiplier;
        }
    };

    bool ret = true;
    for (const auto& ki : keys) {
        const TKeyDesc& key = *ki;
        if (TSysTables::IsSystemTable(key.TableId))
            continue;

        TSet<TKeyDesc::EColumnOperation> columnOpFilter;
        switch (key.RowOperation) {
            case TKeyDesc::ERowOperation::Read:
                columnOpFilter.insert(TKeyDesc::EColumnOperation::Read);
                break;
            case TKeyDesc::ERowOperation::Update:
            case TKeyDesc::ERowOperation::Erase: {
                if (NeedToReadBeforeWrite(key.TableId)) {
                    columnOpFilter.insert(TKeyDesc::EColumnOperation::Set);
                    columnOpFilter.insert(TKeyDesc::EColumnOperation::InplaceUpdate);
                }
                break;
            }
            default:
                break;
        }

        if (columnOpFilter.empty()) {
            continue;
        }

        ui64 localTid = LocalTableId(key.TableId);
        Y_ABORT_UNLESS(localTid, "table not exist");
        const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(localTid);

        Y_DEBUG_ABORT_UNLESS(!key.Range.IsAmbiguous(tableInfo->KeyColumns.size()),
            "%s", key.Range.IsAmbiguousReason(tableInfo->KeyColumns.size()));

        TSmallVec<TRawTypeValue> keyFrom;
        TSmallVec<TRawTypeValue> keyTo;
        ConvertKeys(tableInfo, key.Range.From, keyFrom);
        ConvertKeys(tableInfo, key.Range.To, keyTo);

        TSmallVec<NTable::TTag> tags;
        for (const auto& column : key.Columns) {
            if (columnOpFilter.contains(column.Operation)) {
                tags.push_back(column.Column);
            }
        }

        bool ready =  Db.Precharge(localTid,
                                   keyFrom,
                                   key.Range.Point ? keyFrom : keyTo,
                                   tags,
                                   Settings.DisableByKeyFilter ? (ui64)NTable::NoByKey : 0,
                                   adjustLimit(key.RangeLimits.ItemsLimit),
                                   adjustLimit(key.RangeLimits.BytesLimit),
                                   key.Reverse ? NTable::EDirection::Reverse : NTable::EDirection::Forward,
                                   GetReadVersion(key.TableId));
        ret &= ready;
    }

    if (!ret)
        throw TNotReadyTabletException();
}

NUdf::TUnboxedValue TEngineHost::SelectRow(const TTableId& tableId, const TArrayRef<const TCell>& row,
    TStructLiteral* columnIds, TOptionalType* returnType, const TReadTarget& readTarget,
    const THolderFactory& holderFactory)
{
    // It is assumed that returnType has form:
    //  optinal<struct<optional<data>,
    //                 ...
    //                 optional<data>>>
    // And that struct type has column tags stored inside it's member names as numbers

    Y_UNUSED(returnType);
    Y_UNUSED(readTarget);

    ui64 localTid = LocalTableId(tableId);
    Y_ABORT_UNLESS(localTid, "table not exist");
    const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(localTid);
    TSmallVec<TRawTypeValue> key;
    ConvertKeys(tableInfo, row, key);

    TSmallVec<NTable::TTag> tags;
    TSmallVec<NTable::TTag> systemColumnTags;
    AnalyzeRowType(columnIds, tags, systemColumnTags);

    TSmallVec<NScheme::TTypeInfo> cellTypes;
    cellTypes.reserve(tags.size());
    for (size_t i = 0; i < tags.size(); ++i)
        cellTypes.emplace_back(tableInfo->Columns.at(tags[i]).PType);

    NTable::TRowState dbRow;

    if (key.size() != Db.GetScheme().GetTableInfo(localTid)->KeyColumns.size())
        throw TSchemeErrorTabletException();

    Counters.NSelectRow++;
    Settings.KeyAccessSampler->AddSample(tableId, row);

    NTable::TSelectStats stats;
    ui64 flags = Settings.DisableByKeyFilter ? (ui64)NTable::NoByKey : 0;
    const auto ready = Db.Select(localTid, key, tags, dbRow, stats, flags, GetReadVersion(tableId),
        GetReadTxMap(tableId), GetReadTxObserver(tableId));

    Counters.InvisibleRowSkips += stats.InvisibleRowSkips;

    if (NTable::EReady::Page == ready) {
        throw TNotReadyTabletException();
    }

    if (NTable::EReady::Gone == ready) {
        return NUdf::TUnboxedValue();
    }

    NUdf::TUnboxedValue* rowItems = nullptr;
    auto rowResult = holderFactory.CreateDirectArrayHolder(tags.size(), rowItems);

    ui64 rowBytes = 0;
    for (ui32 i = 0; i < tags.size(); ++i) {
        rowItems[i] = GetCellValue(dbRow.Get(i), cellTypes[i]);
        rowBytes += dbRow.Get(i).IsNull() ? 1 : dbRow.Get(i).Size();
    }
    for (ui32 i = 0; i < systemColumnTags.size(); ++i) {
        switch (systemColumnTags[i]) {
        case TKeyDesc::EColumnIdDataShard:
            rowItems[tags.size() + i] = NUdf::TUnboxedValuePod(GetShardId());
            break;
        default:
            ythrow yexception() << "Unknown system column tag: " << systemColumnTags[i];
        }
    }
    rowBytes = std::max(rowBytes, (ui64)8);

    Counters.SelectRowBytes += rowBytes;
    Counters.SelectRowRows++;

    return std::move(rowResult);
}

template<class TTableIter>
class TSelectRangeLazyRow : public TComputationValue<TSelectRangeLazyRow<TTableIter>> {
private:
    using TBase = TComputationValue<TSelectRangeLazyRow<TTableIter>>;
    NUdf::TUnboxedValue GetElement(ui32 index) const override {
        BuildValue(index);
        return GetPtr()[index];
    }

public:
    ~TSelectRangeLazyRow() {
        for (ui32 i = 0; i < Size(); ++i) {
            (GetPtr() + i)->~TUnboxedValue();
        }
    }

    void* operator new(size_t sz) = delete;
    void* operator new[](size_t sz) = delete;
    void operator delete(void *mem, std::size_t sz) {
        auto ptr = (TSelectRangeLazyRow*)mem;
        auto extraSize = ptr->Size() * sizeof(NUdf::TUnboxedValue) + ptr->GetMaskSize() * sizeof(ui64);
        TBase::FreeWithSize(mem, sz + extraSize);
    }

    void operator delete[](void *mem, std::size_t sz) = delete;

    static NUdf::TUnboxedValue Create(const TDbTupleRef& dbData, const THolderFactory& holderFactory,
        const TSmallVec<NTable::TTag>& systemColumnTags, ui64 shardId) {
        ui32 size = dbData.ColumnCount + systemColumnTags.size();

        ui32 maskSize = size > 0 ? (size - 1) / 64 + 1 : 0;
        void* buffer = TBase::AllocWithSize(sizeof(TSelectRangeLazyRow)
            + size * sizeof(NUdf::TUnboxedValue)
            + maskSize * sizeof(ui64));

        return NUdf::TUnboxedValuePod(::new(buffer) TSelectRangeLazyRow(dbData, holderFactory, maskSize, systemColumnTags, shardId));
    }

    void InvalidateDb() {
        for (ui32 i = 0; i < Size(); ++i) {
            BuildValue(i);
        }
    }

    void OwnDb(THolder<TTableIter>&& iter) {
        Iter = std::move(iter);
    }

    void Reuse(const TDbTupleRef& dbData) {
        Y_DEBUG_ABORT_UNLESS(dbData.ColumnCount == Size());
        DbData = dbData;
        ClearMask();
    }

private:
    TSelectRangeLazyRow(const TDbTupleRef& dbData, const THolderFactory& holderFactory, ui32 maskSize,
        const TSmallVec<NTable::TTag>& systemColumnTags, ui64 shardId)
        : TComputationValue<TSelectRangeLazyRow<TTableIter>>(&holderFactory.GetMemInfo())
        , Iter()
        , DbData(dbData)
        , MaskSize(maskSize)
        , SystemColumnTags(systemColumnTags)
        , ShardId(shardId)
    {
        ClearMask();
        for (ui32 i = 0; i < Size(); ++i) {
            ::new(GetPtr() + i) NUdf::TUnboxedValue();
        }
    }

private:
    inline NUdf::TUnboxedValue* GetPtr() const {
        return const_cast<NUdf::TUnboxedValue*>(reinterpret_cast<const NUdf::TUnboxedValue*>(this + 1));
    }

    inline ui64* GetMaskPtr() const {
        return reinterpret_cast<ui64*>(GetPtr() + Size());
    }

    inline ui32 Size() const {
        return DbData.ColumnCount + SystemColumnTags.size();
    }

    inline ui32 GetMaskSize() const {
        return MaskSize;
    }

    inline void BuildValue(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(MaskSize > 0);

        if (!TestMask(index)) {
            if (index < DbData.ColumnCount) {
                GetPtr()[index] = GetCellValue(DbData.Columns[index], DbData.Types[index]);
            } else {
                switch (SystemColumnTags[index - DbData.ColumnCount]) {
                case TKeyDesc::EColumnIdDataShard:
                    GetPtr()[index] = NUdf::TUnboxedValuePod(ShardId);
                    break;
                default:
                    throw TSchemeErrorTabletException();
                }
            }
            SetMask(index);
        }
        Y_DEBUG_ABORT_UNLESS(TestMask(index));
    }

    inline bool TestMask(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index / 64 < MaskSize);
        return GetMaskPtr()[index / 64] & ((ui64)1 << index % 64);
    }

    inline void SetMask(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index / 64 < MaskSize);
        GetMaskPtr()[index / 64] |= ((ui64)1 << index % 64);
    }

    void ClearMask() {
        memset(GetMaskPtr(), 0, sizeof(ui64) * MaskSize);
    }

private:
    THolder<TTableIter> Iter;
    TDbTupleRef DbData;
    ui32 MaskSize;
    TSmallVec<NTable::TTag> SystemColumnTags;
    ui64 ShardId;
};

class TSelectRangeLazyRowsList : public TCustomListValue {
public:
    template<class>
    friend class TIterator;

    template<class TTableIter>
    class TIterator : public TComputationValue<TIterator<TTableIter>> {
        static const ui32 PeriodicCallbackIterations = 1000;

        using TBase = TComputationValue<TIterator<TTableIter>>;

    public:
        TIterator(TMemoryUsageInfo* memInfo, const TSelectRangeLazyRowsList& list, TAutoPtr<TTableIter>&& iter,
            const TSmallVec<NTable::TTag>& systemColumnTags, ui64 shardId)
            : TBase(memInfo)
            , List(list)
            , Iter(iter.Release())
            , HasCurrent(false)
            , Iterations(0)
            , Items(0)
            , Bytes(0)
            , SystemColumnTags(systemColumnTags)
            , ShardId(shardId) {}

        bool Next(NUdf::TUnboxedValue& value) override {
            bool truncated = false;

            Clear();

            while (Iter->Next(NTable::ENext::Data) == NTable::EReady::Data) {
                TDbTupleRef tuple = Iter->GetKey();

                ++Iterations;
                if (Iterations % PeriodicCallbackIterations == 0) {
                    List.EngineHost.ExecPeriodicCallback();
                }

                List.EngineHost.GetCounters().SelectRangeDeletedRowSkips
                    += std::exchange(Iter->Stats.DeletedRowSkips, 0);

                List.EngineHost.GetCounters().InvisibleRowSkips
                    += std::exchange(Iter->Stats.InvisibleRowSkips, 0);

                // Skip null keys
                Y_ABORT_UNLESS(List.SkipNullKeys.size() <= tuple.ColumnCount);
                bool skipRow = false;
                for (ui32 i = 0; i < List.SkipNullKeys.size(); ++i) {
                    if (List.SkipNullKeys[i] && tuple.Columns[i].IsNull()) {
                        skipRow = true;
                        break;
                    }
                }
                if (skipRow) {
                    Clear();
                    continue;
                }

                if ((List.ItemsLimit && Items >= List.ItemsLimit) || (List.BytesLimit && Bytes >= List.BytesLimit)) {
                    truncated = true;
                    break;
                }

                if (Items == 0) {
                    TArrayRef<TCell> array(const_cast<TCell*>(tuple.Columns), tuple.ColumnCount);
                    auto cells = TSerializedCellVec::Serialize(array);
                    ui32 totalSize = sizeof(ui32) + tuple.ColumnCount * sizeof(NScheme::TTypeId) + cells.size();
                    TString firstKey;
                    firstKey.reserve(totalSize);
                    firstKey.AppendNoAlias((const char*)&tuple.ColumnCount, sizeof(ui32));
                    TVector<NScheme::TTypeId> typeIds;
                    typeIds.reserve(tuple.ColumnCount);
                    for (ui32 i = 0; i < tuple.ColumnCount; ++i) {
                        auto typeId = tuple.Types[i].GetTypeId();
                        Y_ENSURE(typeId != NScheme::NTypeIds::Pg, "pg types are not supported");
                        typeIds.push_back(typeId);
                    }
                    firstKey.AppendNoAlias((const char*)typeIds.data(), tuple.ColumnCount * sizeof(NScheme::TTypeId));
                    firstKey.AppendNoAlias(cells);
                    // TODO: support pg types

                    if (List.FirstKey) {
                        Y_DEBUG_ABORT_UNLESS(*List.FirstKey == firstKey);
                    } else {
                        List.FirstKey = firstKey;
                    }
                }

                TDbTupleRef rowValues = Iter->GetValues();
                ui64 rowSize = 0;
                for (ui32 i = 0; i < rowValues.ColumnCount; ++i) {
                    rowSize += rowValues.Columns[i].IsNull() ? 1 : rowValues.Columns[i].Size();
                }
                // Some per-row overhead to deal with the case when no columns were requested
                rowSize = std::max(rowSize, (ui64)8);

                Items++;
                Bytes += rowSize;
                List.EngineHost.GetCounters().SelectRangeRows++;
                List.EngineHost.GetCounters().SelectRangeBytes += rowSize;
                List.KeyAccessSampler->AddSample(List.TableId, tuple.Cells());

                if (HasCurrent && CurrentRowValue.UniqueBoxed()) {
                    CurrentRow()->Reuse(rowValues);
                } else {
                    CurrentRowValue = TSelectRangeLazyRow<TTableIter>::Create(rowValues, List.HolderFactory, SystemColumnTags, ShardId);
                }

                value = CurrentRowValue;
                HasCurrent = true;
                return true;
            }

            List.EngineHost.GetCounters().SelectRangeDeletedRowSkips += std::exchange(Iter->Stats.DeletedRowSkips, 0);
            List.EngineHost.GetCounters().InvisibleRowSkips += std::exchange(Iter->Stats.InvisibleRowSkips, 0);

            if (Iter->Last() ==  NTable::EReady::Page) {
                throw TNotReadyTabletException();
            }

            if (List.Truncated || List.SizeBytes) {
                Y_DEBUG_ABORT_UNLESS(List.Truncated && *List.Truncated == truncated);
                Y_DEBUG_ABORT_UNLESS(List.SizeBytes && *List.SizeBytes == Bytes);
            } else {
                List.Truncated = truncated;
                List.SizeBytes = Bytes;
            }

            CurrentRowValue = NUdf::TUnboxedValue();
            HasCurrent = false;
            return false;
        }

        ~TIterator() {
            if (HasCurrent) {
                if (!CurrentRowValue.UniqueBoxed()) {
                    CurrentRow()->OwnDb(std::move(Iter));
                }
            }
        }
    private:
        void Clear() {
            if (HasCurrent) {
                if (!CurrentRowValue.UniqueBoxed()) {
                    CurrentRow()->InvalidateDb();
                }
            }
        }

        TSelectRangeLazyRow<TTableIter>* CurrentRow() const {
            return static_cast<TSelectRangeLazyRow<TTableIter>*>(CurrentRowValue.AsBoxed().Get());
        }

    private:
        const TSelectRangeLazyRowsList& List;
        THolder<TTableIter> Iter;
        bool HasCurrent;
        ui64 Iterations;
        ui64 Items;
        ui64 Bytes;
        NUdf::TUnboxedValue CurrentRowValue;
        TSmallVec<NTable::TTag> SystemColumnTags;
        ui64 ShardId;
    };

public:
    TSelectRangeLazyRowsList(NTable::TDatabase& db, const TScheme& scheme, const THolderFactory& holderFactory,
        const TTableId& tableId, ui64 localTid, const TSmallVec<NTable::TTag>& tags, const TSmallVec<bool>& skipNullKeys, const TTableRange& range,
        ui64 itemsLimit, ui64 bytesLimit, bool reverse, TEngineHost& engineHost
        , const TSmallVec<NTable::TTag>& systemColumnTags, ui64 shardId, IKeyAccessSampler::TPtr keyAccessSampler,
        NTable::ITransactionMapPtr&& txMap, NTable::ITransactionObserverPtr&& txObserver)
        : TCustomListValue(&holderFactory.GetMemInfo())
        , Db(db)
        , Scheme(scheme)
        , HolderFactory(holderFactory)
        , TableId(tableId)
        , LocalTid(localTid)
        , Tags(tags)
        , SystemColumnTags(systemColumnTags)
        , ShardId(shardId)
        , SkipNullKeys(skipNullKeys)
        , ItemsLimit(itemsLimit)
        , BytesLimit(bytesLimit)
        , RangeHolder(range)
        , Reverse(reverse)
        , EngineHost(engineHost)
        , KeyAccessSampler(keyAccessSampler)
        , TxMap(std::move(txMap))
        , TxObserver(std::move(txObserver))
    {}

    NUdf::TUnboxedValue GetListIterator() const override {
        const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(LocalTid);
        auto tableRange = RangeHolder.ToTableRange();

        Y_DEBUG_ABORT_UNLESS(!tableRange.IsAmbiguous(tableInfo->KeyColumns.size()),
            "%s", tableRange.IsAmbiguousReason(tableInfo->KeyColumns.size()));

        TSmallVec<TRawTypeValue> keyFrom;
        TSmallVec<TRawTypeValue> keyTo;
        ConvertTableKeys(Scheme, tableInfo, tableRange.From, keyFrom, nullptr);
        ConvertTableKeys(Scheme, tableInfo, tableRange.To, keyTo, nullptr);

        NTable::TKeyRange keyRange;
        keyRange.MinKey = keyFrom;
        keyRange.MaxKey = keyTo;
        keyRange.MinInclusive = tableRange.InclusiveFrom;
        keyRange.MaxInclusive = tableRange.InclusiveTo;
        if (Reverse) {
            auto read = Db.IterateRangeReverse(LocalTid, keyRange, Tags, EngineHost.GetReadVersion(TableId), TxMap, TxObserver);

            return NUdf::TUnboxedValuePod(
                new TIterator<NTable::TTableReverseIter>(GetMemInfo(), *this, std::move(read), SystemColumnTags, ShardId)
            );
        } else {
            auto read = Db.IterateRange(LocalTid, keyRange, Tags, EngineHost.GetReadVersion(TableId), TxMap, TxObserver);

            return NUdf::TUnboxedValuePod(
                new TIterator<NTable::TTableIter>(GetMemInfo(), *this, std::move(read), SystemColumnTags, ShardId)
            );
        }
    }

    NUdf::TUnboxedValue GetTruncated() const {
        if (!Truncated) {
            for (const auto it = GetListIterator(); it.Skip();)
                continue;
        }

        Y_DEBUG_ABORT_UNLESS(Truncated);
        return NUdf::TUnboxedValuePod(*Truncated);
    }

    NUdf::TUnboxedValue GetFirstKey() const {
        if (!FirstKey) {
            GetListIterator().Skip();
        }

        TString key = FirstKey ? *FirstKey : TString();
        return MakeString(key);
    }

    NUdf::TUnboxedValue GetSizeBytes() const {
        if (!SizeBytes) {
            for (const auto it = GetListIterator(); it.Skip();)
                continue;
        }

        Y_DEBUG_ABORT_UNLESS(SizeBytes);
        return NUdf::TUnboxedValuePod(*SizeBytes);
    }

private:
    NTable::TDatabase& Db;
    const TScheme& Scheme;
    const THolderFactory& HolderFactory;
    TTableId TableId;
    ui64 LocalTid;
    TSmallVec<NTable::TTag> Tags;
    TSmallVec<NTable::TTag> SystemColumnTags;
    ui64 ShardId;
    TSmallVec<bool> SkipNullKeys;
    ui64 ItemsLimit;
    ui64 BytesLimit;
    TSerializedTableRange RangeHolder;
    bool Reverse;

    mutable TMaybe<bool> Truncated;
    mutable TMaybe<TString> FirstKey;
    mutable TMaybe<ui64> SizeBytes;
    TEngineHost& EngineHost;
    IKeyAccessSampler::TPtr KeyAccessSampler;

    NTable::ITransactionMapPtr TxMap;
    NTable::ITransactionObserverPtr TxObserver;
};

class TSelectRangeResult : public TComputationValue<TSelectRangeResult> {
public:
    TSelectRangeResult(NTable::TDatabase& db, const TScheme& scheme, const THolderFactory& holderFactory, const TTableId& tableId, ui64 localTid,
        const TSmallVec<NTable::TTag>& tags, const TSmallVec<bool>& skipNullKeys, const TTableRange& range,
        ui64 itemsLimit, ui64 bytesLimit, bool reverse, TEngineHost& engineHost,
        const TSmallVec<NTable::TTag>& systemColumnTags, ui64 shardId, IKeyAccessSampler::TPtr keyAccessSampler,
        NTable::ITransactionMapPtr&& txMap, NTable::ITransactionObserverPtr&& txObserver)
        : TComputationValue(&holderFactory.GetMemInfo())
        , List(NUdf::TUnboxedValuePod(new TSelectRangeLazyRowsList(db, scheme, holderFactory, tableId, localTid, tags,
            skipNullKeys, range, itemsLimit, bytesLimit, reverse, engineHost, systemColumnTags, shardId, keyAccessSampler,
            std::move(txMap), std::move(txObserver)))) {}

private:
    NUdf::TUnboxedValue GetElement(ui32 index) const override {
        TSelectRangeLazyRowsList* list = static_cast<TSelectRangeLazyRowsList*>(List.AsBoxed().Get());
        switch (index) {
            case 0: return List;
            case 1: return list->GetTruncated();
            case 2: return list->GetFirstKey();
            case 3: return list->GetSizeBytes();
        }

        Y_ABORT("TSelectRangeResult: Index out of range.");
    }

    NUdf::TUnboxedValue List;
};

static NUdf::TUnboxedValue CreateEmptyRange(const THolderFactory& holderFactory) {
    NUdf::TUnboxedValue* itemsPtr = nullptr;
    auto res = holderFactory.CreateDirectArrayHolder(4, itemsPtr);
    // Empty list (data container)
    itemsPtr[0] = NUdf::TUnboxedValue(holderFactory.GetEmptyContainerLazy());
    // Truncated flag
    itemsPtr[1] = NUdf::TUnboxedValuePod(false);
    // First key
    itemsPtr[2] = NUdf::TUnboxedValuePod::Zero();
    // Size
    itemsPtr[3] = NUdf::TUnboxedValuePod((ui64)0);

    return res;
}

static TSmallVec<bool> CreateBoolVec(const TListLiteral* list) {
    TSmallVec<bool> res;
    if (list) {
        res.reserve(list->GetItemsCount());
        for (ui32 i = 0; i < list->GetItemsCount(); ++i) {
            res.push_back(AS_VALUE(TDataLiteral, list->GetItems()[i])->AsValue().Get<bool>());
        }
    }

    while (!res.empty() && !res.back()) {
        res.pop_back();
    }

    return res;
}

NUdf::TUnboxedValue TEngineHost::SelectRange(const TTableId& tableId, const TTableRange& range,
    TStructLiteral* columnIds, TListLiteral* skipNullKeys, TStructType* returnType, const TReadTarget& readTarget,
    ui64 itemsLimit, ui64 bytesLimit, bool reverse, std::pair<const TListLiteral*, const TListLiteral*> forbidNullArgs,
    const THolderFactory& holderFactory)
{
    // It is assumed that returnType has form:
    //  struct<
    //          list< // named 'List'
    //                struct<optional<data>,
    //                       ...
    //                       optional<data>>
    //              >
    //        , data // named 'Truncated' (boolean showing that itemsLimit or bytesLimit striked)
    //        >
    // And that struct type has column tags stored inside it's member names (ugly hack)

    Y_UNUSED(readTarget);

    // TODO[serxa]: support for Point in SelectRange()
    Y_ABORT_UNLESS(!range.Point, "point request in TEngineHost::SelectRange");

    ui64 localTid = LocalTableId(tableId);
    Y_ABORT_UNLESS(localTid, "table not exist");

    // Analyze resultType
    TStructType* outerStructType = AS_TYPE(TStructType, returnType);
    Y_DEBUG_ABORT_UNLESS(outerStructType->GetMembersCount() == 2,
        "Unexpected type structure of returnType in TEngineHost::SelectRange()");
    Y_DEBUG_ABORT_UNLESS(outerStructType->GetMemberName(0) == "List",
        "Unexpected type structure of returnType in TEngineHost::SelectRange()");
    Y_DEBUG_ABORT_UNLESS(outerStructType->GetMemberName(1) == "Truncated",
        "Unexpected type structure of returnType in TEngineHost::SelectRange()");

    TSmallVec<NTable::TTag> tags;
    TSmallVec<NTable::TTag> systemColumnTags;
    AnalyzeRowType(columnIds, tags, systemColumnTags);

    TSmallVec<bool> skipNullKeysFlags = CreateBoolVec(skipNullKeys);
    TSmallVec<bool> forbidNullArgsFrom = CreateBoolVec(forbidNullArgs.first);
    TSmallVec<bool> forbidNullArgsTo = CreateBoolVec(forbidNullArgs.second);

    for (size_t keyIdx = 0; keyIdx < forbidNullArgsFrom.size(); keyIdx++) {
        if (!forbidNullArgsFrom[keyIdx])
            continue;

        if (keyIdx < range.From.size() && range.From[keyIdx].IsNull()) {
            return CreateEmptyRange(holderFactory);
        }
    }

    for (size_t keyIdx = 0; keyIdx < forbidNullArgsTo.size(); keyIdx++) {
        if (!forbidNullArgsTo[keyIdx])
            continue;

        if (keyIdx < range.To.size() && range.To[keyIdx].IsNull()) {
            return CreateEmptyRange(holderFactory);
        }
    }

    Counters.NSelectRange++;

    return NUdf::TUnboxedValuePod(new TSelectRangeResult(Db, Scheme, holderFactory, tableId, localTid, tags,
        skipNullKeysFlags, range, itemsLimit, bytesLimit, reverse, *this, systemColumnTags, GetShardId(),
        Settings.KeyAccessSampler, GetReadTxMap(tableId), GetReadTxObserver(tableId)));
}

// Updates the single row. Column in commands must be unique.
void TEngineHost::UpdateRow(const TTableId& tableId, const TArrayRef<const TCell>& row, const TArrayRef<const TUpdateCommand>& commands) {
    ui64 localTid = LocalTableId(tableId);
    Y_ABORT_UNLESS(localTid, "table not exist");
    const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(localTid);
    TSmallVec<TRawTypeValue> key;
    ui64 keyBytes = 0;
    ConvertTableKeys(Scheme, tableInfo, row, key, &keyBytes);

    ui64 valueBytes = 0;
    TSmallVec<NTable::TUpdateOp> ops;
    ConvertTableValues(Scheme, tableInfo, commands, ops, &valueBytes);

    auto* collector = GetChangeCollector(tableId);

    const ui64 writeTxId = GetWriteTxId(tableId);
    if (writeTxId == 0) {
        auto writeVersion = GetWriteVersion(tableId);
        if (collector && !collector->OnUpdate(tableId, localTid, NTable::ERowOp::Upsert, key, ops, writeVersion)) {
            throw TNotReadyTabletException();
        }
        Db.Update(localTid, NTable::ERowOp::Upsert, key, ops, writeVersion);
    } else {
        if (collector && !collector->OnUpdateTx(tableId, localTid, NTable::ERowOp::Upsert, key, ops, writeTxId)) {
            throw TNotReadyTabletException();
        }
        Db.UpdateTx(localTid, NTable::ERowOp::Upsert, key, ops, writeTxId);
    }

    Settings.KeyAccessSampler->AddSample(tableId, row);
    Counters.NUpdateRow++;
    Counters.UpdateRowBytes += keyBytes + valueBytes;
}

// Erases the single row.
void TEngineHost::EraseRow(const TTableId& tableId, const TArrayRef<const TCell>& row) {
    ui64 localTid = LocalTableId(tableId);
    Y_ABORT_UNLESS(localTid, "table not exist");
    const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(localTid);
    TSmallVec<TRawTypeValue> key;
    ui64 keyBytes = 0;
    ConvertTableKeys(Scheme, tableInfo, row, key, &keyBytes);

    auto* collector = GetChangeCollector(tableId);

    const ui64 writeTxId = GetWriteTxId(tableId);
    if (writeTxId == 0) {
        auto writeVersion = GetWriteVersion(tableId);
        if (collector && !collector->OnUpdate(tableId, localTid, NTable::ERowOp::Erase, key, { }, writeVersion)) {
            throw TNotReadyTabletException();
        }
        Db.Update(localTid, NTable::ERowOp::Erase, key, { }, writeVersion);
    } else {
        if (collector && !collector->OnUpdateTx(tableId, localTid, NTable::ERowOp::Erase, key, { }, writeTxId)) {
            throw TNotReadyTabletException();
        }
        Db.UpdateTx(localTid, NTable::ERowOp::Erase, key, { }, writeTxId);
    }

    Settings.KeyAccessSampler->AddSample(tableId, row);
    Counters.NEraseRow++;
    Counters.EraseRowBytes += keyBytes + 8;
}

void TEngineHost::CommitWriteTxId(const TTableId& tableId, ui64 writeTxId) {
    ui64 localTid = LocalTableId(tableId);
    Y_ABORT_UNLESS(localTid, "table does not exist");

    Db.CommitTx(localTid, writeTxId);
}

// Check that table is erased
bool TEngineHost::IsPathErased(const TTableId& tableId) const {
    ui64 localTid = LocalTableId(tableId);
    return (localTid? !Scheme.GetTableInfo(localTid): true);
}

// Returns whether row belong this shard.
bool TEngineHost::IsMyKey(const TTableId& tableId, const TArrayRef<const TCell>& row) const {
    Y_UNUSED(tableId); Y_UNUSED(row);
    return true;
}

ui64 TEngineHost::GetTableSchemaVersion(const TTableId&) const {
    return 0;
}

ui64 TEngineHost::LocalTableId(const TTableId& tableId) const {
    return tableId.PathId.LocalPathId;
}

void TEngineHost::ConvertKeys(const TScheme::TTableInfo* tableInfo, const TArrayRef<const TCell>& row,
    TSmallVec<TRawTypeValue>& key) const
{
    ConvertTableKeys(Scheme, tableInfo, row, key, nullptr);
}

void TEngineHost::SetPeriodicCallback(TPeriodicCallback&& callback) {
    PeriodicCallback = std::move(callback);
}

bool IsValidKey(const TScheme& scheme, ui64 localTableId, TKeyDesc& key) {
    auto* tableInfo = scheme.GetTableInfo(localTableId);
    Y_ABORT_UNLESS(tableInfo);

#define EH_VALIDATE(cond, err_status)                   \
    do {                                                \
        if (!(cond)) {                                  \
            key.Status = TKeyDesc::EStatus::err_status; \
            return false;                               \
        }                                               \
    } while (false) /**/

    EH_VALIDATE(tableInfo, NotExists);  // Table does not exist
    EH_VALIDATE(key.KeyColumnTypes.size() <= tableInfo->KeyColumns.size(), TypeCheckFailed);

    // Specified keys types should be valid for any operation
    for (size_t keyIdx = 0; keyIdx < key.KeyColumnTypes.size(); keyIdx++) {
        ui32 keyCol = tableInfo->KeyColumns[keyIdx];
        auto vtype = scheme.GetColumnInfo(tableInfo, keyCol)->PType;
        EH_VALIDATE(key.KeyColumnTypes[keyIdx] == vtype, TypeCheckFailed);
    }

    if (key.RowOperation == TKeyDesc::ERowOperation::Read) {
        if (key.Range.Point) {
            EH_VALIDATE(key.KeyColumnTypes.size() == tableInfo->KeyColumns.size(), TypeCheckFailed);
        } else {
            EH_VALIDATE(key.KeyColumnTypes.size() <= tableInfo->KeyColumns.size(), TypeCheckFailed);
        }

        for (size_t i = 0; i < key.Columns.size(); i++) {
            const TKeyDesc::TColumnOp& cop = key.Columns[i];
            if (IsSystemColumn(cop.Column)) {
                continue;
            }
            auto* cinfo = scheme.GetColumnInfo(tableInfo, cop.Column);
            EH_VALIDATE(cinfo, TypeCheckFailed);  // Unknown column
            auto vtype = cinfo->PType;
            EH_VALIDATE(cop.ExpectedType == vtype, TypeCheckFailed);
            EH_VALIDATE(cop.Operation == TKeyDesc::EColumnOperation::Read, OperationNotSupported);
        }
    } else if (key.RowOperation == TKeyDesc::ERowOperation::Update) {
        EH_VALIDATE(key.KeyColumnTypes.size() == tableInfo->KeyColumns.size(), TypeCheckFailed);  // Key must be full for updates
        for (size_t i = 0; i < key.Columns.size(); i++) {
            const TKeyDesc::TColumnOp& cop = key.Columns[i];
            auto* cinfo = scheme.GetColumnInfo(tableInfo, cop.Column);
            EH_VALIDATE(cinfo, TypeCheckFailed);  // Unknown column
            auto vtype = cinfo->PType;
            EH_VALIDATE(cop.ExpectedType.GetTypeId() == 0 || cop.ExpectedType == vtype, TypeCheckFailed);
            EH_VALIDATE(cop.Operation == TKeyDesc::EColumnOperation::Set, OperationNotSupported);  // TODO[serxa]: support inplace operations in IsValidKey
        }
    } else if (key.RowOperation == TKeyDesc::ERowOperation::Erase) {
        EH_VALIDATE(key.KeyColumnTypes.size() == tableInfo->KeyColumns.size(), TypeCheckFailed);
    } else {
        EH_VALIDATE(false, OperationNotSupported);
    }

#undef EH_VALIDATE

    key.Status = TKeyDesc::EStatus::Ok;
    return true;
}

void AnalyzeRowType(TStructLiteral* columnIds, TSmallVec<NTable::TTag>& tags, TSmallVec<NTable::TTag>& systemColumnTags) {
    // Find out tags that should be read in Select*() functions
    tags.reserve(columnIds->GetValuesCount());
    for (ui32 i = 0; i < columnIds->GetValuesCount(); i++) {
        NTable::TTag columnId = AS_VALUE(TDataLiteral, columnIds->GetValue(i))->AsValue().Get<ui32>();
        if (IsSystemColumn(columnId)) {
            systemColumnTags.push_back(columnId);
        } else {
            tags.push_back(columnId);
        }
    }
}

NUdf::TUnboxedValue GetCellValue(const TCell& cell, NScheme::TTypeInfo type) {
    if (cell.IsNull()) {
        return NUdf::TUnboxedValue();
    }

    switch (type.GetTypeId()) {
        case NYql::NProto::TypeIds::Bool:
            return NUdf::TUnboxedValuePod(cell.AsValue<bool>());
        case NYql::NProto::TypeIds::Int8:
            return NUdf::TUnboxedValuePod(cell.AsValue<i8>());
        case NYql::NProto::TypeIds::Uint8:
            return NUdf::TUnboxedValuePod(cell.AsValue<ui8>());
        case NYql::NProto::TypeIds::Int16:
            return NUdf::TUnboxedValuePod(cell.AsValue<i16>());
        case NYql::NProto::TypeIds::Uint16:
            return NUdf::TUnboxedValuePod(cell.AsValue<ui16>());
        case NYql::NProto::TypeIds::Int32:
            return NUdf::TUnboxedValuePod(cell.AsValue<i32>());
        case NYql::NProto::TypeIds::Uint32:
            return NUdf::TUnboxedValuePod(cell.AsValue<ui32>());
        case NYql::NProto::TypeIds::Int64:
            return NUdf::TUnboxedValuePod(cell.AsValue<i64>());
        case NYql::NProto::TypeIds::Uint64:
            return NUdf::TUnboxedValuePod(cell.AsValue<ui64>());
        case NYql::NProto::TypeIds::Float:
            return NUdf::TUnboxedValuePod(cell.AsValue<float>());
        case NYql::NProto::TypeIds::Double:
            return NUdf::TUnboxedValuePod(cell.AsValue<double>());

        case NYql::NProto::TypeIds::Decimal: {
            // Decimal values are stored as 128-bit integers in Kikimr but
            // should be stored as 120-bit embedded value in UnboxedValue.
            NYql::NDecimal::TInt128 val;
            std::memcpy(reinterpret_cast<char*>(&val), cell.Data(), cell.Size());
            return NUdf::TUnboxedValuePod(val);
        }

        case NYql::NProto::TypeIds::Date: {
            NUdf::TDataType<NUdf::TDate>::TLayout v = cell.AsValue<ui16>();
            return NUdf::TUnboxedValuePod(v);
        }
        case NYql::NProto::TypeIds::Datetime: {
            NUdf::TDataType<NUdf::TDatetime>::TLayout v = cell.AsValue<ui32>();
            return NUdf::TUnboxedValuePod(v);
        }
        case NYql::NProto::TypeIds::Timestamp: {
            NUdf::TDataType<NUdf::TTimestamp>::TLayout v = cell.AsValue<ui64>();
            return NUdf::TUnboxedValuePod(v);
        }
        case NYql::NProto::TypeIds::Interval: {
            NUdf::TDataType<NUdf::TInterval>::TLayout v = cell.AsValue<i64>();
            return NUdf::TUnboxedValuePod(v);
        }

        case NYql::NProto::TypeIds::Date32: {
            NUdf::TDataType<NUdf::TDate32>::TLayout v = cell.AsValue<i32>();
            return NUdf::TUnboxedValuePod(v);
        }
        case NYql::NProto::TypeIds::Datetime64: {
            NUdf::TDataType<NUdf::TDatetime64>::TLayout v = cell.AsValue<i64>();
            return NUdf::TUnboxedValuePod(v);
        }
        case NYql::NProto::TypeIds::Timestamp64: {
            NUdf::TDataType<NUdf::TTimestamp64>::TLayout v = cell.AsValue<i64>();
            return NUdf::TUnboxedValuePod(v);
        }
        case NYql::NProto::TypeIds::Interval64: {
            NUdf::TDataType<NUdf::TInterval64>::TLayout v = cell.AsValue<i64>();
            return NUdf::TUnboxedValuePod(v);
        }

        case NYql::NProto::TypeIds::TzDate:
        case NYql::NProto::TypeIds::TzDatetime:
        case NYql::NProto::TypeIds::TzTimestamp:
            // TODO: create TUnboxedValue and call .SetTimezoneId()

        case NYql::NProto::TypeIds::String:
        case NYql::NProto::TypeIds::Utf8:
        case NYql::NProto::TypeIds::Yson:
        case NYql::NProto::TypeIds::Json:
        case NYql::NProto::TypeIds::Uuid:
        case NYql::NProto::TypeIds::JsonDocument:
        case NYql::NProto::TypeIds::DyNumber:
            return MakeString(NUdf::TStringRef(cell.Data(), cell.Size()));

        default:
            break;
    }

    if (type.GetTypeId() == NScheme::NTypeIds::Pg) {
        return NYql::NCommon::PgValueFromNativeBinary(cell.AsBuf(), NPg::PgTypeIdFromTypeDesc(type.GetTypeDesc()));
    }

    Y_DEBUG_ABORT("Unsupported type: %" PRIu16, type.GetTypeId());
    return MakeString(NUdf::TStringRef(cell.Data(), cell.Size()));
}

}}
