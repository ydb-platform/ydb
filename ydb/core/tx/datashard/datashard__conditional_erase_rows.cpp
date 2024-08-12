#include "datashard_distributed_erase.h"
#include "datashard_impl.h"
#include "erase_rows_condition.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/protos/datashard_config.pb.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NDataShard {

using namespace NActors;
using namespace NTable;

using TLimits = NKikimrTxDataShard::TEvConditionalEraseRowsRequest::TLimits;

class IEraserOps {
protected:
    struct TKey {
        TTag Tag = Max<TTag>();
        TPos Pos = Max<TPos>();
    };

    virtual TVector<TKey> MakeKeyOrder(TIntrusiveConstPtr<IScan::TScheme> scheme) const = 0;
    virtual TActorId CreateEraser() = 0;
    virtual void CloseEraser() = 0;
};

class TCondEraseScan: public IActorCallback, public IScan, public IEraserOps {
    struct TDataShardId {
        TActorId ActorId;
        ui64 TabletId;
    };

    class TSerializedKeys {
    public:
        explicit TSerializedKeys(
                ui32 bytesLimit = 500 * 1024,
                ui32 minCount = 1000,
                ui32 maxCount = 50000)
            : BytesLimit(bytesLimit)
            , MinCount(minCount)
            , MaxCount(maxCount)
            , Size(0)
        {
        }

        void Add(TString key) {
            Size += key.size();
            Keys.emplace_back(std::move(key));
        }

        void Clear() {
            Keys.clear();
            Size = 0;
        }

        TVector<TString>& GetKeys() {
            return Keys;
        }

        ui32 Count() const {
            return Keys.size();
        }

        ui32 Bytes() const {
            return Size;
        }

        bool CheckLimits() const {
            if (Size < BytesLimit) {
                if (Count() < MaxCount) {
                    return true;
                }
            } else {
                if (Count() < MinCount) {
                    return true;
                }
            }

            return false;
        }

        explicit operator bool() const {
            return Count();
        }

    private:
        const ui32 BytesLimit;
        const ui32 MinCount;
        const ui32 MaxCount;

        TVector<TString> Keys;
        ui32 Size;
    };

    struct TStats {
        TStats()
            : RowsProcessed(0)
            , RowsErased(0)
        {
            auto counters = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "erase_rows");

            MonProcessed = counters->GetCounter("Processed", true);
            MonErased = counters->GetCounter("Erased", true);
        }

        void IncProcessed() {
            ++RowsProcessed;
            *MonProcessed += 1;
        }

        void IncErased() {
            ++RowsErased;
            *MonErased += 1;
        }

        void ToProto(NKikimrTxDataShard::TEvConditionalEraseRowsResponse::TStats& stats) const {
            stats.SetRowsProcessed(RowsProcessed);
            stats.SetRowsErased(RowsErased);
        }

    private:
        ui64 RowsProcessed;
        ui64 RowsErased;
        ::NMonitoring::TDynamicCounters::TCounterPtr MonProcessed;
        ::NMonitoring::TDynamicCounters::TCounterPtr MonErased;
    };

    static TVector<TCell> MakeKeyCells(const TVector<TKey>& keyOrder, const TRow& row) {
        TVector<TCell> keyCells;

        for (const auto& key : keyOrder) {
            Y_ABORT_UNLESS(key.Pos != Max<TPos>());
            Y_ABORT_UNLESS(key.Pos < row.Size());
            keyCells.push_back(row.Get(key.Pos));
        }

        return keyCells;
    }

    static THolder<TEvDataShard::TEvEraseRowsRequest> MakeEraseRowsRequest(const TTableId& tableId,
            IEraseRowsCondition* condition, const TVector<TKey>& keyOrder, TVector<TString>& keys)
    {
        auto request = MakeHolder<TEvDataShard::TEvEraseRowsRequest>();

        request->Record.SetTableId(tableId.PathId.LocalPathId);
        request->Record.SetSchemaVersion(tableId.SchemaVersion);

        for (const auto& key : keyOrder) {
            Y_ABORT_UNLESS(key.Tag != Max<TTag>());
            request->Record.AddKeyColumnIds(key.Tag);
        }

        for (TString& key : keys) {
            request->Record.AddKeyColumns(std::move(key));
        }

        condition->AddToRequest(request->Record);

        return request;
    }

    void SendEraseRowsRequest() {
        Send(CreateEraser(), MakeEraseRowsRequest(TableId, Condition.Get(), KeyOrder, SerializedKeys.GetKeys()));
        SerializedKeys.Clear();
    }

    void Reply(bool aborted = false) {
        auto response = MakeHolder<TEvDataShard::TEvConditionalEraseRowsResponse>();
        response->Record.SetTabletID(DataShard.TabletId);

        if (aborted) {
            response->Record.SetStatus(NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ABORTED);
        } else if (!Success) {
            response->Record.SetStatus(NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ERASE_ERROR);
        } else if (!NoMoreData) {
            response->Record.SetStatus(NKikimrTxDataShard::TEvConditionalEraseRowsResponse::PARTIAL);
        } else {
            response->Record.SetStatus(NKikimrTxDataShard::TEvConditionalEraseRowsResponse::OK);
        }

        response->Record.SetErrorDescription(Error);
        Stats.ToProto(*response->Record.MutableStats());

        Send(ReplyTo, std::move(response));
    }

    void Handle(TEvDataShard::TEvEraseRowsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        Success = (record.GetStatus() == NKikimrTxDataShard::TEvEraseRowsResponse::OK);
        Error = record.GetErrorDescription();

        CloseEraser();

        if (NoMoreData || !Success) {
            Driver->Touch(EScan::Final);
        } else {
            Reply();
            Driver->Touch(EScan::Feed);
        }
    }

    void Handle(TEvDataShard::TEvConditionalEraseRowsRequest::TPtr& ev) {
        ReplyTo = ev->Sender;
        Reply();
    }

public:
    explicit TCondEraseScan(TDataShard* ds, const TActorId& replyTo, const TTableId& tableId, ui64 txId, THolder<IEraseRowsCondition> condition, const TLimits& limits)
        : IActorCallback(static_cast<TReceiveFunc>(&TCondEraseScan::StateWork), NKikimrServices::TActivity::CONDITIONAL_ERASE_ROWS_SCAN_ACTOR)
        , TableId(tableId)
        , DataShard{ds->SelfId(), ds->TabletID()}
        , ReplyTo(replyTo)
        , TxId(txId)
        , Condition(std::move(condition))
        , Driver(nullptr)
        , SerializedKeys(limits.GetBatchMaxBytes(), limits.GetBatchMinKeys(), limits.GetBatchMaxKeys())
        , NoMoreData(false)
        , Success(true)
    {
    }

    void Describe(IOutputStream& o) const noexcept override {
        o << "CondEraseScan {"
          << " TableId: " << TableId
          << " TxId: " << TxId
        << " }";
    }

    IScan::TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) noexcept override {
        TlsActivationContext->AsActorContext().RegisterWithSameMailbox(this);

        Driver = driver;
        Scheme = std::move(scheme);
        KeyOrder = MakeKeyOrder(Scheme); // fill tags

        // fill scan tags & positions in KeyOrder
        ScanTags = Condition->Tags();
        Y_ABORT_UNLESS(ScanTags.size() == 1, "Multi-column conditions are not supported");

        THashMap<TTag, TPos> tagToPos;

        for (TPos pos = 0; pos < ScanTags.size(); ++pos) {
            Y_ABORT_UNLESS(tagToPos.emplace(ScanTags.at(pos), pos).second);
        }

        for (auto& key : KeyOrder) {
            auto it = tagToPos.find(key.Tag);
            if (it == tagToPos.end()) {
                it = tagToPos.emplace(key.Tag, ScanTags.size()).first;
                ScanTags.push_back(key.Tag);
            }

            key.Pos = it->second;
        }

        Condition->Prepare(Scheme, 0);

        return {EScan::Feed, {}};
    }

    void Registered(TActorSystem* sys, const TActorId&) override {
        sys->Send(DataShard.ActorId, new TDataShard::TEvPrivate::TEvConditionalEraseRowsRegistered(TxId, SelfId()));
    }

    EScan Seek(TLead& lead, ui64) noexcept override {
        lead.To(ScanTags, {}, ESeek::Lower);
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell>, const TRow& row) noexcept override {
        Stats.IncProcessed();
        if (!Condition->Check(row)) {
            return EScan::Feed;
        }

        Stats.IncErased();
        SerializedKeys.Add(TSerializedCellVec::Serialize(MakeKeyCells(KeyOrder, row)));
        if (SerializedKeys.CheckLimits()) {
            return EScan::Feed;
        }

        SendEraseRowsRequest();
        return EScan::Sleep;
    }

    EScan Exhausted() noexcept override {
        NoMoreData = true;

        if (!SerializedKeys) {
            return EScan::Final;
        }

        SendEraseRowsRequest();
        return EScan::Sleep;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override {
        Reply(abort != EAbort::None);
        PassAway();

        return nullptr;
    }

    void PassAway() override {
        CloseEraser();
        IActor::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvEraseRowsResponse, Handle);
            hFunc(TEvDataShard::TEvConditionalEraseRowsRequest, Handle);
        }
    }

protected:
    TVector<TKey> MakeKeyOrder(TIntrusiveConstPtr<IScan::TScheme> scheme) const override {
        TVector<TKey> keyOrder;

        for (const auto& col : scheme->Cols) {
            if (!col.IsKey()) {
                continue;
            }

            if (keyOrder.size() < (col.Key + 1)) {
                keyOrder.resize(col.Key + 1);
            }

            keyOrder[col.Key].Tag = col.Tag;
        }

        return keyOrder;
    }

    TActorId CreateEraser() override {
        return DataShard.ActorId;
    }

    void CloseEraser() override {
        // nop
    }

protected:
    const TTableId TableId;

private:
    const TDataShardId DataShard;
    TActorId ReplyTo;
    const ui64 TxId;
    THolder<IEraseRowsCondition> Condition;

    IDriver* Driver;
    TIntrusiveConstPtr<TScheme> Scheme;
    TVector<TKey> KeyOrder;
    TVector<TTag> ScanTags;
    TSerializedKeys SerializedKeys;

    TStats Stats;
    bool NoMoreData;
    bool Success;
    TString Error;

}; // TCondEraseScan

class TIndexedCondEraseScan: public TCondEraseScan {
public:
    explicit TIndexedCondEraseScan(
            TDataShard* ds, const TActorId& replyTo, const TTableId& tableId, ui64 txId,
            THolder<IEraseRowsCondition> condition, const TLimits& limits, TIndexes indexes)
        : TCondEraseScan(ds, replyTo, tableId, txId, std::move(condition), limits)
        , Indexes(std::move(indexes))
    {
    }

protected:
    // Enrich key with indexed columns
    TVector<TKey> MakeKeyOrder(TIntrusiveConstPtr<IScan::TScheme> scheme) const override {
        auto keyOrder = TCondEraseScan::MakeKeyOrder(scheme);

        THashSet<TTag> keys;
        for (const auto& key : keyOrder) {
            keys.insert(key.Tag);
        }

        for (const auto& [_, keyMap] : Indexes) {
            for (const auto& [indexColumnId, mainColumnId] : keyMap) {
                Y_UNUSED(indexColumnId);

                if (keys.contains(mainColumnId)) {
                    continue;
                }

                const TColInfo* col = scheme->ColInfo(mainColumnId);
                Y_ABORT_UNLESS(col);
                Y_ABORT_UNLESS(col->Tag == mainColumnId);

                keyOrder.emplace_back().Tag = col->Tag;
                keys.insert(col->Tag);
            }
        }

        return keyOrder;
    }

    TActorId CreateEraser() override {
        Y_ABORT_UNLESS(!Eraser);
        Eraser = this->Register(CreateDistributedEraser(this->SelfId(), TableId, Indexes));
        return Eraser;
    }

    void CloseEraser() override {
        if (!Eraser) {
            return;
        }

        this->Send(std::exchange(Eraser, TActorId()), new TEvents::TEvPoisonPill());
    }

private:
    const TIndexes Indexes;

    TActorId Eraser;

}; // TIndexedCondEraseScan

IScan* CreateCondEraseScan(
        TDataShard* ds, const TActorId& replyTo, const TTableId& tableId, ui64 txId,
        THolder<IEraseRowsCondition> condition, const TLimits& limits, TIndexes indexes)
{
    Y_ABORT_UNLESS(ds);
    Y_ABORT_UNLESS(condition.Get());

    if (!indexes) {
        return new TCondEraseScan(ds, replyTo, tableId, txId, std::move(condition), limits);
    } else {
        return new TIndexedCondEraseScan(ds, replyTo, tableId, txId, std::move(condition), limits, std::move(indexes));
    }
}

static TIndexes GetIndexes(const NKikimrTxDataShard::TEvConditionalEraseRowsRequest& record) {
    TIndexes result;
    if (!record.IndexesSize()) {
        return result;
    }

    for (const auto& index : record.GetIndexes()) {
        TKeyMap keyMap(Reserve(index.KeyMapSize()));
        for (const auto& kv : index.GetKeyMap()) {
            keyMap.emplace_back(kv.GetIndexColumnId(), kv.GetMainColumnId());
        }

        result.emplace(TTableId(index.GetOwnerId(), index.GetPathId(), index.GetSchemaVersion()), std::move(keyMap));
    }

    return result;
}

static bool CheckUnit(NScheme::TTypeInfo type, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& error) {
    switch (type.GetTypeId()) {
    case NScheme::NTypeIds::Date:
    case NScheme::NTypeIds::Datetime:
    case NScheme::NTypeIds::Timestamp:
    case NScheme::NTypeIds::Date32:
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
        if (unit == NKikimrSchemeOp::TTTLSettings::UNIT_AUTO) {
            return true;
        } else {
            error = "Unit cannot be specified for date type column";
            return false;
        }
        break;

    case NScheme::NTypeIds::Uint32:
    case NScheme::NTypeIds::Uint64:
    case NScheme::NTypeIds::DyNumber:
        switch (unit) {
        case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
        case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
        case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
        case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS:
            return true;
        case NKikimrSchemeOp::TTTLSettings::UNIT_AUTO:
            error = "Unit should be specified for integral type column";
            return false;
        default:
            error = TStringBuilder() << "Unknown unit: " << static_cast<ui32>(unit);
            return false;
        }
        break;

    default:
        error = TStringBuilder() << "Unsupported type: " << static_cast<ui32>(type.GetTypeId());
        return false;
    }
}

void TDataShard::Handle(TEvDataShard::TEvConditionalEraseRowsRequest::TPtr& ev, const TActorContext& ctx) {
    using TEvRequest = TEvDataShard::TEvConditionalEraseRowsRequest;
    using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

    const auto& record = ev->Get()->Record;

    auto response = MakeHolder<TEvResponse>();
    response->Record.SetTabletID(TabletID());

    auto badRequest = [&record = response->Record](const TString& error) {
        record.SetStatus(TEvResponse::ProtoRecordType::BAD_REQUEST);
        record.SetErrorDescription(error);
    };

    const ui64 localPathId = record.GetTableId();
    if (GetUserTables().contains(localPathId)) {
        auto condition = record.GetConditionCase();

        if (condition && InFlightCondErase) {
            if (InFlightCondErase.Condition == condition && InFlightCondErase.IsActive()) {
                ctx.Send(ev->Forward(InFlightCondErase.ActorId));
            } else {
                if (InFlightCondErase.Condition != condition) {
                    response->Record.SetStatus(TEvResponse::ProtoRecordType::OVERLOADED);
                }

                ctx.Send(ev->Sender, std::move(response));
            }

            return;
        }

        TUserTable::TCPtr userTable = GetUserTables().at(localPathId);
        if (record.GetSchemaVersion() && userTable->GetTableSchemaVersion()
            && record.GetSchemaVersion() != userTable->GetTableSchemaVersion()) {

            response->Record.SetStatus(TEvResponse::ProtoRecordType::SCHEME_ERROR);
            response->Record.SetErrorDescription(TStringBuilder() << "Schema version mismatch"
                << ": got " << record.GetSchemaVersion()
                << ", expected " << userTable->GetTableSchemaVersion());
            ctx.Send(ev->Sender, std::move(response));
            return;
        }

        ui64 localTxId = 0;
        THolder<IScan> scan;

        switch (condition) {
            case TEvRequest::ProtoRecordType::kExpiration: {
                const ui32 columnId = record.GetExpiration().GetColumnId();
                auto column = userTable->Columns.find(columnId);

                if (column != userTable->Columns.end()) {
                    TString error;
                    if (CheckUnit(column->second.Type, record.GetExpiration().GetColumnUnit(), error)) {
                        localTxId = NextTieBreakerIndex++;
                        const auto tableId = TTableId(PathOwnerId, localPathId, record.GetSchemaVersion());
                        scan.Reset(CreateCondEraseScan(this, ev->Sender, tableId, localTxId,
                            THolder(CreateEraseRowsCondition(record)), record.GetLimits(), GetIndexes(record)));
                    } else {
                        badRequest(error);
                    }
                } else {
                    badRequest(TStringBuilder() << "Unknown column id: " << columnId);
                }
                break;
            }

            default:
                badRequest(TStringBuilder() << "Unknown condition: " << (ui32)condition);
                break;
        }

        if (scan) {
            const ui32 localTableId = userTable->LocalTid;
            Y_ABORT_UNLESS(Executor()->Scheme().GetTableInfo(localTableId));

            auto* appData = AppData(ctx);
            const auto& taskName = appData->DataShardConfig.GetTtlTaskName();
            const auto taskPrio = appData->DataShardConfig.GetTtlTaskPriority();

            ui64 readAheadLo = appData->DataShardConfig.GetTtlReadAheadLo();
            if (ui64 readAheadLoOverride = GetTtlReadAheadLoOverride(); readAheadLoOverride > 0) {
                readAheadLo = readAheadLoOverride;
            }

            ui64 readAheadHi = appData->DataShardConfig.GetTtlReadAheadHi();
            if (ui64 readAheadHiOverride = GetTtlReadAheadHiOverride(); readAheadHiOverride > 0) {
                readAheadHi = readAheadHiOverride;
            }

            const ui64 scanId = QueueScan(localTableId, scan.Release(), localTxId,
                TScanOptions()
                    .SetResourceBroker(taskName, taskPrio)
                    .SetReadAhead(readAheadLo, readAheadHi)
                    .SetReadPrio(TScanOptions::EReadPrio::Low)
            );
            InFlightCondErase.Enqueue(localTxId, scanId, condition);
        }
    } else {
        badRequest(TStringBuilder() << "Unknown table id: " << localPathId);
    }

    ctx.Send(ev->Sender, std::move(response));
}

void TDataShard::Handle(TEvPrivate::TEvConditionalEraseRowsRegistered::TPtr& ev, const TActorContext& ctx) {
    if (!InFlightCondErase || InFlightCondErase.TxId != ev->Get()->TxId) {
        LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, "Unknown conditional erase actor registered"
            << ": at: " << TabletID());
        return;
    }

    InFlightCondErase.ActorId = ev->Get()->ActorId;
}

} // NDataShard
} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::TEvEraseRowsResponse::EStatus, stream, value) {
    stream << NKikimrTxDataShard::TEvEraseRowsResponse_EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::TEvConditionalEraseRowsResponse::EStatus, stream, value) {
    stream << NKikimrTxDataShard::TEvConditionalEraseRowsResponse_EStatus_Name(value);
}
