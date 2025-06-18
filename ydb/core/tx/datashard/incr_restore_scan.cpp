#include "incr_restore_scan.h"
#include "change_exchange_impl.h"
#include "datashard_impl.h"

#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/tx/datashard/change_collector.h>
#include <ydb/core/tx/datashard/change_record.h>
#include <ydb/core/tx/datashard/change_record_body_serializer.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/incr_restore_helpers.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NDataShard {

using namespace NActors;
using namespace NTable;

class TIncrementalRestoreScan
    : public IActorCallback
    , public IActorExceptionHandler
    , public NTable::IScan
    , protected TChangeRecordBodySerializer
{
    using TLimits = NStreamScan::TLimits;
    using TBuffer = NStreamScan::TBuffer;
    using TChange = IDataShardChangeCollector::TChange;

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[TIncrementalRestoreScan]"
                << "[" << TxId << "]"
                << SourcePathId
                << TargetPathId
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }
public:
    explicit TIncrementalRestoreScan(
            TActorId parent,
            std::function<TActorId(const TActorContext& ctx, TActorId parent)> changeSenderFactory,
            const TPathId& sourcePathId,
            TUserTable::TCPtr table,
            const TPathId& targetPathId,
            ui64 txId,
            NStreamScan::TLimits limits)
        : IActorCallback(static_cast<TReceiveFunc>(&TIncrementalRestoreScan::StateWork), NKikimrServices::TActivity::INCREMENTAL_RESTORE_SCAN_ACTOR)
        , Parent(parent)
        , ChangeSenderFactory(changeSenderFactory)
        , TxId(txId)
        , SourcePathId(sourcePathId)
        , TargetPathId(targetPathId)
        , ValueTags(InitValueTags(table))
        , Limits(limits)
        , Columns(table->Columns)
        , KeyColumnTypes(table->KeyColumnTypes)
        , KeyColumnIds(table->KeyColumnIds)
    {}

    static TVector<TTag> InitValueTags(TUserTable::TCPtr table) {
        Y_ENSURE(table->Columns.size() >= 2);
        TVector<TTag> valueTags;
        valueTags.reserve(table->Columns.size() - 1);
        bool deletedMarkerColumnFound = false;
        for (const auto& [tag, column] : table->Columns) {
            if (!column.IsKey) {
                valueTags.push_back(tag);
                if (column.Name == "__ydb_incrBackupImpl_deleted") {
                    deletedMarkerColumnFound = true;
                }
            }
        }

        Y_ENSURE(deletedMarkerColumnFound);

        return valueTags;
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::INCREMENTAL_RESTORE_SCAN_ACTOR;
    }

    void Registered(TActorSystem*, const TActorId&) override {
        ChangeSender = ChangeSenderFactory(TlsActivationContext->AsActorContext(), SelfId());
    }

    void PassAway() override {
        Send(ChangeSender, new TEvents::TEvPoisonPill());

        IActorCallback::PassAway();
    }

    void Start(TEvIncrementalRestoreScan::TEvServe::TPtr& ev) {
        LOG_D("Handle TEvIncrementalRestoreScan::TEvServe " << ev->Get()->ToString());

        Driver->Touch(EScan::Feed);
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRequestRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        TVector<TChangeRecord::TPtr> records(::Reserve(ev->Get()->Records.size()));

        for (const auto& record : ev->Get()->Records) {
            auto it = PendingRecords.find(record.Order);
            Y_ENSURE(it != PendingRecords.end());
            records.emplace_back(it->second);
        }

        Send(ChangeSender, new NChangeExchange::TEvChangeExchange::TEvRecords(std::move(records)));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRemoveRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        for (auto recordId : ev->Get()->Records) {
            PendingRecords.erase(recordId);
        }

        Driver->Touch(EScan::Feed);
    }

    void Handle(TEvIncrementalRestoreScan::TEvFinished::TPtr& ev) {
        LOG_D("Handle TEvIncrementalRestoreScan::TEvFinished " << ev->Get()->ToString());

        Driver->Touch(EScan::Final);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvIncrementalRestoreScan::TEvServe, Start);
            hFunc(NChangeExchange::TEvChangeExchange::TEvRequestRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvRemoveRecords, Handle);
            hFunc(TEvIncrementalRestoreScan::TEvFinished, Handle);
        }
    }

    IScan::TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) override {
        TlsActivationContext->AsActorContext().RegisterWithSameMailbox(this);
        Driver = driver;
        Y_ENSURE(!LastKey || LastKey->GetCells().size() == scheme->Tags(true).size());

        return {EScan::Sleep, {}};
    }

    EScan Seek(TLead& lead, ui64) override {
        LOG_D("Seek");

        if (LastKey) {
            lead.To(ValueTags, LastKey->GetCells(), ESeek::Upper);
        } else {
            lead.To(ValueTags, {}, ESeek::Lower);
        }

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) override {
        Buffer.AddRow(key, *row);
        if (Buffer.Bytes() < Limits.BatchMaxBytes) {
            if (Buffer.Rows() < Limits.BatchMaxRows) {
                return EScan::Feed;
            }
        } else {
            if (Buffer.Rows() < Limits.BatchMinRows) {
                return EScan::Feed;
            }
        }

        Progress();
        return EScan::Sleep;
    }

    EScan Exhausted() override {
        LOG_D("Exhausted");

        NoMoreData = true;

        if (!Buffer) {
            Send(ChangeSender, new TEvIncrementalRestoreScan::TEvNoMoreData());
            return EScan::Sleep;
        }

        return Progress();
    }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        LOG_D("Finish " << status);

        if (status != EStatus::Done) {
            // TODO: https://github.com/ydb-platform/ydb/issues/18797
        }

        Send(Parent, new TEvIncrementalRestoreScan::TEvFinished(TxId));

        PassAway();
        return nullptr;
    }

    bool OnUnhandledException(const std::exception& exc) override {
        if (!Driver) {
            return false;
        }
        Driver->Throw(exc);
        return true;
    }

    void Describe(IOutputStream& o) const override {
        o << "IncrRestoreScan {"
          << " TxId: " << TxId
          << " SourcePathId: " << SourcePathId
          << " TargetPathId: " << TargetPathId
        << " }";
    }

    EScan Progress() {
        auto rows = Buffer.Flush();
        TVector<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo> records;

        for (auto& [k, v] : rows) {
            const auto key = NStreamScan::MakeKey(k.GetCells(), KeyColumnTypes);
            const auto& keyTags = KeyColumnIds;
            NKikimrChangeExchange::TDataChange body;
            if (auto updates = NIncrRestoreHelpers::MakeRestoreUpdates(v.GetCells(), ValueTags, Columns); updates) {
                Serialize(body, ERowOp::Upsert, key, keyTags, *updates);
            } else {
                Serialize(body, ERowOp::Erase, key, keyTags, {});
            }
            auto recordPtr = TChangeRecordBuilder(TChangeRecord::EKind::IncrementalRestore)
                .WithOrder(++Order)
                .WithGroup(0)
                .WithPathId(TargetPathId)
                .WithTableId(SourcePathId)
                // .WithSchemaVersion(ReadVersion) // TODO(use SchemaVersion)
                .WithBody(body.SerializeAsString())
                .WithSource(TChangeRecord::ESource::InitialScan)
                .Build();

            const auto& record = *recordPtr;

            records.emplace_back(record.GetOrder(), record.GetPathId(), record.GetBody().size());
            PendingRecords.emplace(record.GetOrder(), recordPtr);
        }

        Send(ChangeSender, new NChangeExchange::TEvChangeExchange::TEvEnqueueRecords(records));

        if (NoMoreData) {
            Send(ChangeSender, new TEvIncrementalRestoreScan::TEvNoMoreData());
            return EScan::Sleep;
        }

        // TODO also limit on PendingRecords contents to keep memory usage in reasonable limits
        return EScan::Feed;
    }

private:
    const TActorId Parent;
    const std::function<TActorId(const TActorContext& ctx, TActorId parent)> ChangeSenderFactory;
    const ui64 TxId;
    const TPathId SourcePathId;
    const TPathId TargetPathId;
    const TVector<TTag> ValueTags;
    const TMaybe<TSerializedCellVec> LastKey;
    const TLimits Limits;
    mutable TMaybe<TString> LogPrefix;
    IDriver* Driver;
    bool NoMoreData = false;
    TBuffer Buffer;
    ui64 Order = 0;
    TActorId ChangeSender;
    TMap<ui64, TChangeRecord::TPtr> PendingRecords;

    TMap<ui32, TUserTable::TUserColumn> Columns;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<ui32> KeyColumnIds;
};

THolder<NTable::IScan> CreateIncrementalRestoreScan(
        NActors::TActorId parent,
        std::function<TActorId(const TActorContext& ctx, TActorId parent)> changeSenderFactory,
        const TPathId& sourcePathId,
        TUserTable::TCPtr table,
        const TPathId& targetPathId,
        ui64 txId,
        NStreamScan::TLimits limits)
{
    return MakeHolder<TIncrementalRestoreScan>(
        parent,
        changeSenderFactory,
        sourcePathId,
        table,
        targetPathId,
        txId,
        limits);
}

} // namespace NKikimr::NDataShard
