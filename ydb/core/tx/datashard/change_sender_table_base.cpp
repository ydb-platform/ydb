#include "change_sender_table_base.h"
#include "datashard_impl.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/change_exchange/change_sender_monitoring.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NDataShard {

class TTableChangeSenderShard: public TActorBootstrapped<TTableChangeSenderShard> {
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[TableChangeSenderShard]"
                << "[" << DataShard.TabletId << ":" << DataShard.Generation << "]"
                << "[" << ShardId << "]"
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }

    /// GetProxyServices

    void GetProxyServices() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvGetProxyServicesRequest);
        Become(&TThis::StateGetProxyServices);
    }

    STATEFN(StateGetProxyServices) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvGetProxyServicesResponse, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        LeaderPipeCache = ev->Get()->Services.LeaderPipeCache;
        Handshake();
    }

    /// Handshake

    void Handshake() {
        Send(DataShard.ActorId, new TDataShard::TEvPrivate::TEvConfirmReadonlyLease, 0, ++LeaseConfirmationCookie);
        Become(&TThis::StateHandshake);
    }

    STATEFN(StateHandshake) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TDataShard::TEvPrivate::TEvReadonlyLeaseConfirmation, Handle);
            hFunc(TEvChangeExchange::TEvStatus, Handshake);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TDataShard::TEvPrivate::TEvReadonlyLeaseConfirmation::TPtr& ev) {
        if (ev->Cookie != LeaseConfirmationCookie) {
            LOG_W("Readonly lease confirmation cookie mismatch"
                << ": expected# " << LeaseConfirmationCookie
                << ", got# " << ev->Cookie);
            return;
        }

        auto handshake = MakeHolder<TEvChangeExchange::TEvHandshake>();
        handshake->Record.SetOrigin(DataShard.TabletId);
        handshake->Record.SetGeneration(DataShard.Generation);

        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(handshake.Release(), ShardId, true));
    }

    void Handshake(TEvChangeExchange::TEvStatus::TPtr& ev) {
        LOG_D("Handshake " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
        case NKikimrChangeExchange::TEvStatus::STATUS_OK:
            LastRecordOrder = record.GetLastRecordOrder();
            return Ready();
        default:
            LOG_E("Handshake status"
                << ": status# " << static_cast<ui32>(record.GetStatus())
                << ", reason# " << static_cast<ui32>(record.GetReason()));
            return Leave();
        }
    }

    void Ready() {
        Send(Parent, new NChangeExchange::TEvChangeExchangePrivate::TEvReady(ShardId));
        Become(&TThis::StateWaitingRecords);
    }

    /// WaitingRecords

    STATEFN(StateWaitingRecords) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NChangeExchange::TEvChangeExchange::TEvRecords, Handle);
        default:
            return StateBase(ev);
        }
    }

    class TSerializer: public NChangeExchange::TBaseVisitor {
        NKikimrChangeExchange::TChangeRecord& Record;

    public:
        explicit TSerializer(NKikimrChangeExchange::TChangeRecord& record)
            : Record(record)
        {
        }

        void Visit(const TChangeRecord& record) override {
            record.Serialize(Record);
        }
    };

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        auto records = MakeHolder<TEvChangeExchange::TEvApplyRecords>();
        records->Record.SetOrigin(DataShard.TabletId);
        records->Record.SetGeneration(DataShard.Generation);

        for (auto recordPtr : ev->Get()->Records) {
            const auto& record = *recordPtr;

            if (record.GetOrder() <= LastRecordOrder) {
                continue;
            }

            auto& proto = *records->Record.AddRecords();
            TSerializer serializer(proto);
            record.Accept(serializer);
            Adjust(proto);
        }

        if (!records->Record.RecordsSize()) {
            return Ready();
        }

        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(records.Release(), ShardId, false));
        Become(&TThis::StateWaitingStatus);
    }

    void Adjust(NKikimrChangeExchange::TChangeRecord& record) const {
        record.SetPathOwnerId(TargetTablePathId.OwnerId);
        record.SetLocalPathId(TargetTablePathId.LocalPathId);

        switch(Type) {
            case ETableChangeSenderType::AsyncIndex:
                Y_ABORT_UNLESS(record.HasAsyncIndex());
                AdjustTags(*record.MutableAsyncIndex());
                break;
            case ETableChangeSenderType::IncrementalRestore:
                Y_ABORT_UNLESS(record.HasIncrementalRestore());
                AdjustTags(*record.MutableIncrementalRestore());
                break;
        }
    }

    void AdjustTags(NKikimrChangeExchange::TDataChange& record) const {
        AdjustTags(*record.MutableKey()->MutableTags());

        switch (record.GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert:
            AdjustTags(*record.MutableUpsert()->MutableTags());
            break;
        case NKikimrChangeExchange::TDataChange::kReset:
            AdjustTags(*record.MutableReset()->MutableTags());
            break;
        default:
            break;
        }
    }

    void AdjustTags(google::protobuf::RepeatedField<ui32>& tags) const {
        for (int i = 0; i < tags.size(); ++i) {
            auto it = TagMap.find(tags[i]);
            Y_ABORT_UNLESS(it != TagMap.end());
            tags[i] = it->second;
        }
    }

    /// WaitingStatus

    STATEFN(StateWaitingStatus) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvStatus, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvChangeExchange::TEvStatus::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
        case NKikimrChangeExchange::TEvStatus::STATUS_OK:
            LastRecordOrder = record.GetLastRecordOrder();
            return Ready();
        // TODO: REJECT?
        default:
            LOG_E("Apply status"
                << ": status# " << static_cast<ui32>(record.GetStatus())
                << ", reason# " << static_cast<ui32>(record.GetReason()));
            return Leave();
        }
    }

    bool CanRetry() const {
        if (CurrentStateFunc() != static_cast<TReceiveFunc>(&TThis::StateHandshake)) {
            return false;
        }

        return Attempt < MaxAttempts;
    }

    void Retry() {
        ++Attempt;
        Delay = Min(2 * Delay, MaxDelay);

        LOG_N("Retry"
            << ": attempt# " << Attempt
            << ", delay# " << Delay);

        const auto random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        Schedule(Delay + random, new TEvents::TEvWakeup());
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (ShardId != ev->Get()->TabletId) {
            return;
        }

        if (CanRetry()) {
            Unlink();
            Retry();
        } else {
            Leave();
        }
    }

    void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev) {
        using namespace NChangeExchange;

        TStringStream html;

        HTML(html) {
            Header(html, "Base partition change sender", DataShard.TabletId);

            SimplePanel(html, "Info", [this](IOutputStream& html) {
                HTML(html) {
                    DL_CLASS("dl-horizontal") {
                        TermDescLink(html, "ShardId", ShardId, TabletPath(ShardId));
                        TermDesc(html, "TargetTablePathId", TargetTablePathId);
                        TermDesc(html, "LeaderPipeCache", LeaderPipeCache);
                        TermDesc(html, "LastRecordOrder", LastRecordOrder);
                    }
                }
            });
        }

        Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(html.Str()));
    }

    void Leave() {
        Send(Parent, new NChangeExchange::TEvChangeExchangePrivate::TEvGone(ShardId));
        PassAway();
    }

    void Unlink() {
        if (LeaderPipeCache) {
            Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(ShardId));
        }
    }

    void PassAway() override {
        Unlink();
        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHANGE_SENDER_ASYNC_INDEX_ACTOR_PARTITION;
    }

    TTableChangeSenderShard(
        const TActorId& parent,
        const TDataShardId& dataShard,
        ui64 shardId,
        const TPathId& targetTablePathId,
        const TMap<TTag, TTag>& tagMap,
        ETableChangeSenderType type)
            : Type(type)
            , Parent(parent)
            , DataShard(dataShard)
            , ShardId(shardId)
            , TargetTablePathId(targetTablePathId)
            , TagMap(tagMap)
            , LeaseConfirmationCookie(0)
            , LastRecordOrder(0)
    {
    }

    void Bootstrap() {
        GetProxyServices();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NMon::TEvRemoteHttpInfo, Handle);
            sFunc(TEvents::TEvWakeup, Handshake);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const ETableChangeSenderType Type;
    const TActorId Parent;
    const TDataShardId DataShard;
    const ui64 ShardId;
    const TPathId TargetTablePathId;
    const TMap<TTag, TTag> TagMap; // from main to index
    mutable TMaybe<TString> LogPrefix;

    TActorId LeaderPipeCache;
    ui64 LeaseConfirmationCookie;
    ui64 LastRecordOrder;

    // Retry on delivery problem
    static constexpr ui32 MaxAttempts = 3;
    static constexpr auto MaxDelay = TDuration::MilliSeconds(50);
    ui32 Attempt = 0;
    TDuration Delay = TDuration::MilliSeconds(10);

}; // TTableChangeSenderShard

IActor* CreateTableChangeSenderShard(
    const TActorId& parent,
    const TDataShardId& dataShard,
    ui64 shardId,
    const TPathId& targetTablePathId,
    const TMap<TTag, TTag>& tagMap,
    ETableChangeSenderType type)
{
    return new TTableChangeSenderShard(
        parent,
        dataShard,
        shardId,
        targetTablePathId,
        tagMap,
        type);
}

} // namespace NKikimr::NDataShard
