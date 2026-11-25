#include "change_exchange.h"
#include "change_exchange_helpers.h"
#include "datashard_impl.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CHANGE_EXCHANGE

namespace NKikimr {
namespace NDataShard {

class TCdcPartitionWorker: public TActorBootstrapped<TCdcPartitionWorker> {
    NActors::NStructuredLog::TStructuredMessage GetLogPrefix() const {
        return YDB_LOG_CREATE_MESSAGE(
            {"actorClassName", "ChangeExchangeSplitCdcPartitionWorker"},
            {"selfId", SelfId()},
            {"srcTabletId", SrcTabletId},
            {"partitionId", PartitionId});
    }

    void Ack() {
        YDB_LOG_INFO("Sending split acknowledgment");
        Send(Parent, new TEvChangeExchange::TEvSplitAck());
        PassAway();
    }

    void Leave() {
        YDB_LOG_INFO("Leaving CDC partition split worker");
        Send(Parent, new TEvents::TEvGone());
        PassAway();
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});

        const auto& response = ev->Get()->Record;
        switch (response.GetStatus()) {
        case NMsgBusProxy::MSTATUS_OK:
            if (response.GetErrorCode() == NPersQueue::NErrorCode::OK) {
                return Ack();
            }
            break;
        case NMsgBusProxy::MSTATUS_ERROR:
            if (response.GetErrorCode() == NPersQueue::NErrorCode::SOURCEID_DELETED) {
                return Ack();
            }
            break;
        default:
            break;
        }

        Leave();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->TabletId == TabletId && ev->Get()->Status != NKikimrProto::OK) {
            YDB_LOG_WARN("PersQueue pipe connection failed");
            Leave();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->TabletId == TabletId) {
            YDB_LOG_WARN("PersQueue pipe disconnected");
            Leave();
        }
    }

    void PassAway() override {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        if (PipeClient) {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeClient);
        }

        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHANGE_EXCHANGE_SPLIT_ACTOR;
    }

    explicit TCdcPartitionWorker(const TActorId& parent, ui32 partitionId, ui64 tabletId,
            ui64 srcTabletId, const TVector<ui64>& dstTabletIds)
        : Parent(parent)
        , PartitionId(partitionId)
        , TabletId(tabletId)
        , SrcTabletId(srcTabletId)
        , DstTabletIds(dstTabletIds)
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());

        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };

        PipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), TabletId, config));

        auto ev = MakeHolder<TEvPersQueue::TEvRequest>();
        auto& request = *ev->Record.MutablePartitionRequest();
        request.SetPartition(PartitionId);
        ActorIdToProto(PipeClient, request.MutablePipeClient());

        auto& cmd = *request.MutableCmdSplitMessageGroup();
        {
            auto& group = *cmd.AddDeregisterGroups();
            group.SetId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(SrcTabletId)));
        }
        for (const auto dstTabletId : DstTabletIds) {
            auto& group = *cmd.AddRegisterGroups();
            group.SetId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(dstTabletId)));
        }

        NTabletPipe::SendData(SelfId(), PipeClient, ev.Release());
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPersQueue::TEvResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui32 PartitionId;
    const ui64 TabletId;
    const ui64 SrcTabletId;
    const TVector<ui64> DstTabletIds;

    TActorId PipeClient;

}; // TCdcPartitionWorker

class TCdcWorker
    : public TActorBootstrapped<TCdcWorker>
    , private NSchemeCache::TSchemeCacheHelpers
{
    NActors::NStructuredLog::TStructuredMessage GetLogPrefix() const {
        return YDB_LOG_CREATE_MESSAGE(
            {"actorClassName", "CdcWorker"},
            {"selfId", SelfId()},
            {"srcTabletId", SrcTabletId});
    }

    void Ack() {
        YDB_LOG_INFO("Sending split acknowledgment");
        Send(Parent, new TEvChangeExchange::TEvSplitAck());
        PassAway();
    }

    bool IsResolvingCdcStream() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveCdcStream);
    }

    bool IsResolvingTopic() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveTopic);
    }

    bool IsResolving() const {
        return IsResolvingCdcStream()
            || IsResolvingTopic();
    }

    TStringBuf CurrentStateName() const {
        if (IsResolvingCdcStream()) {
            return "ResolveCdcStream";
        } else if (IsResolvingTopic()) {
            return "ResolveTopic";
        } else {
            return "";
        }
    }

    void Retry() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    void LogCritAndRetry(const TString& error) {
        YDB_LOG_CRIT("Critical error during change exchange operation, retrying",
            {"errorMessage", error});
        Retry();
    }

    void LogWarnAndRetry(const TString& error) {
        YDB_LOG_WARN("Recoverable error during change exchange operation, retrying",
            {"errorMessage", error});
        Retry();
    }

    template <typename CheckFunc, typename FailFunc, typename T, typename... Args>
    bool Check(CheckFunc checkFunc, FailFunc failFunc, const T& subject, Args&&... args) {
        return checkFunc(CurrentStateName(), subject, std::forward<Args>(args)..., std::bind(failFunc, this, std::placeholders::_1));
    }

    bool CheckNotEmpty(const TAutoPtr<TNavigate>& result) {
        return Check(&TSchemeCacheHelpers::CheckNotEmpty<TNavigate>, &TThis::LogCritAndRetry, result);
    }

    bool CheckEntriesCount(const TAutoPtr<TNavigate>& result, ui32 expected) {
        return Check(&TSchemeCacheHelpers::CheckEntriesCount<TNavigate>, &TThis::LogCritAndRetry, result, expected);
    }

    bool CheckTableId(const TNavigate::TEntry& entry, const TTableId& expected) {
        return Check(&TSchemeCacheHelpers::CheckTableId<TNavigate::TEntry>, &TThis::LogCritAndRetry, entry, expected);
    }

    bool CheckEntrySucceeded(const TNavigate::TEntry& entry) {
        return Check(&TSchemeCacheHelpers::CheckEntrySucceeded<TNavigate::TEntry>, &TThis::LogWarnAndRetry, entry);
    }

    bool CheckEntryKind(const TNavigate::TEntry& entry, TNavigate::EKind expected) {
        return Check(&TSchemeCacheHelpers::CheckEntryKind<TNavigate::TEntry>, &TThis::LogWarnAndRetry, entry, expected);
    }

    bool CheckNotEmpty(const TIntrusiveConstPtr<TNavigate::TPQGroupInfo>& pqInfo) {
        if (pqInfo) {
            return true;
        }

        LogCritAndRetry(TStringBuilder() << "Empty pq info at '" << CurrentStateName() << "'");
        return false;
    }

    /// ResolveCdcStream

    void ResolveCdcStream() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(PathId, TNavigate::OpList));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveCdcStream);
    }

    STATEFN(StateResolveCdcStream) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCdcStream);
            sFunc(TEvents::TEvWakeup, ResolveCdcStream);
        default:
            return StateBase(ev);
        }
    }

    void HandleCdcStream(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        YDB_LOG_DEBUG("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"navigateResult", (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr")});

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, PathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindCdcStream)) {
            return;
        }

        if (entry.Self && entry.Self->Info.GetPathState() == NKikimrSchemeOp::EPathStateDrop) {
            YDB_LOG_NOTICE("Auto-acknowledging split: CDC stream is planned to drop");
            return Ack();
        }

        Y_ENSURE(entry.ListNodeEntry->Children.size() == 1);
        const auto& topic = entry.ListNodeEntry->Children.at(0);

        Y_ENSURE(topic.Kind == TNavigate::KindTopic);
        ResolveTopic(topic.PathId);
    }

    /// ResolveTopic

    void ResolveTopic(const TPathId& pathId) {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(pathId, TNavigate::OpTopic));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveTopic);
    }

    STATEFN(StateResolveTopic) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleTopic);
            sFunc(TEvents::TEvWakeup, ResolveCdcStream);
        default:
            return StateBase(ev);
        }
    }

    void HandleTopic(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        YDB_LOG_DEBUG("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"navigateResult", (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr")});

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindTopic)) {
            return;
        }

        if (!CheckNotEmpty(entry.PQGroupInfo)) {
            return;
        }

        THashMap<ui32, TActorId> workers;

        for (const auto& partition : entry.PQGroupInfo->Description.GetPartitions()) {
            const auto partitionId = partition.GetPartitionId();
            const auto tabletId = partition.GetTabletId();

            if (NKikimrPQ::ETopicPartitionStatus::Active != partition.GetStatus()) {
                continue;
            }

            auto it = Workers.find(partitionId);
            if (it != Workers.end()) {
                workers.emplace(partitionId, it->second);
                Workers.erase(it);
            } else {
                YDB_LOG_TRACE("Registering CDC partition split worker",
                    {"partitionId", partitionId});

                const auto worker = Register(new TCdcPartitionWorker(SelfId(), partitionId, tabletId, SrcTabletId, DstTabletIds));
                workers.emplace(partitionId, worker);
                Pending.emplace(worker, partitionId);
            }
        }

        for (const auto& kv : Workers) {
            const auto& partitionId = kv.first;
            const auto& worker = kv.second;

            if (worker) {
                YDB_LOG_TRACE("Stopping stale CDC partition split worker",
                    {"partitionId", partitionId});

                Send(worker, new TEvents::TEvPoisonPill());
                Pending.erase(worker);
            }
        }

        if (Pending.empty()) {
            return Ack();
        }

        YDB_LOG_INFO("Waiting for split workers to finish",
            {"pendingWorkerCount", Pending.size()});

        Workers = std::move(workers);
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        return StateBase(ev);
    }

    void Handle(TEvChangeExchange::TEvSplitAck::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});

        auto it = Pending.find(ev->Sender);
        if (it == Pending.end()) {
            YDB_LOG_WARN("Split acknowledgment from unknown worker",
                {"workerActorId", ev->Sender});
            return;
        }

        YDB_LOG_NOTICE("Received split acknowledgment",
            {"workerActorId", it->first},
            {"partitionId", it->second});

        Workers[it->second] = TActorId();
        Pending.erase(it);

        if (!IsResolving() && Pending.empty()) {
            Ack();
        }
    }

    void Handle(TEvents::TEvGone::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});

        auto it = Pending.find(ev->Sender);
        if (it == Pending.end()) {
            YDB_LOG_WARN("Unknown split worker disconnected",
                {"workerActorId", ev->Sender});
            return;
        }

        YDB_LOG_INFO("Split worker disconnected",
            {"workerActorId", it->first},
            {"partitionId", it->second});

        Workers.erase(it->second);
        Pending.erase(it);

        if (!IsResolving()) {
            ResolveCdcStream();
        }
    }

    void PassAway() override {
        for (const auto& [_, worker] : Workers) {
            if (worker) {
                Send(worker, new TEvents::TEvPoisonPill());
            }
        }

        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHANGE_EXCHANGE_SPLIT_ACTOR;
    }

    explicit TCdcWorker(const TActorId& parent, const TPathId& pathId,
            ui64 srcTabletId, const TVector<ui64>& dstTabletIds)
        : Parent(parent)
        , PathId(pathId)
        , SrcTabletId(srcTabletId)
        , DstTabletIds(dstTabletIds)
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        ResolveCdcStream();
    }

    STATEFN(StateBase) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvSplitAck, Handle);
            hFunc(TEvents::TEvGone, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const TPathId PathId;
    const ui64 SrcTabletId;
    const TVector<ui64> DstTabletIds;

    THashMap<ui32, TActorId> Workers;
    THashMap<TActorId, ui32> Pending;

}; // TCdcWorker

class TChangeExchageSplit: public TActorBootstrapped<TChangeExchageSplit> {
    using EWorkerType = TEvChangeExchange::ESenderType;

    struct TWorker {
        EWorkerType Type;
        TActorId ActorId;

        explicit TWorker(EWorkerType type)
            : Type(type)
        {
        }
    };

    NActors::NStructuredLog::TStructuredMessage GetLogPrefix() const {
        return YDB_LOG_CREATE_MESSAGE(
            {"actorClassName", "ChangeExchangeSplit"},
            {"selfId", SelfId()},
            {"srcTabletId", DataShard.TabletId});
    }

    TActorId RegisterWorker(const TPathId& pathId, EWorkerType type) const {
        switch (type) {
        case EWorkerType::CdcStream:
            return Register(new TCdcWorker(SelfId(), pathId, DataShard.TabletId, DstDataShards));
        case EWorkerType::AsyncIndex:
            Y_ENSURE(false, "unreachable");
        }
    }

    TActorId RegisterWorker(const TPathId& pathId, TWorker& worker) const {
        Y_DEBUG_ABORT_UNLESS(!worker.ActorId);
        worker.ActorId = RegisterWorker(pathId, worker.Type);
        return worker.ActorId;
    }

    void Ack() {
        YDB_LOG_INFO("Sending split acknowledgment");
        Send(DataShard.ActorId, new TEvChangeExchange::TEvSplitAck());
        PassAway();
    }

    void Handle(TEvChangeExchange::TEvSplitAck::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});

        auto it = Pending.find(ev->Sender);
        if (it == Pending.end()) {
            YDB_LOG_WARN("Split acknowledgment from unknown worker",
                {"workerActorId", ev->Sender});
            return;
        }

        YDB_LOG_NOTICE("Received split acknowledgment",
            {"workerActorId", ev->Sender});

        Workers.at(it->second).ActorId = TActorId();
        Pending.erase(it);

        if (Pending.empty()) {
            Ack();
        }
    }

    void PassAway() override {
        for (const auto& [_, worker] : Workers) {
            if (worker.ActorId) {
                Send(worker.ActorId, new TEvents::TEvPoisonPill());
            }
        }

        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHANGE_EXCHANGE_SPLIT_ACTOR;
    }

    explicit TChangeExchageSplit(const TDataShard* self, const TVector<ui64>& dstDataShards)
        : TActorBootstrapped()
        , DataShard{self->TabletID(), self->Generation(), self->SelfId()}
        , DstDataShards(dstDataShards)
    {
        for (const auto& [_, tableInfo] : self->GetUserTables()) {
            for (const auto& [streamPathId, _] : tableInfo->CdcStreams) {
                Workers.emplace(streamPathId, EWorkerType::CdcStream);
            }
        }
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        if (!Workers) {
            YDB_LOG_NOTICE("Auto-acknowledging split: no active workers");
            return Ack();
        }

        for (auto& kv : Workers) {
            const auto& pathId = kv.first;
            auto& worker = kv.second;

            YDB_LOG_DEBUG("Register worker",
                {"pathId", pathId});
            Pending.emplace(RegisterWorker(pathId, worker), pathId);
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvSplitAck, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TDataShardId DataShard;
    const TVector<ui64> DstDataShards;

    THashMap<TPathId, TWorker> Workers;
    THashMap<TActorId, TPathId> Pending;

}; // TChangeExchageSplit

IActor* CreateChangeExchangeSplit(const TDataShard* self, const TVector<ui64>& dstDataShards) {
    return new TChangeExchageSplit(self, dstDataShards);
}

} // NDataShard
} // NKikimr
