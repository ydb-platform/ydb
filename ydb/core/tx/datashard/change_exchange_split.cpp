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

namespace NKikimr {
namespace NDataShard {

class TCdcPartitionWorker: public TActorBootstrapped<TCdcPartitionWorker> {
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[ChangeExchangeSplitCdcPartitionWorker]"
                << "[" << SrcTabletId << "]"
                << "[" << PartitionId << "]"
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }

    void Ack() {
        LOG_I("Send ack");
        Send(Parent, new TEvChangeExchange::TEvSplitAck());
        PassAway();
    }

    void Leave() {
        LOG_I("Leave");
        Send(Parent, new TEvents::TEvGone());
        PassAway();
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

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
            LOG_W("Pipe connection error");
            Leave();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->TabletId == TabletId) {
            LOG_W("Pipe disconnected");
            Leave();
        }
    }

    void PassAway() override {
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
    mutable TMaybe<TString> LogPrefix;

    TActorId PipeClient;

}; // TCdcPartitionWorker

class TCdcWorker
    : public TActorBootstrapped<TCdcWorker>
    , private NSchemeCache::TSchemeCacheHelpers
{
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[ChangeExchangeSplitCdcWorker]"
                << "[" << SrcTabletId << "]"
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }

    void Ack() {
        LOG_I("Send ack");
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
        LOG_C(error);
        Retry();
    }

    void LogWarnAndRetry(const TString& error) {
        LOG_W(error);
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
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCdcStream);
            sFunc(TEvents::TEvWakeup, ResolveCdcStream);
        default:
            return StateBase(ev);
        }
    }

    void HandleCdcStream(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

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
            LOG_N("Auto-ack (stream is planned to drop)");
            return Ack();
        }

        Y_ABORT_UNLESS(entry.ListNodeEntry->Children.size() == 1);
        const auto& topic = entry.ListNodeEntry->Children.at(0);

        Y_ABORT_UNLESS(topic.Kind == TNavigate::KindTopic);
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
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleTopic);
            sFunc(TEvents::TEvWakeup, ResolveCdcStream);
        default:
            return StateBase(ev);
        }
    }

    void HandleTopic(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

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

            auto it = Workers.find(partitionId);
            if (it != Workers.end()) {
                workers.emplace(partitionId, it->second);
                Workers.erase(it);
            } else {
                LOG_T("Register new worker"
                    << ": partitionId# " << partitionId);

                const auto worker = Register(new TCdcPartitionWorker(SelfId(), partitionId, tabletId, SrcTabletId, DstTabletIds));
                workers.emplace(partitionId, worker);
                Pending.emplace(worker, partitionId);
            }
        }

        for (const auto& kv : Workers) {
            const auto& partitionId = kv.first;
            const auto& worker = kv.second;

            if (worker) {
                LOG_T("Kill stale worker"
                    << ": partitionId# " << partitionId);

                Send(worker, new TEvents::TEvPoisonPill());
                Pending.erase(worker);
            }
        }

        if (Pending.empty()) {
            return Ack();
        }

        LOG_I("Wait " << Pending.size() << " worker(s)");

        Workers = std::move(workers);
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        return StateBase(ev);
    }

    void Handle(TEvChangeExchange::TEvSplitAck::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        auto it = Pending.find(ev->Sender);
        if (it == Pending.end()) {
            LOG_W("Split ack from unknown worker"
                << ": worker# " << ev->Sender);
            return;
        }

        LOG_N("Split ack"
            << ": worker# " << it->first
            << ", partition# " << it->second);

        Workers[it->second] = TActorId();
        Pending.erase(it);

        if (!IsResolving() && Pending.empty()) {
            Ack();
        }
    }

    void Handle(TEvents::TEvGone::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        auto it = Pending.find(ev->Sender);
        if (it == Pending.end()) {
            LOG_W("Unknown worker has gone"
                << ": worker# " << ev->Sender);
            return;
        }

        LOG_I("Worker has gone"
            << ": worker# " << it->first
            << ", partition# " << it->second);

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
        ResolveCdcStream();
    }

    STATEFN(StateBase) {
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
    mutable TMaybe<TString> LogPrefix;

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

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[ChangeExchangeSplit]"
                << "[" << DataShard.TabletId << "]"
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }

    TActorId RegisterWorker(const TPathId& pathId, EWorkerType type) const {
        switch (type) {
        case EWorkerType::CdcStream:
            return Register(new TCdcWorker(SelfId(), pathId, DataShard.TabletId, DstDataShards));
        case EWorkerType::AsyncIndex:
            Y_ABORT("unreachable");
        }
    }

    TActorId RegisterWorker(const TPathId& pathId, TWorker& worker) const {
        Y_DEBUG_ABORT_UNLESS(!worker.ActorId);
        worker.ActorId = RegisterWorker(pathId, worker.Type);
        return worker.ActorId;
    }

    void Ack() {
        LOG_I("Send ack");
        Send(DataShard.ActorId, new TEvChangeExchange::TEvSplitAck());
        PassAway();
    }

    void Handle(TEvChangeExchange::TEvSplitAck::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        auto it = Pending.find(ev->Sender);
        if (it == Pending.end()) {
            LOG_W("Split ack from unknown worker"
                << ": worker# " << ev->Sender);
            return;
        }

        LOG_N("Split ack"
            << ": worker# " << ev->Sender);

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
        if (!Workers) {
            LOG_N("Auto-ack (no active worker)");
            return Ack();
        }

        for (auto& kv : Workers) {
            const auto& pathId = kv.first;
            auto& worker = kv.second;

            LOG_D("Register worker"
                << ": pathId# " << pathId);
            Pending.emplace(RegisterWorker(pathId, worker), pathId);
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvSplitAck, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TDataShardId DataShard;
    const TVector<ui64> DstDataShards;
    mutable TMaybe<TString> LogPrefix;

    THashMap<TPathId, TWorker> Workers;
    THashMap<TActorId, TPathId> Pending;

}; // TChangeExchageSplit

IActor* CreateChangeExchangeSplit(const TDataShard* self, const TVector<ui64>& dstDataShards) {
    return new TChangeExchageSplit(self, dstDataShards);
}

} // NDataShard
} // NKikimr
