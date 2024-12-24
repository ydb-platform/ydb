#include "logging.h"
#include "service.h"
#include "table_writer.h"
#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/size_literals.h>

#include <tuple>

namespace NKikimr::NReplication::NService {

class TSessionInfo {
public:
    explicit TSessionInfo(const TActorId& actorId)
        : ActorId(actorId)
        , Generation(0)
    {
    }

    operator TActorId() const {
        return ActorId;
    }

    ui64 GetGeneration() const {
        return Generation;
    }

    void Update(const TActorId& actorId, ui64 generation) {
        Y_ABORT_UNLESS(Generation <= generation);
        ActorId = actorId;
        Generation = generation;
    }

    bool HasWorker(const TWorkerId& id) const {
        return Workers.contains(id);
    }

    bool HasWorker(const TActorId& id) const {
        return ActorIdToWorkerId.contains(id);
    }

    TActorId GetWorkerActorId(const TWorkerId& id) const {
        auto it = Workers.find(id);
        Y_ABORT_UNLESS(it != Workers.end());
        return it->second;
    }

    TWorkerId GetWorkerId(const TActorId& id) const {
        auto it = ActorIdToWorkerId.find(id);
        Y_ABORT_UNLESS(it != ActorIdToWorkerId.end());
        return it->second;
    }

    TActorId RegisterWorker(IActorOps* ops, const TWorkerId& id, IActor* actor) {
        auto res = Workers.emplace(id, ops->Register(actor));
        Y_ABORT_UNLESS(res.second);

        const auto actorId = res.first->second;
        ActorIdToWorkerId.emplace(actorId, id);

        SendWorkerStatus(ops, id, NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING);
        return actorId;
    }

    void StopWorker(IActorOps* ops, const TWorkerId& id) {
        auto it = Workers.find(id);
        Y_ABORT_UNLESS(it != Workers.end());

        ops->Send(it->second, new TEvents::TEvPoison());
        SendWorkerStatus(ops, id, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED);

        ActorIdToWorkerId.erase(it->second);
        Workers.erase(it);
    }

    template <typename... Args>
    void StopWorker(IActorOps* ops, const TActorId& id, Args&&... args) {
        auto it = ActorIdToWorkerId.find(id);
        Y_ABORT_UNLESS(it != ActorIdToWorkerId.end());

        // actor already stopped
        SendWorkerStatus(ops, it->second, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED, std::forward<Args>(args)...);

        Workers.erase(it->second);
        ActorIdToWorkerId.erase(it);
    }

    template <typename... Args>
    void SendWorkerStatus(IActorOps* ops, const TWorkerId& id, Args&&... args) {
        ops->Send(ActorId, new TEvService::TEvWorkerStatus(id, std::forward<Args>(args)...));
    }

    void SendStatus(IActorOps* ops) const {
        auto ev = MakeHolder<TEvService::TEvStatus>();
        auto& record = ev->Record;

        for (const auto& [id, _] : Workers) {
            id.Serialize(*record.AddWorkers());
        }

        ops->Send(ActorId, ev.Release());
    }

    void SendWorkerDataEnd(IActorOps* ops, const TWorkerId& id, ui64 partitionId, const TVector<ui64>&& adjacentPartitionsIds, const TVector<ui64>&& childPartitionsIds) {
        auto ev = MakeHolder<TEvService::TEvWorkerDataEnd>();
        auto& record = ev->Record;

        id.Serialize(*record.MutableWorker());
        record.SetPartitionId(partitionId);
        for (auto id : adjacentPartitionsIds) {
            record.AddAdjacentPartitionsIds(id);
        }
        for (auto id : childPartitionsIds) {
            record.AddChildPartitionsIds(id);
        }

        ops->Send(ActorId, ev.Release());
    }

    void Shutdown(IActorOps* ops) const {
        for (const auto& [_, actorId] : Workers) {
            ops->Send(actorId, new TEvents::TEvPoison());
        }
    }

private:
    TActorId ActorId;
    ui64 Generation;
    THashMap<TWorkerId, TActorId> Workers;
    THashMap<TActorId, TWorkerId> ActorIdToWorkerId;

}; // TSessionInfo

struct TConnectionParams: std::tuple<TString, TString, bool, TString> {
    explicit TConnectionParams(const TString& endpoint, const TString& database, bool ssl, const TString& user)
        : std::tuple<TString, TString, bool, TString>(endpoint, database, ssl, user)
    {
    }

    const TString& Endpoint() const {
        return std::get<0>(*this);
    }

    const TString& Database() const {
        return std::get<1>(*this);
    }

    bool EnableSsl() const {
        return std::get<2>(*this);
    }

    static TConnectionParams FromProto(const NKikimrReplication::TConnectionParams& params) {
        const auto& endpoint = params.GetEndpoint();
        const auto& database = params.GetDatabase();
        const bool ssl = params.GetEnableSsl();

        switch (params.GetCredentialsCase()) {
        case NKikimrReplication::TConnectionParams::kStaticCredentials:
            return TConnectionParams(endpoint, database, ssl, params.GetStaticCredentials().GetUser());
        case NKikimrReplication::TConnectionParams::kOAuthToken:
            return TConnectionParams(endpoint, database, ssl, params.GetOAuthToken().GetToken());
        default:
            Y_ABORT("Unexpected credentials");
        }
    }

}; // TConnectionParams

} // NKikimr::NReplication::NService

template <>
struct THash<NKikimr::NReplication::NService::TConnectionParams> : THash<std::tuple<TString, TString, bool, TString>> {};

namespace NKikimr::NReplication {

namespace NService {

class TReplicationService: public TActorBootstrapped<TReplicationService> {
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[Service]"
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

    void RunBoardPublisher() {
        const auto& tenant = AppData()->TenantName;

        auto* domainInfo = AppData()->DomainsInfo->GetDomainByName(ExtractDomain(tenant));
        if (!domainInfo) {
            return PassAway();
        }

        const auto boardPath = MakeDiscoveryPath(tenant);
        BoardPublisher = Register(CreateBoardPublishActor(boardPath, TString(), SelfId(), 0, true));
    }

    void Handle(TEvService::TEvHandshake::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;
        const auto& controller = record.GetController();

        auto it = Sessions.find(controller.GetTabletId());
        if (it == Sessions.end()) {
            it = Sessions.emplace(controller.GetTabletId(), ev->Sender).first;
        }

        auto& session = it->second;

        if (session.GetGeneration() > controller.GetGeneration()) {
            LOG_W("Ignore stale controller"
                << ": controller# " << controller.GetTabletId()
                << ", generation# " << controller.GetGeneration());
            return;
        }

        session.Update(ev->Sender, controller.GetGeneration());
        session.SendStatus(this);
    }

    template <typename... Args>
    const TActorId& GetOrCreateYdbProxy(TConnectionParams&& params, Args&&... args) {
        auto it = YdbProxies.find(params);
        if (it == YdbProxies.end()) {
            auto ydbProxy = Register(CreateYdbProxy(params.Endpoint(), params.Database(), params.EnableSsl(), std::forward<Args>(args)...));
            auto res = YdbProxies.emplace(std::move(params), std::move(ydbProxy));
            Y_ABORT_UNLESS(res.second);
            it = res.first;
        }

        return it->second;
    }

    std::function<IActor*(void)> ReaderFn(const NKikimrReplication::TRemoteTopicReaderSettings& settings) {
        TActorId ydbProxy;
        const auto& params = settings.GetConnectionParams();
        switch (params.GetCredentialsCase()) {
        case NKikimrReplication::TConnectionParams::kStaticCredentials:
            ydbProxy = GetOrCreateYdbProxy(TConnectionParams::FromProto(params), params.GetStaticCredentials());
            break;
        case NKikimrReplication::TConnectionParams::kOAuthToken:
            ydbProxy = GetOrCreateYdbProxy(TConnectionParams::FromProto(params), params.GetOAuthToken().GetToken());
            break;
        default:
            Y_ABORT("Unexpected credentials");
        }

        auto topicReaderSettings = TEvYdbProxy::TTopicReaderSettings()
            .MaxMemoryUsageBytes(1_MB)
            .ConsumerName(settings.GetConsumerName())
            .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                .Path(settings.GetTopicPath())
                .AppendPartitionIds(settings.GetTopicPartitionId())
            );

        return [ydbProxy, settings = std::move(topicReaderSettings)]() {
            return CreateRemoteTopicReader(ydbProxy, settings);
        };
    }

    static std::function<IActor*(void)> WriterFn(
            const NKikimrReplication::TLocalTableWriterSettings& writerSettings,
            const NKikimrReplication::TConsistencySettings& consistencySettings)
    {
        const auto mode = consistencySettings.HasGlobal()
            ? EWriteMode::Consistent
            : EWriteMode::Simple;
        return [tablePathId = TPathId::FromProto(writerSettings.GetPathId()), mode]() {
            return CreateLocalTableWriter(tablePathId, mode);
        };
    }

    void Handle(TEvService::TEvRunWorker::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;
        const auto& controller = record.GetController();
        const auto& id = TWorkerId::Parse(record.GetWorker());

        auto it = Sessions.find(controller.GetTabletId());
        if (it == Sessions.end()) {
            LOG_W("Cannot run worker"
                << ": controller# " << controller.GetTabletId()
                << ", worker# " << id
                << ", reason# " << R"("unknown session")");
            return;
        }

        auto& session = it->second;
        if (session.GetGeneration() != controller.GetGeneration()) {
            LOG_W("Cannot run worker"
                << ": controller# " << controller.GetTabletId()
                << ", generation# " << controller.GetGeneration()
                << ", worker# " << id
                << ", reason# " << R"("generation mismatch")");
            return;
        }

        if (session.HasWorker(id)) {
            return session.SendWorkerStatus(this, id, NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING);
        }

        LOG_I("Run worker"
            << ": worker# " << id);

        const auto& cmd = record.GetCommand();
        // TODO: validate settings
        const auto& readerSettings = cmd.GetRemoteTopicReader();
        const auto& writerSettings = cmd.GetLocalTableWriter();
        const auto& consistencySettings = cmd.GetConsistencySettings();
        const auto actorId = session.RegisterWorker(this, id,
            CreateWorker(SelfId(), ReaderFn(readerSettings), WriterFn(writerSettings, consistencySettings)));
        WorkerActorIdToSession[actorId] = controller.GetTabletId();
    }

    void Handle(TEvService::TEvStopWorker::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;
        const auto& controller = record.GetController();
        const auto& id = TWorkerId::Parse(record.GetWorker());

        auto it = Sessions.find(controller.GetTabletId());
        if (it == Sessions.end()) {
            LOG_W("Cannot stop worker"
                << ": controller# " << controller.GetTabletId()
                << ", worker# " << id
                << ", reason# " << R"("unknown session")");
            return;
        }

        auto& session = it->second;
        if (session.GetGeneration() != controller.GetGeneration()) {
            LOG_W("Cannot stop worker"
                << ": controller# " << controller.GetTabletId()
                << ", generation# " << controller.GetGeneration()
                << ", worker# " << id
                << ", reason# " << R"("generation mismatch")");
            return;
        }

        if (!session.HasWorker(id)) {
            return session.SendWorkerStatus(this, id, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED);
        }

        LOG_I("Stop worker"
            << ": worker# " << id);
        WorkerActorIdToSession.erase(session.GetWorkerActorId(id));
        session.StopWorker(this, id);
    }

    void SendTxIdResult(const TActorId& recipient, const TMap<TRowVersion, ui64>& result) {
        auto ev = MakeHolder<TEvService::TEvTxIdResult>();

        for (const auto& [version, txId] : result) {
            auto& item = *ev->Record.AddVersionTxIds();
            version.ToProto(item.MutableVersion());
            item.SetTxId(txId);
        }

        Send(recipient, ev.Release());
    }

    void Handle(TEvService::TEvGetTxId::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        const auto* session = SessionFromWorker(ev->Sender);
        if (!session) {
            return;
        }

        if (!session->HasWorker(ev->Sender)) {
            LOG_E("Cannot find worker"
                << ": worker# " << ev->Sender);
            return;
        }

        TMap<TRowVersion, ui64> result;
        TVector<TRowVersion> versionsWithoutTxId;

        for (const auto& v : ev->Get()->Record.GetVersions()) {
            const auto version = TRowVersion::FromProto(v);
            if (auto it = TxIds.upper_bound(version); it != TxIds.end()) {
                result[it->first] = it->second;
            } else {
                versionsWithoutTxId.push_back(version);
                PendingTxId[version].insert(ev->Sender);
            }
        }

        if (versionsWithoutTxId) {
            Send(*session, new TEvService::TEvGetTxId(versionsWithoutTxId));
        }

        if (result) {
            SendTxIdResult(ev->Sender, result);
        }
    }

    void Handle(TEvService::TEvTxIdResult::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;
        const auto& controller = record.GetController();

        auto it = Sessions.find(controller.GetTabletId());
        if (it == Sessions.end()) {
            LOG_W("Cannot process tx id result"
                << ": controller# " << controller.GetTabletId()
                << ", reason# " << R"("unknown session")");
            return;
        }

        auto& session = it->second;
        if (session.GetGeneration() != controller.GetGeneration()) {
            LOG_W("Cannot process tx id result"
                << ": controller# " << controller.GetTabletId()
                << ", generation# " << controller.GetGeneration()
                << ", reason# " << R"("generation mismatch")");
            return;
        }

        THashMap<TActorId, TMap<TRowVersion, ui64>> results;

        for (const auto& kv : record.GetVersionTxIds()) {
            const auto version = TRowVersion::FromProto(kv.GetVersion());
            TxIds.emplace(version, kv.GetTxId());

            for (auto it = PendingTxId.begin(); it != PendingTxId.end();) {
                if (it->first >= version) {
                    break;
                }

                for (const auto& actorId : it->second) {
                    results[actorId].emplace(version, kv.GetTxId());
                }

                PendingTxId.erase(it++);
            }
        }

        for (const auto& [actorId, result] : results) {
            SendTxIdResult(actorId, result);
        }
    }

    void Handle(TEvService::TEvHeartbeat::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        const auto* session = SessionFromWorker(ev->Sender);
        if (!session) {
            return;
        }

        if (!session->HasWorker(ev->Sender)) {
            LOG_E("Cannot find worker"
                << ": worker# " << ev->Sender);
            return;
        }

        auto& record = ev->Get()->Record;

        LOG_I("Heartbeat"
            << ": worker# " << ev->Sender
            << ", version# " << TRowVersion::FromProto(record.GetVersion()));

        session->GetWorkerId(ev->Sender).Serialize(*record.MutableWorker());
        Send(ev->Forward(*session));
    }

    void Handle(TEvWorker::TEvDataEnd::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        auto* session = SessionFromWorker(ev->Sender);
        if (!session) {
            return;
        }

        if (!session->HasWorker(ev->Sender)) {
            LOG_E("Cannot find worker"
                << ": worker# " << ev->Sender);
            return;
        }

        LOG_I("Worker has ended"
            << ": worker# " << ev->Sender);
        session->SendWorkerDataEnd(this, session->GetWorkerId(ev->Sender), ev->Get()->PartitionId,
            std::move(ev->Get()->AdjacentPartitionsIds), std::move(ev->Get()->ChildPartitionsIds));
    }

    void Handle(TEvWorker::TEvGone::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        auto* session = SessionFromWorker(ev->Sender);
        if (!session) {
            return;
        }

        if (!session->HasWorker(ev->Sender)) {
            LOG_E("Cannot find worker"
                << ": worker# " << ev->Sender);
            return;
        }

        LOG_I("Worker has gone"
            << ": worker# " << ev->Sender);
        WorkerActorIdToSession.erase(ev->Sender);
        session->StopWorker(this, ev->Sender, ToReason(ev->Get()->Status), ev->Get()->ErrorDescription);
    }

    void Handle(TEvWorker::TEvStatus::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        auto* session = SessionFromWorker(ev->Sender);
        if (session && session->HasWorker(ev->Sender)) {
            session->SendWorkerStatus(this, session->GetWorkerId(ev->Sender), ev->Get()->Lag);
        }
    }

    TSessionInfo* SessionFromWorker(const TActorId& id) {
        auto wit = WorkerActorIdToSession.find(id);
        if (wit == WorkerActorIdToSession.end()) {
            LOG_W("Unknown worker has gone"
                << ": worker# " << id);
            return nullptr;
        }

        auto it = Sessions.find(wit->second);
        if (it == Sessions.end()) {
            LOG_E("Cannot find session"
                << ": worker# " << id
                << ", session# " << wit->second);
            return nullptr;
        }

        return &it->second;
    }

    static NKikimrReplication::TEvWorkerStatus::EReason ToReason(TEvWorker::TEvGone::EStatus status) {
        switch (status) {
        case TEvWorker::TEvGone::SCHEME_ERROR:
            return NKikimrReplication::TEvWorkerStatus::REASON_ERROR;
        default:
            return NKikimrReplication::TEvWorkerStatus::REASON_UNSPECIFIED;
        }
    }

    void PassAway() override {
        if (auto actorId = std::exchange(BoardPublisher, {})) {
            Send(actorId, new TEvents::TEvPoison());
        }

        for (const auto& [_, session] : Sessions) {
            session.Shutdown(this);
        }

        for (const auto& [_, actorId] : YdbProxies) {
            Send(actorId, new TEvents::TEvPoison());
        }

        TActorBootstrapped<TReplicationService>::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_SERVICE;
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        RunBoardPublisher();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvService::TEvHandshake, Handle);
            hFunc(TEvService::TEvRunWorker, Handle);
            hFunc(TEvService::TEvStopWorker, Handle);
            hFunc(TEvService::TEvGetTxId, Handle);
            hFunc(TEvService::TEvTxIdResult, Handle);
            hFunc(TEvService::TEvHeartbeat, Handle);
            hFunc(TEvWorker::TEvDataEnd, Handle);
            hFunc(TEvWorker::TEvGone, Handle);
            hFunc(TEvWorker::TEvStatus, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    mutable TMaybe<TString> LogPrefix;
    TActorId BoardPublisher;
    THashMap<ui64, TSessionInfo> Sessions;
    THashMap<TConnectionParams, TActorId> YdbProxies;
    THashMap<TActorId, ui64> WorkerActorIdToSession;
    TMap<TRowVersion, ui64> TxIds;
    TMap<TRowVersion, THashSet<TActorId>> PendingTxId;

}; // TReplicationService

} // NService

IActor* CreateReplicationService() {
    return new NService::TReplicationService();
}

}
