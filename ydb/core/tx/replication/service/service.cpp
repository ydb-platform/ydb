#include "service.h"
#include "table_writer.h"
#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/tx/replication/common/worker_id.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash.h>

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

    void RegisterWorker(IActorOps* ops, const TWorkerId& id, IActor* actor) {
        auto res = Workers.emplace(id, ops->Register(actor));
        Y_ABORT_UNLESS(res.second);
    }

    void StopWorker(IActorOps* ops, const TWorkerId& id) {
        auto it = Workers.find(id);
        Y_ABORT_UNLESS(it != Workers.end());

        ops->Send(it->second, new TEvents::TEvPoison());
        Workers.erase(it);
    }

    void SendStatus(IActorOps* ops) const {
        auto ev = MakeHolder<TEvService::TEvStatus>();
        auto& record = ev->Record;

        for (const auto& [id, _] : Workers) {
            id.Serialize(*record.AddWorkers());
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

}; // TSessionInfo

struct TCredentialsKey: std::tuple<TString, TString, TString> {
    explicit TCredentialsKey(const TString& endpoint, const TString& database, const TString& user)
        : std::tuple<TString, TString, TString>(endpoint, database, user)
    {
    }

    const TString& Endpoint() const {
        return std::get<0>(*this);
    }

    const TString& Database() const {
        return std::get<1>(*this);
    }

    static TCredentialsKey FromParams(const NKikimrReplication::TConnectionParams& params) {
        switch (params.GetCredentialsCase()) {
        case NKikimrReplication::TConnectionParams::kStaticCredentials:
            return TCredentialsKey(params.GetEndpoint(), params.GetDatabase(), params.GetStaticCredentials().GetUser());
        case NKikimrReplication::TConnectionParams::kOAuthToken:
            return TCredentialsKey(params.GetEndpoint(), params.GetDatabase(), params.GetOAuthToken() /* TODO */);
        default:
            Y_ABORT("Unexpected credentials");
        }
    }

}; // TCredentialsKey

} // NKikimr::NReplication::NService

template <>
struct THash<NKikimr::NReplication::NService::TCredentialsKey> : THash<std::tuple<TString, TString, TString>> {};

namespace NKikimr::NReplication {

namespace NService {

class TReplicationService: public TActorBootstrapped<TReplicationService> {
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
        const auto& record = ev->Get()->Record;
        const auto& controller = record.GetController();

        auto it = Sessions.find(controller.GetTabletId());
        if (it == Sessions.end()) {
            it = Sessions.emplace(controller.GetTabletId(), ev->Sender).first;
        }

        auto& session = it->second;

        if (session.GetGeneration() > controller.GetGeneration()) {
            // ignore stale controller
            return;
        }

        session.Update(ev->Sender, controller.GetGeneration());
        session.SendStatus(this);
    }

    template <typename... Args>
    const TActorId& GetOrCreateYdbProxy(TCredentialsKey&& key, Args&&... args) {
        auto it = YdbProxies.find(key);
        if (it == YdbProxies.end()) {
            auto ydbProxy = Register(CreateYdbProxy(key.Endpoint(), key.Database(), std::forward<Args>(args)...));
            auto res = YdbProxies.emplace(std::move(key), std::move(ydbProxy));
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
            ydbProxy = GetOrCreateYdbProxy(TCredentialsKey::FromParams(params), params.GetStaticCredentials());
            break;
        case NKikimrReplication::TConnectionParams::kOAuthToken:
            ydbProxy = GetOrCreateYdbProxy(TCredentialsKey::FromParams(params), params.GetOAuthToken());
            break;
        default:
            Y_ABORT("Unexpected credentials");
        }

        auto topicReaderSettings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName(settings.GetConsumerName())
            .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                .Path(settings.GetTopicPath())
                .AppendPartitionIds(settings.GetTopicPartitionId())
            );

        return [ydbProxy, settings = std::move(topicReaderSettings)]() {
            return CreateRemoteTopicReader(ydbProxy, settings);
        };
    }

    static std::function<IActor*(void)> WriterFn(const NKikimrReplication::TLocalTableWriterSettings& settings) {
        return [tablePathId = PathIdFromPathId(settings.GetPathId())]() {
            return CreateLocalTableWriter(tablePathId);
        };
    }

    void Handle(TEvService::TEvRunWorker::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& controller = record.GetController();

        auto it = Sessions.find(controller.GetTabletId());
        if (it == Sessions.end()) {
            return;
        }

        auto& session = it->second;
        if (session.GetGeneration() != controller.GetGeneration()) {
            return;
        }

        const auto& id = TWorkerId::Parse(record.GetWorker());
        if (session.HasWorker(id)) {
            return;
        }

        const auto& cmd = record.GetCommand();
        // TODO: validate settings
        const auto& readerSettings = cmd.GetRemoteTopicReader();
        const auto& writerSettings = cmd.GetLocalTableWriter();
        session.RegisterWorker(this, id, CreateWorker(ReaderFn(readerSettings), WriterFn(writerSettings)));
    }

    void Handle(TEvService::TEvStopWorker::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& controller = record.GetController();

        auto it = Sessions.find(controller.GetTabletId());
        if (it == Sessions.end()) {
            return;
        }

        auto& session = it->second;
        if (session.GetGeneration() != controller.GetGeneration()) {
            return;
        }

        const auto& id = TWorkerId::Parse(record.GetWorker());
        if (session.HasWorker(id)) {
            session.StopWorker(this, id);
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
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    TActorId BoardPublisher;
    THashMap<ui64, TSessionInfo> Sessions;
    THashMap<TCredentialsKey, TActorId> YdbProxies;

}; // TReplicationService

} // NService

IActor* CreateReplicationService() {
    return new NService::TReplicationService();
}

}
