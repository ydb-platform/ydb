#include "ydb_proxy.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/base/appdata.h>

#include <util/generic/hash_set.h>
#include <util/string/join.h>

#include <memory>
#include <mutex>

namespace NKikimr::NReplication {

using namespace NKikimrReplication;
using namespace NYdb;
using namespace NYdb::NScheme;
using namespace NYdb::NTable;
using namespace NYdb::NTopic;

void TEvYdbProxy::TReadTopicResult::TMessage::Out(IOutputStream& out) const {
    out << "{"
        << " Offset: " << Offset
        << " Data: " << Data.size() << "b"
        << " Codec: " << Codec
    << " }";
}

void TEvYdbProxy::TReadTopicResult::Out(IOutputStream& out) const {
    out << "{"
        << " PartitionId: " << PartitionId
        << " Messages [" << JoinSeq(",", Messages) << "]"
    << " }";
}

template <typename TDerived>
class TBaseProxyActor: public TActor<TDerived> {
    class TRequest;
    using TRequestPtr = std::shared_ptr<TRequest>;

protected:
    struct TEvPrivate {
        enum EEv {
            EvComplete = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvTopicEventReady,

            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvComplete: public TEventLocal<TEvComplete, EvComplete> {
            const TRequestPtr Request;

            explicit TEvComplete(const TRequestPtr& request)
                : Request(request)
            {
            }
        };

        struct TEvTopicEventReady: public TEventLocal<TEvTopicEventReady, EvTopicEventReady> {
            const TActorId Sender;
            const ui64 Cookie;

            explicit TEvTopicEventReady(const TActorId& sender, ui64 cookie)
                : Sender(sender)
                , Cookie(cookie)
            {
            }
        };

    }; // TEvPrivate

private:
    class TRequest: public std::enable_shared_from_this<TRequest> {
        friend class TBaseProxyActor<TDerived>;

    public:
        explicit TRequest(const TActorSystem* sys, const TActorId& self, const TActorId& sender, ui64 cookie)
            : ActorSystem(sys)
            , Self(self)
            , Sender(sender)
            , Cookie(cookie)
        {
        }

        void Complete(IEventBase* ev) {
            Send(Sender, ev, Cookie);
            Send(Self, new typename TEvPrivate::TEvComplete(this->shared_from_this()));
        }

    private:
        void Send(const TActorId& recipient, IEventBase* ev, ui64 cookie = 0) {
            std::lock_guard<std::mutex> lock(RWActorSystem);

            if (ActorSystem) {
                ActorSystem->Send(new IEventHandle(recipient, Self, ev, 0, cookie));
            }
        }

        void ClearActorSystem() {
            std::lock_guard<std::mutex> lock(RWActorSystem);
            ActorSystem = nullptr;
        }

    private:
        const TActorSystem* ActorSystem;
        const TActorId Self;
        const TActorId Sender;
        const ui64 Cookie;

        std::mutex RWActorSystem;

    }; // TRequest

    struct TRequestPtrHash {
        Y_FORCE_INLINE size_t operator()(const TRequestPtr& ptr) const {
            return THash<TRequest*>()(ptr.get());
        }
    };

    void Handle(typename TEvPrivate::TEvComplete::TPtr& ev) {
        Requests.erase(ev->Get()->Request);
    }

protected:
    using TActor<TDerived>::TActor;

    virtual ~TBaseProxyActor() {
        for (auto& request : Requests) {
            request->ClearActorSystem();
        }
    }

    void PassAway() override {
        Requests.clear();
        IActor::PassAway();
    }

    std::weak_ptr<TRequest> MakeRequest(const TActorId& sender, ui64 cookie = 0) {
        auto request = std::make_shared<TRequest>(TlsActivationContext->ActorSystem(), this->SelfId(), sender, cookie);
        Requests.emplace(request);
        return request;
    }

    template <typename TEvResponse>
    static void Complete(std::weak_ptr<TRequest> request, const typename TEvResponse::TResult& result) {
        if (auto r = request.lock()) {
            r->Complete(new TEvResponse(result));
        }
    }

    template <typename TEvResponse>
    static auto CreateCallback(std::weak_ptr<TRequest> request) {
        return [request](const typename TEvResponse::TAsyncResult& result) {
            Complete<TEvResponse>(request, result.GetValueSync());
        };
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvComplete, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    THashSet<TRequestPtr, TRequestPtrHash> Requests;

}; // TBaseProxyActor

class TTopicReader: public TBaseProxyActor<TTopicReader> {
    void Handle(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
        if (AutoCommit) {
            DeferredCommit.Commit();
        }
        WaitEvent(ev->Sender, ev->Cookie);
    }

    void WaitEvent(const TActorId& sender, ui64 cookie) {
        auto request = MakeRequest(SelfId());
        auto cb = [request, sender, cookie](const NThreading::TFuture<void>&) {
            if (auto r = request.lock()) {
                r->Complete(new TEvPrivate::TEvTopicEventReady(sender, cookie));
            }
        };

        Session->WaitEvent().Subscribe(std::move(cb));
    }

    void Handle(TEvPrivate::TEvTopicEventReady::TPtr& ev) {
        auto event = Session->GetEvent(false);
        if (!event) {
            return WaitEvent(ev->Get()->Sender, ev->Get()->Cookie);
        }

        if (auto* x = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            x->Confirm();
            return WaitEvent(ev->Get()->Sender, ev->Get()->Cookie);
        } else if (auto* x = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
            x->Confirm();
            return WaitEvent(ev->Get()->Sender, ev->Get()->Cookie);
        } else if (auto* x = std::get_if<TReadSessionEvent::TEndPartitionSessionEvent>(&*event)) {
            x->Confirm();
            return WaitEvent(ev->Get()->Sender, ev->Get()->Cookie);
        } else if (auto* x = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            if (AutoCommit) {
                DeferredCommit.Add(*x);
            }
            return (void)Send(ev->Get()->Sender, new TEvYdbProxy::TEvReadTopicResponse(*x), 0, ev->Get()->Cookie);
        } else if (std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&*event)) {
            return WaitEvent(ev->Get()->Sender, ev->Get()->Cookie);
        } else if (std::get_if<TReadSessionEvent::TPartitionSessionStatusEvent>(&*event)) {
            return WaitEvent(ev->Get()->Sender, ev->Get()->Cookie);
        } else if (auto* x = std::get_if<TReadSessionEvent::TPartitionSessionClosedEvent>(&*event)) {
            auto status = TStatus(EStatus::UNAVAILABLE, NYql::TIssues{NYql::TIssue(x->DebugString())});
            return Leave(ev->Get()->Sender, std::move(status));
        } else if (auto* x = std::get_if<TSessionClosedEvent>(&*event)) {
            return Leave(ev->Get()->Sender, std::move(*x));
        } else {
            Y_ABORT("Unexpected event");
        }
    }

    void Leave(const TActorId& client, TStatus&& status) {
        Send(client, new TEvYdbProxy::TEvTopicReaderGone(std::move(status)));
        PassAway();
    }

    void PassAway() override {
        Session->Close(TDuration::Zero());
        TBaseProxyActor<TTopicReader>::PassAway();
    }

public:
    explicit TTopicReader(const std::shared_ptr<IReadSession>& session, bool autoCommit)
        : TBaseProxyActor(&TThis::StateWork)
        , Session(session)
        , AutoCommit(autoCommit)
    {
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvReadTopicRequest, Handle);
            hFunc(TEvPrivate::TEvTopicEventReady, Handle);

        default:
            return StateBase(ev);
        }
    }

private:
    std::shared_ptr<IReadSession> Session;
    const bool AutoCommit;
    TDeferredCommit DeferredCommit;

}; // TTopicReader

class TYdbProxy: public TBaseProxyActor<TYdbProxy> {
    template <typename TEvResponse, typename TClient, typename... Args>
    using TFunc = typename TEvResponse::TAsyncResult(TClient::*)(Args...);

    template <typename TSettings>
    static TSettings ClientSettings(const TCommonClientSettings& base) {
        auto derived = TSettings();

        if (base.DiscoveryEndpoint_) {
            derived.DiscoveryEndpoint(*base.DiscoveryEndpoint_);
        }
        if (base.DiscoveryMode_) {
            derived.DiscoveryMode(*base.DiscoveryMode_);
        }
        if (base.Database_) {
            derived.Database(*base.Database_);
        }
        if (base.CredentialsProviderFactory_) {
            derived.CredentialsProviderFactory(*base.CredentialsProviderFactory_);
        }

        return derived;
    }

    template <typename TClient, typename TSettings>
    TClient* EnsureClient(THolder<TClient>& client) {
        if (!client) {
            Y_ABORT_UNLESS(AppData()->YdbDriver);
            client.Reset(new TClient(*AppData()->YdbDriver, ClientSettings<TSettings>(CommonSettings)));
        }

        return client.Get();
    }

    template <typename TClient>
    TClient* EnsureClient() {
        if constexpr (std::is_same_v<TClient, TSchemeClient>) {
            return EnsureClient<TClient, TCommonClientSettings>(SchemeClient);
        } else if constexpr (std::is_same_v<TClient, TTableClient>) {
            return EnsureClient<TClient, TClientSettings>(TableClient);
        } else if constexpr (std::is_same_v<TClient, TTopicClient>) {
            return EnsureClient<TClient, TTopicClientSettings>(TopicClient);
        } else {
            Y_ABORT("unreachable");
        }
    }

    template <typename TEvResponse, typename TEvRequestPtr, typename TClient, typename... Args>
    void Call(TEvRequestPtr& ev, TFunc<TEvResponse, TClient, Args...> func) {
        auto* client = EnsureClient<TClient>();
        auto request = MakeRequest(ev->Sender, ev->Cookie);
        auto args = std::move(ev->Get()->GetArgs());
        auto cb = CreateCallback<TEvResponse>(request);

        std::apply(func, std::tuple_cat(std::tie(client), std::move(args))).Subscribe(std::move(cb));
    }

    template <typename TEvResponse, typename TEvRequestPtr, typename TSession, typename... Args>
    void CallSession(TEvRequestPtr& ev, TFunc<TEvResponse, TSession, Args...> func) {
        auto* client = EnsureClient<TTableClient>();
        auto request = MakeRequest(ev->Sender, ev->Cookie);
        auto args = std::move(ev->Get()->GetArgs());
        auto cb = [request, func, args = std::move(args)](const TAsyncCreateSessionResult& result) {
            auto sessionResult = result.GetValueSync();
            if (!sessionResult.IsSuccess()) {
                return Complete<TEvYdbProxy::TEvCreateSessionResponse>(request, sessionResult);
            }

            auto session = sessionResult.GetSession();
            auto cb = CreateCallback<TEvResponse>(request);
            std::apply(func, std::tuple_cat(std::tie(session), std::move(args))).Subscribe(std::move(cb));
        };

        client->GetSession().Subscribe(std::move(cb));
    }

    void Handle(TEvYdbProxy::TEvMakeDirectoryRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvMakeDirectoryResponse>(ev, &TSchemeClient::MakeDirectory);
    }

    void Handle(TEvYdbProxy::TEvRemoveDirectoryRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvRemoveDirectoryResponse>(ev, &TSchemeClient::RemoveDirectory);
    }

    void Handle(TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvDescribePathResponse>(ev, &TSchemeClient::DescribePath);
    }

    void Handle(TEvYdbProxy::TEvListDirectoryRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvListDirectoryResponse>(ev, &TSchemeClient::ListDirectory);
    }

    void Handle(TEvYdbProxy::TEvModifyPermissionsRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvModifyPermissionsResponse>(ev, &TSchemeClient::ModifyPermissions);
    }

    void Handle(TEvYdbProxy::TEvCreateTableRequest::TPtr& ev) {
        CallSession<TEvYdbProxy::TEvCreateTableResponse>(ev, &TSession::CreateTable);
    }

    void Handle(TEvYdbProxy::TEvDropTableRequest::TPtr& ev) {
        CallSession<TEvYdbProxy::TEvDropTableResponse>(ev, &TSession::DropTable);
    }

    void Handle(TEvYdbProxy::TEvAlterTableRequest::TPtr& ev) {
        CallSession<TEvYdbProxy::TEvAlterTableResponse>(ev, &TSession::AlterTable);
    }

    void Handle(TEvYdbProxy::TEvCopyTableRequest::TPtr& ev) {
        CallSession<TEvYdbProxy::TEvCopyTableResponse>(ev, &TSession::CopyTable);
    }

    void Handle(TEvYdbProxy::TEvCopyTablesRequest::TPtr& ev) {
        CallSession<TEvYdbProxy::TEvCopyTablesResponse>(ev, &TSession::CopyTables);
    }

    void Handle(TEvYdbProxy::TEvRenameTablesRequest::TPtr& ev) {
        CallSession<TEvYdbProxy::TEvRenameTablesResponse>(ev, &TSession::RenameTables);
    }

    void Handle(TEvYdbProxy::TEvDescribeTableRequest::TPtr& ev) {
        CallSession<TEvYdbProxy::TEvDescribeTableResponse>(ev, &TSession::DescribeTable);
    }

    void Handle(TEvYdbProxy::TEvCreateTopicRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvCreateTopicResponse>(ev, &TTopicClient::CreateTopic);
    }

    void Handle(TEvYdbProxy::TEvAlterTopicRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvAlterTopicResponse>(ev, &TTopicClient::AlterTopic);
    }

    void Handle(TEvYdbProxy::TEvDropTopicRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvDropTopicResponse>(ev, &TTopicClient::DropTopic);
    }

    void Handle(TEvYdbProxy::TEvDescribeTopicRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvDescribeTopicResponse>(ev, &TTopicClient::DescribeTopic);
    }

    void Handle(TEvYdbProxy::TEvDescribeConsumerRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvDescribeConsumerResponse>(ev, &TTopicClient::DescribeConsumer);
    }

    void Handle(TEvYdbProxy::TEvCreateTopicReaderRequest::TPtr& ev) {
        auto* client = EnsureClient<TTopicClient>();
        auto args = std::move(ev->Get()->GetArgs());
        const auto& settings = std::get<TEvYdbProxy::TTopicReaderSettings>(args);
        auto session = std::invoke(&TTopicClient::CreateReadSession, client, settings.GetBase());
        auto reader = RegisterWithSameMailbox(new TTopicReader(session, settings.AutoCommit_));
        Send(ev->Sender, new TEvYdbProxy::TEvCreateTopicReaderResponse(reader));
    }

    void Handle(TEvYdbProxy::TEvCommitOffsetRequest::TPtr& ev) {
        Call<TEvYdbProxy::TEvCommitOffsetResponse>(ev, &TTopicClient::CommitOffset);
    }

    static TCommonClientSettings MakeSettings(const TString& endpoint, const TString& database, bool ssl) {
        return TCommonClientSettings()
            .DiscoveryEndpoint(endpoint)
            .DiscoveryMode(EDiscoveryMode::Async)
            .Database(database)
            .SslCredentials(ssl);
    }

    static TCommonClientSettings MakeSettings(const TString& endpoint, const TString& database, bool ssl, const TString& token) {
        return MakeSettings(endpoint, database, ssl)
            .AuthToken(token);
    }

    static TCommonClientSettings MakeSettings(const TString& endpoint, const TString& database, bool ssl, const TStaticCredentials& credentials) {
        return MakeSettings(endpoint, database, ssl)
            .CredentialsProviderFactory(CreateLoginCredentialsProviderFactory({
                .User = credentials.GetUser(),
                .Password = credentials.GetPassword(),
            }));
    }

public:
    template <typename... Args>
    explicit TYdbProxy(Args&&... args)
        : TBaseProxyActor(&TThis::StateWork)
        , CommonSettings(MakeSettings(std::forward<Args>(args)...))
    {
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            // Scheme
            hFunc(TEvYdbProxy::TEvMakeDirectoryRequest, Handle);
            hFunc(TEvYdbProxy::TEvRemoveDirectoryRequest, Handle);
            hFunc(TEvYdbProxy::TEvDescribePathRequest, Handle);
            hFunc(TEvYdbProxy::TEvListDirectoryRequest, Handle);
            hFunc(TEvYdbProxy::TEvModifyPermissionsRequest, Handle);
            // Table
            hFunc(TEvYdbProxy::TEvCreateTableRequest, Handle);
            hFunc(TEvYdbProxy::TEvDropTableRequest, Handle);
            hFunc(TEvYdbProxy::TEvAlterTableRequest, Handle);
            hFunc(TEvYdbProxy::TEvCopyTableRequest, Handle);
            hFunc(TEvYdbProxy::TEvCopyTablesRequest, Handle);
            hFunc(TEvYdbProxy::TEvRenameTablesRequest, Handle);
            hFunc(TEvYdbProxy::TEvDescribeTableRequest, Handle);
            // Topic
            hFunc(TEvYdbProxy::TEvCreateTopicRequest, Handle);
            hFunc(TEvYdbProxy::TEvAlterTopicRequest, Handle);
            hFunc(TEvYdbProxy::TEvDropTopicRequest, Handle);
            hFunc(TEvYdbProxy::TEvDescribeTopicRequest, Handle);
            hFunc(TEvYdbProxy::TEvDescribeConsumerRequest, Handle);
            hFunc(TEvYdbProxy::TEvCreateTopicReaderRequest, Handle);
            hFunc(TEvYdbProxy::TEvCommitOffsetRequest, Handle);

        default:
            return StateBase(ev);
        }
    }

private:
    const TCommonClientSettings CommonSettings;
    THolder<TSchemeClient> SchemeClient;
    THolder<TTableClient> TableClient;
    THolder<TTopicClient> TopicClient;

}; // TYdbProxy

IActor* CreateYdbProxy(const TString& endpoint, const TString& database, bool ssl) {
    return new TYdbProxy(endpoint, database, ssl);
}

IActor* CreateYdbProxy(const TString& endpoint, const TString& database, bool ssl, const TString& token) {
    return new TYdbProxy(endpoint, database, ssl, token);
}

IActor* CreateYdbProxy(const TString& endpoint, const TString& database, bool ssl, const TStaticCredentials& credentials) {
    return new TYdbProxy(endpoint, database, ssl, credentials);
}

}

Y_DECLARE_OUT_SPEC(, NKikimr::NReplication::TEvYdbProxy::TReadTopicResult::TMessage, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NKikimr::NReplication::TEvYdbProxy::TReadTopicResult, o, x) {
    return x.Out(o);
}
