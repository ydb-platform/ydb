#if defined(KIKIMR_DISABLE_YT)
#error "yt wrapper is disabled"
#endif

#include "yt_wrapper.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/table_writer.h>

#include <util/generic/hash_set.h>

#include <mutex>

namespace NKikimr {
namespace NWrappers {

using namespace NYT;
using namespace NYT::NApi;

class TRequest;
using TRequestPtr = NYT::TIntrusivePtr<TRequest>;

struct TEvPrivate {
    enum EEv {
        EvComplete = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvCreateTableWriter,
        EvCloseTableWriter,

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

    #define DEFINE_PRIVATE_EVENT(name, result_t) \
        struct TEv##name: public TEvYtWrapper::TGenericResponse<TEv##name, Ev##name, result_t> { \
            TActorId Sender; \
            explicit TEv##name(const TResult& result, const TActorId& sender) \
                : TBase(result) \
                , Sender(sender) \
            {} \
        }

    DEFINE_PRIVATE_EVENT(CreateTableWriter, ITableWriterPtr);
    DEFINE_PRIVATE_EVENT(CloseTableWriter, void);

    #undef DEFINE_PRIVATE_EVENT

}; // TEvPrivate

template <typename TDerived> class TBaseWrapperActor;

class TRequest: public NYT::TRefCounted {
    template <typename TDerived> friend class TBaseWrapperActor;

public:
    explicit TRequest(const TActorSystem* sys, const TActorId& self, const TActorId& sender)
        : ActorSystem(sys)
        , Self(self)
        , Sender(sender)
    {
    }

    void Send(const TActorId& recipient, IEventBase* ev) {
        std::lock_guard<std::mutex> lock(RWActorSystem);

        if (ActorSystem) {
            ActorSystem->Send(recipient, ev);
        }
    }

    void Complete(const TActorId& recipient, IEventBase* ev) {
        Send(recipient, ev);
        Send(Self, new TEvPrivate::TEvComplete(NYT::MakeStrong(this)));
    }

    void Complete(IEventBase* ev) {
        Complete(Sender, ev);
    }

    const TActorId& GetSelf() const {
        return Self;
    }

    const TActorId& GetSender() const {
        return Sender;
    }

private:
    void ClearActorSystem() {
        std::lock_guard<std::mutex> lock(RWActorSystem);
        ActorSystem = nullptr;
    }

private:
    const TActorSystem* ActorSystem;
    const TActorId Self;
    const TActorId Sender;

    std::mutex RWActorSystem;

}; // TRequest

template <typename TDerived>
class TBaseWrapperActor: public TActor<TDerived> {
    void Handle(TEvPrivate::TEvComplete::TPtr& ev) {
        Requests.erase(ev->Get()->Request);
    }

    void PassAway() override {
        Requests.clear();
        IActor::PassAway();
    }

protected:
    using TActor<TDerived>::TActor;
    using TBase = TBaseWrapperActor<TDerived>;

    virtual ~TBaseWrapperActor() {
        for (auto& request : Requests) {
            request->ClearActorSystem();
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvComplete, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    TWeakPtr<TRequest> MakeRequest(const TActorId& sender) {
        auto request = New<TRequest>(TlsActivationContext->ActorSystem(), this->SelfId(), sender);
        Requests.emplace(request);
        return NYT::MakeWeak(request);
    }

private:
    THashSet<TRequestPtr> Requests;

}; // TBaseWrapperActor

class TYtTableWriter: public TBaseWrapperActor<TYtTableWriter> {
    TVector<NTableClient::TUnversionedRow> RowsRef() const {
        TVector<NTableClient::TUnversionedRow> result(Reserve(Rows.size()));

        for (const auto& row : Rows) {
            result.push_back(row);
        }

        return result;
    }

    void Handle(TEvYtWrapper::TEvGetNameTableRequest::TPtr& ev) {
        Send(ev->Sender, new TEvYtWrapper::TEvGetNameTableResponse(Writer->GetNameTable()));
    }

    void Handle(TEvYtWrapper::TEvWriteTableRequest::TPtr& ev) {
        Rows.swap(std::get<0>(*ev->Get()));
        const bool last = std::get<1>(*ev->Get());

        auto request = MakeRequest(ev->Sender);

        try {
            if (!Writer->Write(RowsRef())) {
                Writer->GetReadyEvent()
                    .Subscribe(BIND([request, last](const TErrorOr<void>& err) {
                        MaybeClose(request, last, err);
                    }));
            } else {
                MaybeClose(request, last, TErrorOr<void>());
            }
        } catch (const yexception& ex) {
            MaybeClose(request, last, ex);
        }
    }

    static void MaybeClose(TWeakPtr<TRequest> request, bool last, const TErrorOr<void>& err) {
        if (auto r = request.Lock()) {
            if (!last && err.IsOK()) {
                r->Complete(new TEvYtWrapper::TEvWriteTableResponse(err));
            } else {
                r->Complete(r->GetSelf(), new TEvPrivate::TEvCloseTableWriter(err, r->GetSender()));
            }
        }
    }

    void Handle(TEvPrivate::TEvCloseTableWriter::TPtr& ev) {
        auto request = MakeRequest(ev->Get()->Sender);

        try {
            Writer->Close().Subscribe(BIND([request, result = std::move(ev->Get()->Result)](const TErrorOr<void>& err) {
                if (auto r = request.Lock()) {
                    if (!result.IsOK()) {
                        r->Complete(new TEvYtWrapper::TEvWriteTableResponse(result));
                    } else {
                        r->Complete(new TEvYtWrapper::TEvWriteTableResponse(err));
                    }
                    r->Send(r->GetSelf(), new TEvents::TEvPoisonPill());
                }
            }));
        } catch (const yexception& ex) {
            if (auto r = request.Lock()) {
                r->Complete(new TEvYtWrapper::TEvWriteTableResponse(ex));
                r->Send(r->GetSelf(), new TEvents::TEvPoisonPill());
            }
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::YT_TABLE_WRITER_ACTOR;
    }

    explicit TYtTableWriter(ITableWriterPtr writer)
        : TBase(&TThis::StateWork)
        , Writer(writer)
    {
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYtWrapper::TEvGetNameTableRequest, Handle);
            hFunc(TEvYtWrapper::TEvWriteTableRequest, Handle);
            hFunc(TEvPrivate::TEvCloseTableWriter, Handle);

        default:
            return TBase::StateWork(ev);
        }
    }

private:
    ITableWriterPtr Writer;
    TVector<NTableClient::TUnversionedOwningRow> Rows;

}; // TYtTableWriter

class TYtWrapper: public TBaseWrapperActor<TYtWrapper> {
    static IClientPtr CreateClient(NRpcProxy::TConnectionConfigPtr config, const TClientOptions& options) {
        auto connection = NRpcProxy::CreateConnection(config);
        return connection->CreateClient(options);
    }

    template <typename TEvResponse, typename TClient, typename... Args>
    using TFunc = typename TEvResponse::TAsyncResult(TClient::*)(Args...);

    template <typename TEvResponse>
    using TCompleteFunc = void(*)(TWeakPtr<TRequest>, const typename TEvResponse::TResult&);

    template <typename TEvResponse, typename TEvRequestPtr, typename TClient, typename... Args>
    void CallImpl(TEvRequestPtr& ev, TFunc<TEvResponse, TClient, Args...> func, TCompleteFunc<TEvResponse> completer) {
        auto request = MakeRequest(ev->Sender);
        auto args = std::move(ev->Get()->GetArgs());
        auto cb = BIND([request, completer](const typename TEvResponse::TResult& result) {
            completer(request, result);
        });

        try {
            std::apply(func, std::tuple_cat(std::tie(Client), std::move(args))).Subscribe(std::move(cb));
        } catch (const yexception& ex) {
            completer(request, ex);
        }
    }

    template <typename TEvResponse>
    static void Complete(TWeakPtr<TRequest> request, const typename TEvResponse::TResult& result) {
        if (auto r = request.Lock()) {
            r->Complete(new TEvResponse(result));
        }
    }

    template <typename TEvResponse, typename TEvRequestPtr, typename TClient, typename... Args>
    void Call(TEvRequestPtr& ev, TFunc<TEvResponse, TClient, Args...> func) {
        CallImpl<TEvResponse, TEvRequestPtr, TClient, Args...>(ev, func, &TThis::Complete<TEvResponse>);
    }

    template <typename TEvPrivResponse>
    static void CompletePriv(TWeakPtr<TRequest> request, const typename TEvPrivResponse::TResult& result) {
        if (auto r = request.Lock()) {
            r->Complete(r->GetSelf(), new TEvPrivResponse(result, r->GetSender()));
        }
    }

    template <typename TEvPrivResponse, typename TEvRequestPtr, typename TClient, typename... Args>
    void CallPriv(TEvRequestPtr& ev, TFunc<TEvPrivResponse, TClient, Args...> func) {
        CallImpl<TEvPrivResponse, TEvRequestPtr, TClient, Args...>(ev, func, &TThis::CompletePriv<TEvPrivResponse>);
    }

    template <typename TEvResponse, typename TProcessor, typename TEvPrivResponsePtr>
    void HandlePriv(TEvPrivResponsePtr& ev) {
        const auto& result = ev->Get()->Result;
        if (result.IsOK()) {
            Send(ev->Get()->Sender, new TEvResponse(RegisterWithSameMailbox(new TProcessor(result.Value()))));
        } else {
            Send(ev->Get()->Sender, new TEvResponse(yexception() << ToString(result)));
        }
    }

    void Handle(TEvYtWrapper::TEvCreateTableWriterRequest::TPtr& ev) {
        CallPriv<TEvPrivate::TEvCreateTableWriter>(ev, &IClient::CreateTableWriter);
    }

    void Handle(TEvPrivate::TEvCreateTableWriter::TPtr& ev) {
        HandlePriv<TEvYtWrapper::TEvCreateTableWriterResponse, TYtTableWriter>(ev);
    }

    void Handle(TEvYtWrapper::TEvGetNodeRequest::TPtr& ev) {
        Call<TEvYtWrapper::TEvGetNodeResponse>(ev, &IClient::GetNode);
    }

    void Handle(TEvYtWrapper::TEvCreateNodeRequest::TPtr& ev) {
        Call<TEvYtWrapper::TEvCreateNodeResponse>(ev, &IClient::CreateNode);
    }

    void Handle(TEvYtWrapper::TEvNodeExistsRequest::TPtr& ev) {
        Call<TEvYtWrapper::TEvNodeExistsResponse>(ev, &IClient::NodeExists);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::YT_WRAPPER_ACTOR;
    }

    explicit TYtWrapper(NRpcProxy::TConnectionConfigPtr config, const TClientOptions& options)
        : TBase(&TThis::StateWork)
        , Client(CreateClient(config, options))
    {
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            // Tables
            hFunc(TEvYtWrapper::TEvCreateTableWriterRequest, Handle);
            hFunc(TEvPrivate::TEvCreateTableWriter, Handle);

            // Cypress
            hFunc(TEvYtWrapper::TEvGetNodeRequest, Handle);
            hFunc(TEvYtWrapper::TEvCreateNodeRequest, Handle);
            hFunc(TEvYtWrapper::TEvNodeExistsRequest, Handle);

        default:
            return TBase::StateWork(ev);
        }
    }

private:
    IClientPtr Client;

}; // TYtWrapper

IActor* CreateYtWrapper(NRpcProxy::TConnectionConfigPtr config, const TClientOptions& options) {
    return new TYtWrapper(config, options);
}

IActor* CreateYtWrapper(const TString& serverName, const TString& token) {
    auto config = New<NRpcProxy::TConnectionConfig>();
    config->ClusterUrl = serverName;
    config->ConnectionType = EConnectionType::Rpc;
    config->DynamicChannelPool->MaxPeerCount = 5;
    return CreateYtWrapper(config, TClientOptions::FromToken(token));
}

} // NWrappers
} // NKikimr
