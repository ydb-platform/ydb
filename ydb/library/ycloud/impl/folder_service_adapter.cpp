#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/events.h>
#include <ydb/library/folder_service/mock/mock_folder_service.h>

#include <ydb/library/ycloud/impl/folder_service.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/stream/file.h>

namespace {

class TFolderServiceRequestHandler : public NActors::TActor<TFolderServiceRequestHandler> {
    using TThis = TFolderServiceRequestHandler;
    using TBase = NActors::TActor<TFolderServiceRequestHandler>;

    NActors::TActorId Sender;
    NActors::TActorId Delegatee;

    void Handle(NKikimr::NFolderService::TEvFolderService::TEvGetFolderRequest::TPtr& ev) {
        auto request = std::make_unique<NCloud::TEvFolderService::TEvListFolderRequest>();
        request->Request.set_id(ev->Get()->Request.folder_id());
        request->Token = ev->Get()->Token;
        request->RequestId = ev->Get()->RequestId;
        Send(Delegatee, request.release());
    }

    void Handle(NCloud::TEvFolderService::TEvListFolderResponse::TPtr& ev) {
        auto responseEvent = std::make_unique<NKikimr::NFolderService::TEvFolderService::TEvGetFolderResponse>();

        const auto& status = ev->Get()->Status;
        responseEvent->Status = status;

        if (status.Ok()) {
            const auto& response = ev->Get()->Response;
            if (response.result_size() > 0) {
                Y_VERIFY(responseEvent->Response.mutable_folder()->ParseFromString(response.result(0).SerializeAsString()));
            }
        }

        Send(Sender, responseEvent.release());
        PassAway();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr&) {
        Y_FAIL("Can't deliver local message");
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKikimr::NFolderService::TEvFolderService::TEvGetFolderRequest, Handle);
            hFunc(NCloud::TEvFolderService::TEvListFolderResponse, Handle);
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway);
        }
    }

public:
    TFolderServiceRequestHandler(NActors::TActorId sender, NActors::TActorId delegatee)
        : TBase(&TThis::StateWork)
        , Sender(sender)
        , Delegatee(delegatee) {
    }
};
}; // namespace

namespace NKikimr::NFolderService {

class TFolderServiceAdapter : public NActors::TActorBootstrapped<TFolderServiceAdapter> {
    using TThis = TFolderServiceAdapter;
    using TBase = NActors::TActor<TFolderServiceAdapter>;

    NKikimrProto::NFolderService::TFolderServiceConfig Config;
    NActors::TActorId Delegatee;

    void Handle(::NKikimr::NFolderService::TEvFolderService::TEvGetFolderRequest::TPtr& ev) {
        auto actor = Register(new TFolderServiceRequestHandler(ev->Sender, Delegatee));
        Send(actor, ev->Release().Release());
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(::NKikimr::NFolderService::TEvFolderService::TEvGetFolderRequest, Handle)
            cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway)
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FOLDER_SERVICE_ACTOR;
    }

    void Bootstrap() {
        NCloud::TFolderServiceSettings settings;
        settings.Endpoint = Config.GetEndpoint();
        settings.CertificateRootCA = TUnbufferedFileInput(Config.GetPathToRootCA()).ReadAll();
        Delegatee = Register(NCloud::CreateFolderServiceWithCache(settings));
        Become(&TThis::StateWork);
    }

    explicit TFolderServiceAdapter(const NKikimrProto::NFolderService::TFolderServiceConfig& config)
        : Config(config) {
    }
};

NActors::IActor* CreateFolderServiceActor(const NKikimrProto::NFolderService::TFolderServiceConfig& config) {
    if (config.GetEnable()) {
        return new NKikimr::NFolderService::TFolderServiceAdapter(config);
    } else {
        return NKikimr::NFolderService::CreateMockFolderServiceActor(config);
    }
}
} // namespace NKikimr::NFolderService
