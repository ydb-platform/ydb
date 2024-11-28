#include "grpc_endpoint.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/location.h>
#include <ydb/core/base/statestorage.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

class TGRpcEndpointPublishActor : public TActorBootstrapped<TGRpcEndpointPublishActor> {
    TIntrusivePtr<TGrpcEndpointDescription> Description;
    TString SelfDatacenter;
    TActorId PublishActor;

    void CreatePublishActor() {
        ui32 nodeId = SelfId().NodeId();
        TString database = AppData()->TenantName;

        auto *domains = AppData()->DomainsInfo.Get();
        auto domainName = ExtractDomain(database);
        auto *domainInfo = domains->GetDomainByName(domainName);
        if (!domainInfo)
            return;

        auto assignedPath = MakeEndpointsBoardPath(database);
        TString payload;
        NKikimrStateStorage::TEndpointBoardEntry entry;
        entry.SetAddress(Description->Address);
        entry.SetPort(Description->Port);
        entry.SetLoad(0.0f);
        entry.SetSsl(Description->Ssl);
        entry.MutableServices()->Reserve(Description->ServedServices.size());
        entry.SetDataCenter(SelfDatacenter);
        entry.SetNodeId(nodeId);
        for (const auto& addr : Description->AddressesV4) {
            entry.AddAddressesV4(addr);
        }
        for (const auto& addr : Description->AddressesV6) {
            entry.AddAddressesV6(addr);
        }
        if (Description->TargetNameOverride) {
            entry.SetTargetNameOverride(Description->TargetNameOverride);
        }
        if (Description->EndpointId) {
            entry.SetEndpointId(Description->EndpointId);
        }
        for (const auto &service : Description->ServedServices)
            entry.AddServices(service);

        Y_ABORT_UNLESS(entry.SerializeToString(&payload));

        PublishActor = Register(CreateBoardPublishActor(assignedPath, payload, SelfId(), 0, true));
    }

    void PassAway() override {
        if (PublishActor) {
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Stop publish endpoints for database: " << AppData()->TenantName);
            Send(PublishActor, new TEvents::TEvPoisonPill());
        }

        TActor::PassAway();
    }

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev) {
        auto *msg = ev->Get();
        if (msg->Node && msg->Node->Location.GetDataCenterId())
            SelfDatacenter = msg->Node->Location.GetDataCenterId();

        CreatePublishActor();
        Become(&TThis::StateWork);
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_ENDPOINT_PUBLISH;
    }

    TGRpcEndpointPublishActor(TGrpcEndpointDescription *desc)
        : Description(desc)
    {}

    void Bootstrap() {
        Become(&TThis::StateResolveDC);
        if (!Description || !Description->Port)
            return; // leave in zombie state for now

        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));
    }

    STFUNC(StateResolveDC) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateGrpcEndpointPublishActor(TGrpcEndpointDescription *description) {
    return new TGRpcEndpointPublishActor(description);
}

} // NKikimr::NGRpcService
