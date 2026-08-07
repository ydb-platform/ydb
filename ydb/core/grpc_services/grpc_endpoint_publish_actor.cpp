#include "grpc_endpoint.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/location.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

class TGRpcEndpointPublishActor : public TActorBootstrapped<TGRpcEndpointPublishActor> {
    // Update period for load_factor polling from Node Whiteboard.
    // Synchronized with Node Whiteboard's internal update period (15 seconds)
    // to ensure we always get fresh data without unnecessary polling.
    static constexpr TDuration LOAD_FACTOR_UPDATE_PERIOD_SECONDS = TDuration::Seconds(15);
    
    // Minimum relative change in load_factor to trigger an update (1%).
    // This prevents excessive updates to State Storage Board when load fluctuates slightly.
    static constexpr float LOAD_FACTOR_CHANGE_THRESHOLD = 0.01f;

    TIntrusivePtr<TGrpcEndpointDescription> Description;
    TString SelfDatacenter;
    std::optional<TString> BridgePileName;
    TActorId PublishActor;
    float CurrentLoadFactor = 0.0f;

    void BuildEndpointBoardEntry(NKikimrStateStorage::TEndpointBoardEntry& entry, ui32 nodeId) {
        entry.SetAddress(Description->Address);
        entry.SetPort(Description->Port);
        entry.SetLoad(CurrentLoadFactor);
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
        for (const auto &service : Description->ServedServices) {
            entry.AddServices(service);
        }
        if (BridgePileName) {
            entry.SetBridgePileName(*BridgePileName);
        }
    }

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
        BuildEndpointBoardEntry(entry, nodeId);

        Y_ABORT_UNLESS(entry.SerializeToString(&payload));

        PublishActor = Register(CreateBoardPublishActor(assignedPath, payload, SelfId(), 0, true));
    }
    
    void UpdatePublishActor() {
        TString payload;
        NKikimrStateStorage::TEndpointBoardEntry entry;
        BuildEndpointBoardEntry(entry, SelfId().NodeId());
        
        Y_ABORT_UNLESS(entry.SerializeToString(&payload));
        
        Send(PublishActor, new TEvStateStorage::TEvBoardPublishUpdate(payload));
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
        if (msg->Node && msg->Node->Location.GetDataCenterId()) {
            SelfDatacenter = msg->Node->Location.GetDataCenterId();
        }
        if (msg->Node) {
            BridgePileName = msg->Node->Location.GetBridgePileName();
        }
        CreatePublishActor();
        Become(&TThis::StateWork);
        
        // Schedule periodic load factor updates
        Schedule(LOAD_FACTOR_UPDATE_PERIOD_SECONDS, new TEvents::TEvWakeup());
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        if (record.SystemStateInfoSize() == 0) {
            return;
        }

        const auto& systemState = record.GetSystemStateInfo(0);
        if (systemState.LoadAverageSize() == 0) {
            return;
        }

        auto loadAvg = systemState.GetLoadAverage(0);
        auto numCpus = systemState.GetNumberOfCpus();
        if (numCpus == 0) {
            return;
        }
    
        auto newLoadFactor = loadAvg / numCpus;
        if (std::abs(newLoadFactor - CurrentLoadFactor) >= LOAD_FACTOR_CHANGE_THRESHOLD) {
            CurrentLoadFactor = newLoadFactor;

            if (PublishActor) {
                UpdatePublishActor();
            } else {
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
                    "Cannot update load_factor: PublishActor is not initialized. "
                    "Database: " << AppData()->TenantName << ", "
                    "load_factor: " << newLoadFactor);
            }
        }
    }

    void Wakeup() {
        Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId()),
             new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest());
        
        Schedule(LOAD_FACTOR_UPDATE_PERIOD_SECONDS, new TEvents::TEvWakeup());
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
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            sFunc(TEvents::TEvWakeup, Wakeup);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateGrpcEndpointPublishActor(TGrpcEndpointDescription *description) {
    return new TGRpcEndpointPublishActor(description);
}

} // NKikimr::NGRpcService
