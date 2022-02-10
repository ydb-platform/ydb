#include "grpc_endpoint.h"

#include <util/generic/map.h>
#include <util/generic/set.h>

#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/interconnect/interconnect.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/location.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/mind/tenant_pool.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;

class TGRpcEndpointPublishActor : public TActorBootstrapped<TGRpcEndpointPublishActor> {
    TIntrusivePtr<TGrpcEndpointDescription> Description;

    TString SelfDatacenter;

    bool resolvedState;
    TMap<TActorId, TSet<TString>> ServedDatabases;
    TMap<TString, TActorId> PublishedDatabases;

    TActorId CreatePublishActor(const TString &database, TString &payload, ui32 nodeId) {
        auto *domains = AppData()->DomainsInfo.Get();
        auto domainName = ExtractDomain(database);
        auto *domainInfo = domains->GetDomainByName(domainName);
        if (!domainInfo)
            return TActorId();

        auto statestorageGroupId = domainInfo->DefaultStateStorageGroup;
        auto assignedPath = MakeEndpointsBoardPath(database);
        TActorId &aid = PublishedDatabases[database];
        if (!aid) {
            if (!payload) {
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
                for (const auto &service : Description->ServedServices)
                    entry.AddServices(service);
                Y_VERIFY(entry.SerializeToString(&payload));
            }

            aid = Register(CreateBoardPublishActor(assignedPath, payload, SelfId(), statestorageGroupId, 0, true));
        }
        return aid;
    }

    void Handle(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev) {
        const auto &record = ev->Get()->Record;

        auto &served = ServedDatabases[ev->Sender];
        auto toRemove = std::move(served);
        served = TSet<TString>();

        TString payload;

        for (auto &x : record.GetSlots()) {
            if (const TString &assignedDatabase = x.GetAssignedTenant()) {
                if (toRemove.erase(assignedDatabase) == 0)
                    CreatePublishActor(assignedDatabase, payload, SelfId().NodeId());

                served.insert(assignedDatabase);
            }
        }

        for (auto &x : toRemove) {
            auto it = PublishedDatabases.find(x);
            if (it != PublishedDatabases.end()) {
                LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Stop publish endpoints for database: " << x);
                Send(it->second, new TEvents::TEvPoisonPill());
                PublishedDatabases.erase(it);
            } else {
                LOG_CRIT_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "multiple eviction of " << x << " database. Ignoring");
            }
        }
    }

    void PassAway() override {
        if (resolvedState) {
            Send(MakeTenantPoolRootID(), new TEvents::TEvUnsubscribe());
            for(const auto& x: PublishedDatabases) {
                LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Stop publish endpoints for database: " << x.first);
                Send(x.second, new TEvents::TEvPoisonPill());
            }
        }

        TActor::PassAway();
    }

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev) {
        auto *msg = ev->Get();
        if (msg->Node && msg->Node->Location.GetDataCenterId())
            SelfDatacenter = msg->Node->Location.GetDataCenterId();

        if (Description->ServedDatabases) {
            TString payload;
            for (auto &x : Description->ServedDatabases) {
                ServedDatabases[TActorId()].insert(x);
                CreatePublishActor(x, payload, SelfId().NodeId());
            }
        }

        Send(MakeTenantPoolRootID(), new TEvents::TEvSubscribe());
        Become(&TThis::StateWork);
        resolvedState = true;
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_ENDPOINT_PUBLISH;
    }

    TGRpcEndpointPublishActor(TGrpcEndpointDescription *desc)
        : Description(desc)
        , resolvedState(false)
    {}

    void Bootstrap() {
        Become(&TThis::StateResolveDC);
        if (!Description || !Description->Port)
            return; // leave in zombie state for now

        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));
    }

    STFUNC(StateResolveDC) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STFUNC(StateWork) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTenantPool::TEvTenantPoolStatus, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateGrpcEndpointPublishActor(TGrpcEndpointDescription *description) {
    return new TGRpcEndpointPublishActor(description);
}

}
}
