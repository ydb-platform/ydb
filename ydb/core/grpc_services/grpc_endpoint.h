#pragma once
#include "defs.h"

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;

struct TGrpcEndpointDescription : public TThrRefBase {
    TString Address;
    ui32 Port = 0;
    bool Ssl = false;

    TVector<TString> AddressesV4;
    TVector<TString> AddressesV6;
    TString TargetNameOverride;

    TVector<TString> ServedServices;
    TVector<TString> ServedDatabases;
    TString EndpointId;
};

IActor* CreateGrpcEndpointPublishActor(TGrpcEndpointDescription *description);
IActor* CreateGrpcPublisherServiceActor(TVector<TIntrusivePtr<TGrpcEndpointDescription>>&& endpoints);

inline TActorId CreateGrpcPublisherServiceActorId() {
    const auto actorId = TActorId(0, "GrpcPublishS");
    return actorId;
}

const static TString KafkaEndpointId = "KafkaProxy";
}
}
