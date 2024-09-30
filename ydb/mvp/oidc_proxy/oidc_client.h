#pragma once
namespace NActors {

class TActorSystem;
struct TActorId;

}  // NActors
namespace NMVP {
namespace NOIDC {

struct TOpenIdConnectSettings;

void InitOIDC(NActors::TActorSystem& actorSystem, const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);

}  // NOIDC
}  // NMVP
