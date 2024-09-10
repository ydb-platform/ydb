#pragma once
namespace NActors {

class TActorSystem;
struct TActorId;

}  // NActors
namespace NMVP {
namespace NOIDC {

struct TOpenIdConnectSettings;
class TContextStorage;

void InitOIDC(NActors::TActorSystem& actorSystem, const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings, TContextStorage* const contextStorage);

}  // NOIDC
}  // NMVP
