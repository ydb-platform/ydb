#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/actors/core/actorsystem.h>

namespace NKikimr {

NMonitoring::IMonPage* CreateLoginPage(NActors::TActorSystem* actorSystem, const TString& path = "login");
NMonitoring::IMonPage* CreateLogoutPage(NActors::TActorSystem* actorSystem, const TString& path = "logout");

}
