#include <library/cpp/monlib/service/pages/mon_page.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {

inline NActors::TActorId MakeWebLoginServiceId() {
    const char name[12] = "webloginsvc";
    return NActors::TActorId(0, TStringBuf(name, 12));
}

NActors::IActor* CreateWebLoginService();

}
