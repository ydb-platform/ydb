#include "service_id.h"

namespace NKikimr::NStat {

NActors::TActorId MakeStatServiceID(ui32 node) {
    const char x[12] = "StatService";
    return NActors::TActorId(node, TStringBuf(x, 12));
}

}   // namespace NKikimr::NStat
