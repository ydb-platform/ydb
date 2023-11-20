#include "read_context.h"
#include "read_metadata.h"
#include <library/cpp/actors/core/events.h>

namespace NKikimr::NOlap {

void TActorBasedMemoryAccesor::DoOnBufferReady() {
    OwnerId.Send(OwnerId, new NActors::TEvents::TEvWakeup(1));
}


IDataReader::IDataReader(const std::shared_ptr<NOlap::TReadContext>& context)
    : Context(context) {
}

}
