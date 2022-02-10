#include "processor.h"

#include "processor_impl.h"

namespace NKikimr {
namespace NSysView {

IActor* CreateSysViewProcessor(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TSysViewProcessor(tablet, info, EProcessorMode::MINUTE);
}

IActor* CreateSysViewProcessorForTests(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TSysViewProcessor(tablet, info, EProcessorMode::FAST);
}

} // NSysView
} // NKikimr

