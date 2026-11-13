#include "processor.h"

#include "processor_impl.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NSysView {

IActor* CreateSysViewProcessor(const NActors::TActorId& tablet, TTabletStorageInfo* info) {
    return new TSysViewProcessor(tablet, info, AppData()->SysViewConfig);
}

IActor* CreateSysViewProcessorForTests(const NActors::TActorId& tablet, TTabletStorageInfo* info) {
    return new TSysViewProcessor(tablet, info, SysViewConfigForTests());
}

} // NSysView
} // NKikimr

