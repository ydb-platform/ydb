#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include "json_handlers.h"

#include "json_pdisk_restart.h"
#include "pdisk_info.h"


namespace NKikimr::NViewer {

void InitPDiskJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/pdisk/restart", new TJsonHandler<TJsonPDiskRestart>);
    jsonHandlers.AddHandler("/pdisk/info", new TJsonHandler<TPDiskInfo>);
}

}
