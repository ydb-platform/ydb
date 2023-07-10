#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include "json_handlers.h"

#include "json_vdiskstat.h"
#include "json_getblob.h"
#include "json_blobindexstat.h"


namespace NKikimr::NViewer {

template <>
void TVDiskJsonHandlers::Init() {
    JsonHandlers["vdisk/json/vdiskstat"] = new TJsonHandler<TJsonVDiskStat>;
    JsonHandlers["vdisk/json/getblob"] = new TJsonHandler<TJsonGetBlob>;
    JsonHandlers["vdisk/json/blobindexstat"] = new TJsonHandler<TJsonBlobIndexStat>;
}

}
