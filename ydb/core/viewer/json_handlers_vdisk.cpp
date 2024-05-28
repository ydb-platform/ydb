#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include "json_handlers.h"

#include "json_vdiskstat.h"
#include "json_getblob.h"
#include "json_blobindexstat.h"


namespace NKikimr::NViewer {

void InitVDiskJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/vdisk/json/vdiskstat", new TJsonHandler<TJsonVDiskStat>);
    jsonHandlers.AddHandler("/vdisk/json/getblob", new TJsonHandler<TJsonGetBlob>);
    jsonHandlers.AddHandler("/vdisk/json/blobindexstat", new TJsonHandler<TJsonBlobIndexStat>);
}

}
