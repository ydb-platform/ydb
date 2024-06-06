#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include "json_handlers.h"

#include "json_vdiskstat.h"
#include "json_getblob.h"
#include "json_blobindexstat.h"
#include "json_vdisk_evict.h"

namespace NKikimr::NViewer {

void InitVDiskJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/vdisk/vdiskstat", new TJsonHandler<TJsonVDiskStat>);
    jsonHandlers.AddHandler("/vdisk/getblob", new TJsonHandler<TJsonGetBlob>);
    jsonHandlers.AddHandler("/vdisk/blobindexstat", new TJsonHandler<TJsonBlobIndexStat>);
    jsonHandlers.AddHandler("/vdisk/evict", new TJsonHandler<TJsonVDiskEvict>);
}

}
