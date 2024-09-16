#include "json_handlers.h"
#include "vdisk_vdiskstat.h"
#include "vdisk_blobindexstat.h"
#include "vdisk_getblob.h"
#include "vdisk_evict.h"

namespace NKikimr::NViewer {

void InitVDiskStatJsonHandler(TJsonHandlers& handlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "vdisk",
        .Summary = "VDisk statistic",
        .Description = "VDisk statistic",
    });
    yaml.SetParameters(TJsonVDiskStat::GetParameters());
    yaml.SetResponseSchema(TJsonVDiskStat::GetSchema());
    handlers.AddHandler("/vdisk/vdiskstat", new TJsonHandler<TJsonVDiskStat>(yaml));
}

void InitVDiskGetBlobJsonHandler(TJsonHandlers& handlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "vdisk",
        .Summary = "Get blob from VDisk",
        .Description = "Get blob from VDisk",
    });
    yaml.SetParameters(TJsonGetBlob::GetParameters());
    yaml.SetResponseSchema(TJsonGetBlob::GetSchema());
    handlers.AddHandler("/vdisk/getblob", new TJsonHandler<TJsonGetBlob>(yaml));
}

void InitVDiskBlobIndexStatJsonHandler(TJsonHandlers& jsonHandlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "vdisk",
        .Summary = "Get logoblob index stat from VDisk",
        .Description = "Get logoblob index stat from VDisk",
    });
    yaml.SetParameters(TJsonBlobIndexStat::GetParameters());
    yaml.SetResponseSchema(TJsonBlobIndexStat::GetSchema());
    jsonHandlers.AddHandler("/vdisk/blobindexstat", new TJsonHandler<TJsonBlobIndexStat>(yaml));
}

void InitVDiskEvictJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/vdisk/evict", new TJsonHandler<TJsonVDiskEvict>(TJsonVDiskEvict::GetSwagger()));
}

void InitVDiskJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitVDiskStatJsonHandler(jsonHandlers);
    InitVDiskGetBlobJsonHandler(jsonHandlers);
    InitVDiskBlobIndexStatJsonHandler(jsonHandlers);
    InitVDiskEvictJsonHandler(jsonHandlers);
}

}
