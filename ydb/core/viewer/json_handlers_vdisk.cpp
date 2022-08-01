#include "json_handlers.h"

#include "json_vdiskstat.h"
#include "json_getblob.h"


namespace NKikimr::NViewer {

template <>
void TVDiskJsonHadlers::Init() {
    JsonHandlers["vdisk/json/vdiskstat"] = new TJsonHandler<TJsonVDiskStat>;
    JsonHandlers["vdisk/json/getblob"] = new TJsonHandler<TJsonGetBlob>;
}

}
