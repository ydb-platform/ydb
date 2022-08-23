#include "json_handlers.h"

#include "json_vdiskstat.h"
#include "json_getblob.h"


namespace NKikimr::NViewer {

template <>
void TVDiskJsonHandlers::Init() {
    Router.RegisterGetHandler("/vdisk/json/vdiskstat", std::make_shared<TJsonHandler<TJsonVDiskStat>>());
    Router.RegisterGetHandler("/vdisk/json/getblob", std::make_shared<TJsonHandler<TJsonGetBlob>>());
}

}
