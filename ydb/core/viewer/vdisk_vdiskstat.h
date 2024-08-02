#pragma once
#include "json_vdisk_req.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr::NViewer {

using TJsonVDiskStat = TJsonVDiskRequest<TEvVDiskStatRequest, TEvVDiskStatResponse>;

}
