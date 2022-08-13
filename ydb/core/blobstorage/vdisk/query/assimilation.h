#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

namespace NKikimr {

    IActor *CreateAssimilationActor(THullDsSnap&& snap, TEvBlobStorage::TEvVAssimilate::TPtr& ev, TVDiskID vdiskId);

} // NKikimr
