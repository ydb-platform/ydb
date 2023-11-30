#pragma once
#include "defs.h"
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/msgbus_kv.pb.h>

namespace NKikimr {

IActor* CreateKeyValueFlat(const TActorId &tablet, TTabletStorageInfo *info);

} //NKikimr
