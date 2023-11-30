#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/blobstorage/vdisk/defs.h
#include <ydb/core/base/defs.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>


namespace NKikimr {

    typedef ui32 TChunkIdx;

} // NKikimr

