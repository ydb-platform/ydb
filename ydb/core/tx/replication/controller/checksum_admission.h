#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NReplication::NController {

IActor* CreateChecksumAdmission(TActorId parent, NKikimrReplication::TEvChecksumAdmission request);

}
