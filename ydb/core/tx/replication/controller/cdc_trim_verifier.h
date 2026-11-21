#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/scheme/scheme_pathid.h>

namespace NKikimrSchemeOp {
class TPersQueueGroupDescription;
}

namespace NKikimr::NReplication::NController {

IActor* CreateCdcTrimVerifier(const TActorId& parent, const TPathId& topicId, ui64 trimId,
    const NKikimrSchemeOp::TPersQueueGroupDescription& topic);

}
