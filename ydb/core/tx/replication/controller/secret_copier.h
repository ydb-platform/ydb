#pragma once

#include "replication.h"

namespace NKikimrReplication {
    class TSecretRecord;
}

namespace NKikimr::NReplication {
    class TTopicMessage;
}

namespace NKikimr::NReplication::NController {

bool DecodeSecretRecord(const TTopicMessage& message, NKikimrReplication::TSecretRecord& record);
// The factory snapshots controller state before the actor gets its own mailbox.
IActor* CreateSecretCopier(const TActorId& parent, const TReplication& replication, bool recoverOnly = false);

}
