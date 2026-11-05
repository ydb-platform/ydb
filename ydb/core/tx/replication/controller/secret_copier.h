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
IActor* CreateSecretCopier(const TActorId& parent, TReplication::TPtr replication);

}
