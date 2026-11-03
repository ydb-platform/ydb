#pragma once

#include <ydb/core/security/iam_delegation/tablet/public/events.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NIamDelegation {

// A single request over Tablet Pipe. On disconnect/timeout the outcome of a
// mutation is unknown: read durable state before replaying a command. Transport
// failures return STATUS_UNSPECIFIED, never a successful tablet acknowledgement.
NActors::IActor* CreateIamDelegationTabletRequestActor(ui64 tabletId);

} // namespace NKikimr::NIamDelegation
