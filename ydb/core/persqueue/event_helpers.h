#pragma once

#include "partition_id.h"

#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NPQ {

void ReplyPersQueueError(
    TActorId dstActor,
    const TActorContext& ctx,
    ui64 tabletId,
    const TString& topicName,
    TMaybe<TPartitionId> partition,
    NKikimr::TTabletCountersBase& counters,
    NKikimrServices::EServiceKikimr service,
    const ui64 responseCookie,
    NPersQueue::NErrorCode::EErrorCode errorCode,
    const TString& error,
    bool logDebug = false
);

}// NPQ
}// NKikimr
