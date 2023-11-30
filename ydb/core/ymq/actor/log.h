#pragma once
#include "defs.h"
#include <ydb/library/actors/core/log.h>

#include <util/stream/output.h>

#include <limits>

namespace NKikimr::NSQS {

struct TQueuePath;

ui64 RequestIdSample(const TStringBuf& requestId);

// Lightweight class for simple logging queue name
class TLogQueueName {
public:
    TLogQueueName(const TString& userName, const TString& queueName, ui64 shard = std::numeric_limits<ui64>::max());
    TLogQueueName(const TQueuePath& queuePath, ui64 shard = std::numeric_limits<ui64>::max());

    void OutTo(IOutputStream& out) const;

private:
    const TString& UserName;
    const TString& QueueName;
    const ui64 Shard;
};

} // namespace NKikimr::NSQS


//
// Outside actor system
//

#define LOG_SQS_BASE(actorCtxOrSystem, priority, stream) \
    LOG_LOG_S(actorCtxOrSystem, priority, NKikimrServices::SQS, stream)

#define RLOG_SQS_REQ_BASE(actorCtxOrSystem, priority, requestId, stream) \
    LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, priority, NKikimrServices::SQS, NKikimr::NSQS::RequestIdSample(requestId), "Request [" << requestId << "] " << stream)

#define RLOG_SQS_BASE(actorCtxOrSystem, priority, stream) \
    RLOG_SQS_REQ_BASE(actorCtxOrSystem, priority, RequestId_, stream)


// Log under SQS service component
#define LOG_SQS_BASE_EMERG(actorCtxOrSystem, stream)  LOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_EMERG, stream)
#define LOG_SQS_BASE_ALERT(actorCtxOrSystem, stream)  LOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_ALERT, stream)
#define LOG_SQS_BASE_CRIT(actorCtxOrSystem, stream)   LOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_CRIT, stream)
#define LOG_SQS_BASE_ERROR(actorCtxOrSystem, stream)  LOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_ERROR, stream)
#define LOG_SQS_BASE_WARN(actorCtxOrSystem, stream)   LOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_WARN, stream)
#define LOG_SQS_BASE_NOTICE(actorCtxOrSystem, stream) LOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_NOTICE, stream)
#define LOG_SQS_BASE_INFO(actorCtxOrSystem, stream)   LOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_INFO, stream)
#define LOG_SQS_BASE_DEBUG(actorCtxOrSystem, stream)  LOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_DEBUG, stream)
#define LOG_SQS_BASE_TRACE(actorCtxOrSystem, stream)  LOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_TRACE, stream)

// Log with explicitly specified request id
#define RLOG_SQS_REQ_BASE_EMERG(actorCtxOrSystem, requestId, stream)  RLOG_SQS_REQ_BASE(actorCtxOrSystem, NActors::NLog::PRI_EMERG, requestId, stream)
#define RLOG_SQS_REQ_BASE_ALERT(actorCtxOrSystem, requestId, stream)  RLOG_SQS_REQ_BASE(actorCtxOrSystem, NActors::NLog::PRI_ALERT, requestId, stream)
#define RLOG_SQS_REQ_BASE_CRIT(actorCtxOrSystem, requestId, stream)   RLOG_SQS_REQ_BASE(actorCtxOrSystem, NActors::NLog::PRI_CRIT, requestId, stream)
#define RLOG_SQS_REQ_BASE_ERROR(actorCtxOrSystem, requestId, stream)  RLOG_SQS_REQ_BASE(actorCtxOrSystem, NActors::NLog::PRI_ERROR, requestId, stream)
#define RLOG_SQS_REQ_BASE_WARN(actorCtxOrSystem, requestId, stream)   RLOG_SQS_REQ_BASE(actorCtxOrSystem, NActors::NLog::PRI_WARN, requestId, stream)
#define RLOG_SQS_REQ_BASE_NOTICE(actorCtxOrSystem, requestId, stream) RLOG_SQS_REQ_BASE(actorCtxOrSystem, NActors::NLog::PRI_NOTICE, requestId, stream)
#define RLOG_SQS_REQ_BASE_INFO(actorCtxOrSystem, requestId, stream)   RLOG_SQS_REQ_BASE(actorCtxOrSystem, NActors::NLog::PRI_INFO, requestId, stream)
#define RLOG_SQS_REQ_BASE_DEBUG(actorCtxOrSystem, requestId, stream)  RLOG_SQS_REQ_BASE(actorCtxOrSystem, NActors::NLog::PRI_DEBUG, requestId, stream)
#define RLOG_SQS_REQ_BASE_TRACE(actorCtxOrSystem, requestId, stream)  RLOG_SQS_REQ_BASE(actorCtxOrSystem, NActors::NLog::PRI_TRACE, requestId, stream)

// Log with imlicitly specified request id (RequestId_ member)
#define RLOG_SQS_BASE_EMERG(actorCtxOrSystem, stream)  RLOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_EMERG, stream)
#define RLOG_SQS_BASE_ALERT(actorCtxOrSystem, stream)  RLOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_ALERT, stream)
#define RLOG_SQS_BASE_CRIT(actorCtxOrSystem, stream)   RLOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_CRIT, stream)
#define RLOG_SQS_BASE_ERROR(actorCtxOrSystem, stream)  RLOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_ERROR, stream)
#define RLOG_SQS_BASE_WARN(actorCtxOrSystem, stream)   RLOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_WARN, stream)
#define RLOG_SQS_BASE_NOTICE(actorCtxOrSystem, stream) RLOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_NOTICE, stream)
#define RLOG_SQS_BASE_INFO(actorCtxOrSystem, stream)   RLOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_INFO, stream)
#define RLOG_SQS_BASE_DEBUG(actorCtxOrSystem, stream)  RLOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_DEBUG, stream)
#define RLOG_SQS_BASE_TRACE(actorCtxOrSystem, stream)  RLOG_SQS_BASE(actorCtxOrSystem, NActors::NLog::PRI_TRACE, stream)



//
// Inside actor system
//

#define LOG_SQS(priority, stream) \
    LOG_SQS_BASE(TActivationContext::AsActorContext(), priority, stream)

#define RLOG_SQS_REQ(priority, requestId, stream) \
    RLOG_SQS_REQ_BASE(TActivationContext::AsActorContext(), priority, requestId, stream)

#define RLOG_SQS(priority, stream) \
    RLOG_SQS_BASE(TActivationContext::AsActorContext(), priority, stream)


// Log under SQS service component
#define LOG_SQS_EMERG(stream)  LOG_SQS_BASE_EMERG(TActivationContext::AsActorContext(), stream)
#define LOG_SQS_ALERT(stream)  LOG_SQS_BASE_ALERT(TActivationContext::AsActorContext(), stream)
#define LOG_SQS_CRIT(stream)   LOG_SQS_BASE_CRIT(TActivationContext::AsActorContext(), stream)
#define LOG_SQS_ERROR(stream)  LOG_SQS_BASE_ERROR(TActivationContext::AsActorContext(), stream)
#define LOG_SQS_WARN(stream)   LOG_SQS_BASE_WARN(TActivationContext::AsActorContext(), stream)
#define LOG_SQS_NOTICE(stream) LOG_SQS_BASE_NOTICE(TActivationContext::AsActorContext(), stream)
#define LOG_SQS_INFO(stream)   LOG_SQS_BASE_INFO(TActivationContext::AsActorContext(), stream)
#define LOG_SQS_DEBUG(stream)  LOG_SQS_BASE_DEBUG(TActivationContext::AsActorContext(), stream)
#define LOG_SQS_TRACE(stream)  LOG_SQS_BASE_TRACE(TActivationContext::AsActorContext(), stream)

// Log with explicitly specified request id
#define RLOG_SQS_REQ_EMERG(requestId, stream)  RLOG_SQS_REQ_BASE_EMERG(TActivationContext::AsActorContext(), requestId, stream)
#define RLOG_SQS_REQ_ALERT(requestId, stream)  RLOG_SQS_REQ_BASE_ALERT(TActivationContext::AsActorContext(), requestId, stream)
#define RLOG_SQS_REQ_CRIT(requestId, stream)   RLOG_SQS_REQ_BASE_CRIT(TActivationContext::AsActorContext(), requestId, stream)
#define RLOG_SQS_REQ_ERROR(requestId, stream)  RLOG_SQS_REQ_BASE_ERROR(TActivationContext::AsActorContext(), requestId, stream)
#define RLOG_SQS_REQ_WARN(requestId, stream)   RLOG_SQS_REQ_BASE_WARN(TActivationContext::AsActorContext(), requestId, stream)
#define RLOG_SQS_REQ_NOTICE(requestId, stream) RLOG_SQS_REQ_BASE_NOTICE(TActivationContext::AsActorContext(), requestId, stream)
#define RLOG_SQS_REQ_INFO(requestId, stream)   RLOG_SQS_REQ_BASE_INFO(TActivationContext::AsActorContext(), requestId, stream)
#define RLOG_SQS_REQ_DEBUG(requestId, stream)  RLOG_SQS_REQ_BASE_DEBUG(TActivationContext::AsActorContext(), requestId, stream)
#define RLOG_SQS_REQ_TRACE(requestId, stream)  RLOG_SQS_REQ_BASE_TRACE(TActivationContext::AsActorContext(), requestId, stream)

// Log with imlicitly specified request id (RequestId_ member)
#define RLOG_SQS_EMERG(stream)  RLOG_SQS_BASE_EMERG(TActivationContext::AsActorContext(), stream)
#define RLOG_SQS_ALERT(stream)  RLOG_SQS_BASE_ALERT(TActivationContext::AsActorContext(), stream)
#define RLOG_SQS_CRIT(stream)   RLOG_SQS_BASE_CRIT(TActivationContext::AsActorContext(), stream)
#define RLOG_SQS_ERROR(stream)  RLOG_SQS_BASE_ERROR(TActivationContext::AsActorContext(), stream)
#define RLOG_SQS_WARN(stream)   RLOG_SQS_BASE_WARN(TActivationContext::AsActorContext(), stream)
#define RLOG_SQS_NOTICE(stream) RLOG_SQS_BASE_NOTICE(TActivationContext::AsActorContext(), stream)
#define RLOG_SQS_INFO(stream)   RLOG_SQS_BASE_INFO(TActivationContext::AsActorContext(), stream)
#define RLOG_SQS_DEBUG(stream)  RLOG_SQS_BASE_DEBUG(TActivationContext::AsActorContext(), stream)
#define RLOG_SQS_TRACE(stream)  RLOG_SQS_BASE_TRACE(TActivationContext::AsActorContext(), stream)
