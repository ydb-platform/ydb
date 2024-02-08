#pragma once
#include "defs.h"

#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSQS {

class IReplyCallback {
public:
    virtual ~IReplyCallback() = default;

    virtual void DoSendReply(const NKikimrClient::TSqsResponse& resp) = 0;
};

class IPingReplyCallback {
public:
    virtual ~IPingReplyCallback() = default;

    virtual void DoSendReply() = 0;
};

// Create actor that would process request.
// Called from leader node.
IActor* CreateActionActor(const NKikimrClient::TSqsRequest& req, THolder<IReplyCallback> cb);

// Create actor that would proxy request to leader
// or process it if leader is not required for given operation type.
IActor* CreateProxyActionActor(const NKikimrClient::TSqsRequest& req, THolder<IReplyCallback> cb, bool enableQueueLeader);

IActor* CreatePingActor(THolder<IPingReplyCallback> cb, const TString& requestId);

} // namespace NKikimr::NSQS
