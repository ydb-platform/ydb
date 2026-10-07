#pragma once
#include <ydb/core/ymq/actor/cfg/defs.h>

#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSQS {

class IReplyCallback {
public:
    virtual ~IReplyCallback() = default;

    virtual void DoSendReply(const NKikimrClient::TSqsResponse& resp) = 0;
    virtual void OnIamAuthSuccess() {}
};

class IPingReplyCallback {
public:
    virtual ~IPingReplyCallback() = default;

    virtual void DoSendReply() = 0;
};

// Create actor that would process request.
// Called from leader node.
IActor* CreateActionActor(const NKikimrClient::TSqsRequest& req, std::unique_ptr<IReplyCallback> cb);

// Create actor that would proxy request to leader
// or process it if leader is not required for given operation type.
IActor* CreateProxyActionActor(const NKikimrClient::TSqsRequest& req, std::unique_ptr<IReplyCallback> cb, bool enableQueueLeader);

IActor* CreatePingActor(std::unique_ptr<IPingReplyCallback> cb, const TString& requestId);

} // namespace NKikimr::NSQS
