#pragma once

#include "mlp.h"
#include "mlp_types.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>

#include <util/datetime/base.h>

#include <vector>

namespace NKikimr::NPQ::NMLP {

template<typename TEv>
struct TResponseHolder {
    TActorId Sender;
    ui64 Cookie;
    std::unique_ptr<TEv> Response;
};

class TBatch {
public:
    TBatch(TActorIdentity selfActorId, const TActorId& partitionActorId);

    void Add(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev);
    void Add(TEvPersQueue::TEvMLPUnlockRequest::TPtr& ev);
    void Add(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev);
    void Add(TEvPersQueue::TEvMLPReadRequest::TPtr& ev, std::vector<TMessageId>&& messages);

    void Clear();
    void Commit();

    void Rollback();

    bool Empty() const;

private:
    const TActorIdentity SelfActorId;
    const TActorId PartitionActorId;

    std::vector<TReadResult> ReadResponses;
    std::vector<TResponseHolder<TEvPersQueue::TEvMLPCommitResponse>> CommitResponses;
    std::vector<TResponseHolder<TEvPersQueue::TEvMLPUnlockResponse>> UnlockResponses;
    std::vector<TResponseHolder<TEvPersQueue::TEvMLPChangeMessageDeadlineResponse>> ChangeMessageDeadlineResponses;
};

}
