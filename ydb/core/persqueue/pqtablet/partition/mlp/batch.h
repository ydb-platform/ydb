#pragma once

#include "mlp.h"

#include <ydb/core/persqueue/events/global.h>

#include <util/datetime/base.h>

#include <vector>

namespace NKikimr::NPQ::NMLP {

class TBatch {
public:    
    void Add(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev);
    void Add(TEvPersQueue::TEvMLPReleaseRequest::TPtr& ev);
    void Add(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev);
    void Add(TEvPersQueue::TEvMLPReadRequest::TPtr& ev, std::vector<TMessageId>&& messages);

    void Clear();
    void Commit();

    void Rollback();

    bool Empty() const;

private:

};

}
