#pragma once
#include "defs.h"
#include "events.h"
#include "log.h"
#include "serviceid.h"

#include <ydb/library/actors/core/actor.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSQS {

class TQueuesListReader : public TActor<TQueuesListReader> {
public:
    explicit TQueuesListReader(const TIntrusivePtr<TTransactionCounters>& transactionCounters);
    ~TQueuesListReader();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_QUEUES_LIST_READER_ACTOR;
    }

private:
    STATEFN(StateFunc);
    void HandleReadQueuesList(TSqsEvents::TEvReadQueuesList::TPtr& ev);
    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);

    void CompileRequest();
    void OnRequestCompiled(const TSqsEvents::TEvExecuted::TRecord& record);

    void NextRequest();
    void OnQueuesList(const TSqsEvents::TEvExecuted::TRecord& record);

    void Success();
    void Fail();

private:
    TString CompiledQuery;

    TIntrusivePtr<TTransactionCounters> TransactionCounters;
    TString CurrentUser;
    TString CurrentQueue;
    THolder<TSqsEvents::TEvQueuesList> Result;
    bool ListingQueues = false;
    THashSet<TActorId> Recipients;
};

} // namespace NKikimr::NSQS
