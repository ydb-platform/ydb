#pragma once

#include "events.h"
#include <ydb/core/ymq/base/events_writer_iface.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/json/json_writer.h>
#include <util/stream/file.h>



namespace NKikimr::NSQS {



class TSearchEventsProcessor : public NActors::TActorBootstrapped<TSearchEventsProcessor> {
using TActorContext = NActors::TActorContext;
friend class TIndexProcesorTests;
public:

    TSearchEventsProcessor(const TString& root, const TDuration& reindexInterval, const TDuration& rescanInterval,
                           const TString& database,
                           IEventsWriterWrapper::TPtr eventsWriter,
                           bool waitForWake = false);

    ~TSearchEventsProcessor();
    void Bootstrap(const TActorContext& ctx);
    ui64 GetReindexCount() const;

private:
    TString SelectQueuesQuery;
    TString SelectEventsQuery;
    TString DeleteEventQuery;
    TDuration RescanInterval;
    TDuration ReindexInterval;
    TInstant LastReindex = TInstant::Zero();
    IEventsWriterWrapper::TPtr EventsWriter;

    TString Database;

    TString SessionId;


    bool WaitForWake = false;
    enum class EQueueEventType {
        Deleted = 0,
        Created = 1,
        Existed = 2,
    };
    struct TQueueEvent {
        EQueueEventType Type;
        ui64 Timestamp;
        TString CustomName;
        TString CloudId;
        TString FolderId;
        TString Labels;
    };
    struct TQueueTableKey {
        TString Account;
        TString QueueName;
    };
    TQueueTableKey LastQueuesKey = {};

    THashMap<TString, TMap<ui64, TQueueEvent>> QueuesEvents;
    THashMap<TString, TQueueEvent> ExistingQueues;

    TAtomicCounter ReindexComplete = 0;

    STRICT_STFUNC(StateFunc,
          HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
          HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleQueryResponse);
          HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
          IgnoreFunc(NKqp::TEvKqp::TEvCloseSessionResponse);
    )

    void HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    void HandleFailure(const TActorContext& ctx);

    void StartQueuesListing(const TActorContext& ctx);
    void RunQueuesListQuery(const TActorContext& ctx);
    void OnQueuesListQueryComplete(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    void RunEventsListing(const TActorContext& ctx);
    void OnEventsListingDone(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void RunEventsCleanup(const TActorContext& ctx);
    void OnCleanupQueryComplete(const TActorContext&ctx);

    void RunQuery(const TString& query, NYdb::TParams* params, bool readonly,
                  const TActorContext& ctx);

    void ProcessEventsQueue(const TActorContext& ctx);
    void ProcessReindexIfRequired(const TActorContext& ctx);
    void ProcessReindexResult(const TActorContext& ctx);
    void WaitNextCycle(const TActorContext& ctx);

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);

    void StopSession(const TActorContext& ctx);

    void InitQueries(const TString& root);
    void SaveQueueEvent(const TString& queueName, const TQueueEvent& event, const TActorContext& ctx);
    void SendJson(const TString& json, const TActorContext &ctx);
    static NActors::TActorSystem* GetActorSystem();


    enum class EState {
        Initial,
        QueuesListingExecute,
        EventsListingExecute,
        CleanupExecute,
        Stopping
    };
    EState State = EState::Initial;

};
} // namespace NKikimr::NSQS
