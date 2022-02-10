#pragma once

#include "events.h"
#include <ydb/core/ymq/base/events_writer_iface.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/json/json_writer.h>
#include <util/stream/file.h>



namespace NKikimr::NSQS {



class TSearchEventsProcessor : public NActors::TActorBootstrapped<TSearchEventsProcessor> {
using TActorContext = NActors::TActorContext;
friend class TIndexProcesorTests;
public:

    TSearchEventsProcessor(const TString& root, const TDuration& reindexInterval, const TDuration& rescanInterval,
                           const TSimpleSharedPtr<NYdb::NTable::TTableClient>& tableClient,
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
    TSimpleSharedPtr<NYdb::NTable::TTableClient> TableClient;
    IEventsWriterWrapper::TPtr EventsWriter;

    NYdb::TAsyncStatus Status;
    NYdb::NTable::TAsyncCreateSessionResult SessionFuture;
    NYdb::NTable::TAsyncBeginTransactionResult TxFuture;
    NYdb::NTable::TAsyncCommitTransactionResult CommitTxFuture;
    NYdb::NTable::TAsyncPrepareQueryResult PrepQueryFuture;
    NYdb::NTable::TAsyncDataQueryResult QueryResultFuture;

    TMaybe<NYdb::NTable::TSession> Session;
    TMaybe<NYdb::NTable::TDataQuery> PreparedQuery;
    TMaybe<NYdb::NTable::TTransaction> CurrentTx;
    TMaybe<NYdb::NTable::TDataQueryResult> QueryResult;

    bool WaitForWake = false;
    bool Stopping = false;
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
    };
    struct TQueueTableKey {
        TString Account;
        TString QueueName;
    };
    TQueueTableKey LastQueuesKey = {};

    THashMap<TString, TMap<ui64, TQueueEvent>> QueuesEvents;
    THashMap<TString, TQueueEvent> ExistingQueues;

    TAtomicCounter ReindexComplete = 0;

    STATEFN(StateFunc);
    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    void StartQueuesListing(const TActorContext& ctx);
    void OnQueuesListSessionReady(const TActorContext& ctx);
    void OnQueuesListPrepared(const TActorContext& ctx);
    void RunQueuesListQuery(const TActorContext& ctx, bool initial = false);
    void OnQueuesListQueryComplete(const TActorContext& ctx);

    void RunEventsListing(const TActorContext& ctx);
    void OnEventsListingDone(const TActorContext& ctx);
    void StartCleanupSession(const TActorContext& ctx);
    void OnCleanupSessionReady(const TActorContext& ctx);
    void OnCleanupTxReady(const TActorContext& ctx);
    void OnCleanupPrepared(const TActorContext& ctx);
    void RunEventsCleanup(const TActorContext& ctx);
    void OnCleanupQueryComplete(const TActorContext&ctx);
    void OnCleanupTxCommitted(const TActorContext&ctx);

    void ProcessEventsQueue(const TActorContext& ctx);
    void ProcessReindexIfRequired(const TActorContext& ctx);
    void ProcessReindexResult(const TActorContext& ctx);
    void WaitNextCycle(const TActorContext& ctx);


    void StartSession(ui32 evTag, const TActorContext& ctx);
    void PrepareDataQuery(const TString& query, ui32 evTag, const TActorContext& ctx);
    void StartTx(ui32 evTag, const TActorContext& ctx);
    void CommitTx(ui32 evTag, const TActorContext& ctx);
    void RunPrepared(NYdb::TParams&& params, ui32 evTag, const TActorContext& ctx);
    bool SessionStarted(const TActorContext& ctx);
    bool QueryPrepared(const TActorContext& ctx);
    bool QueryComplete(const TActorContext& ctx);
    bool TxStarted(const TActorContext& ctx);
    bool TxCommitted(const TActorContext& ctx);


    template<class TFutureType>
    void Apply(TFutureType* future, ui32 evTag, const TActorContext& ctx) {
        future->Subscribe(
                [evTag, as = GetActorSystem(), selfId = ctx.SelfID](const auto&)
                {as->Send(selfId, new TEvWakeup(evTag));}
        );
    }
    void InitQueries(const TString& root);
    void SaveQueueEvent(const TString& queueName, const TQueueEvent& event, const TActorContext& ctx);
    void SendJson(const TString& json, const TActorContext &ctx);
    static NActors::TActorSystem* GetActorSystem();


    constexpr static ui32 StartQueuesListingTag = 1;
    constexpr static ui32 QueuesListSessionStartedTag = 2;
    constexpr static ui32 QueuesListPreparedTag = 3;
    constexpr static ui32 QueuesListQueryCompleteTag = 4;

    constexpr static ui32 RunEventsListingTag = 10;
    constexpr static ui32 EventsListingDoneTag = 11;

    constexpr static ui32 StartCleanupTag = 20;
    constexpr static ui32 CleanupSessionReadyTag = 21;
    constexpr static ui32 CleanupTxReadyTag = 22;
    constexpr static ui32 CleanupPreparedTag = 23;
    constexpr static ui32 CleanupQueryCompleteTag = 24;
    constexpr static ui32 CleanupTxCommittedTag = 25;

    constexpr static ui32 StopAllTag = 99;


};
} // namespace NKikimr::NSQS
