#pragma once

#include <ydb/core/ymq/actor/cloud_events/proto/ymq.pb.h>

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/ymq/base/events_writer_iface.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/aclib/aclib.h>
#include <util/random/mersenne64.h>
#include <util/random/entropy.h>

namespace NKikimr::NSQS {
namespace NCloudEvents {
    using TCreateQueueEvent = yandex::cloud::events::ymq::CreateQueue;
    using TUpdateQueueEvent = yandex::cloud::events::ymq::UpdateQueue;
    using TDeleteQueueEvent = yandex::cloud::events::ymq::DeleteQueue;
    using EStatus = yandex::cloud::events::EventStatus;

    class TEventIdGenerator {
    public:
        static ui64 Generate();
    };

    struct TEventInfo {
        static constexpr TStringBuf ResourceType = "message-queue";

        TString UserSID;
        TString MaskedToken;
        TString AuthType;

        ui64 OriginalId;
        TString Id;
        TString Type;

        TInstant CreatedAt;

        TString CloudId;
        TString FolderId;
        TString ResourceId;

        TString RemoteAddress;
        TString RequestId;
        TString IdempotencyId;

        TString Issue = "";

        TString QueueName;
        TString Labels;

        TString Permission;
    };

    template<typename TProtoEvent>
    class TFiller {
    protected:
        const TEventInfo& EventInfo;    // Be careful with reference
        TProtoEvent& Ev;                // Be careful with reference

        void FillAuthentication();
        void FillAuthorization();
        void FillEventMetadata();
        void FillRequestMetadata();
        void FillStatus();
        void FillDetails();
    public:
        void Fill();

        TFiller(const TEventInfo& eventInfo, TProtoEvent& ev)
        : EventInfo(eventInfo)
        , Ev(ev)
        {}
    };

    class TAuditSender {
        template<typename TProtoEvent>
        static void SendProto(const TProtoEvent& ev, IEventsWriterWrapper::TPtr);
    public:
        static void Send(const TEventInfo& evInfo, IEventsWriterWrapper::TPtr);
    };

    class TProcessor : public NActors::TActorBootstrapped<TProcessor> {
    public:
        static constexpr TStringBuf EventTableName = ".CloudEventsYmq";

    private:

        const TString Root;
        const TString Database;

        const TDuration RetryTimeout;

        const TString SelectQuery;
        const TString DeleteQuery;

        IEventsWriterWrapper::TPtr EventsWriter;

        TString GetFullTablePath() const;
        TString GetInitSelectQuery() const;
        TString GetInitDeleteQuery() const;

        TString SessionId = TString();
        std::vector<TEventInfo> Events;

        void RunQuery(TString query, std::unique_ptr<NYdb::TParams> params = nullptr);
        void MakeDeleteResponse();
        void UpdateSessionId(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev);
        void StopSession();
        void PutToSleep();

        static std::vector<TEventInfo> ConvertSelectResponseToEventList(const ::NKikimrKqp::TQueryResponse& response);

    public:
        TProcessor(
            const TString& root,
            const TString& database,
            const TDuration& retryTimeout,
            IEventsWriterWrapper::TPtr eventsWriter
        );
        
        ~TProcessor();

        void Bootstrap();

    private:
        void HandleWakeup(const NActors::TEvents::TEvWakeup::TPtr&);
        void HandleUndelivered(const NActors::TEvents::TEvUndelivered::TPtr&);
        void HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr&);
        void HandleDeleteResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr&);

        STRICT_STFUNC(
            StateWaitWakeUp,
            IgnoreFunc(NActors::TEvents::TEvUndelivered);
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            cFunc(TKikimrEvents::TEvPoisonPill::EventType, PassAway);
        )

        STRICT_STFUNC(
            StateWaitSelectResponse,
            hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleSelectResponse);
            cFunc(TKikimrEvents::TEvPoisonPill::EventType, PassAway);
        )

        STRICT_STFUNC(
            StateWaitDeleteResponse,                                        // For error's tracking
            hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleDeleteResponse);
            cFunc(TKikimrEvents::TEvPoisonPill::EventType, PassAway);
        )
    };

} // namespace NCloudEvents
} // namespace NKikimr::NSQS
