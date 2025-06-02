#include <ydb/core/ymq/actor/events.h>
#include <ydb/core/ymq/actor/service.h>

#include <ydb/public/api/client/yc_public/events/ymq.pb.h>

#include <ydb/core/kqp/common/kqp.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NSQS {
namespace NCloudEvents {
    using TCreateQueueEvent = yandex::cloud::events::ymq::CreateQueue;
    using TUpdateQueueEvent = yandex::cloud::events::ymq::UpdateQueue;
    using TDeleteQueueEvent = yandex::cloud::events::ymq::DeleteQueue;
    using EStatus = yandex::cloud::events::EventStatus;

    struct TEventInfo {
        TString UserSID;
        TString UserSanitizedToken;
        TString AuthType;

        static constexpr TStringBuf Permission = "ymq.queues.list";
        static constexpr TStringBuf ResourceType = "message-queue";
        // ResourceId = FolderId

        uint_fast64_t OriginalId;
        TString Id;
        TString Type;
        uint_fast64_t CreatedAt;
        TString CloudId;
        TString FolderId;

        TString RemoteAddress;
        TString RequestId;
        TString IdempotencyId;

        TString Issue = "";

        TString QueueName;
        THashMap<TBasicString<char>, NJson::TJsonValue> Labels;
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
    public:
        template<typename TProtoEvent>
        static void Send(const TProtoEvent& ev);
    };

    class TProcessor : public NActors::TActorBootstrapped<TProcessor> {
    private:
        static constexpr TStringBuf EventTableName = NKikimr::NSQS::TSqsService::CloudEventsTableName;
        static constexpr TStringBuf DefaultEventTypePrefix = "yandex.cloud.events.ymq.";

        const TString Root;
        const TString Database;

        const TDuration RetryTimeout;

        const TString SelectQuery;
        const TString DeleteQuery;


        TString GetFullTablePath() const;
        TString GetInitSelectQuery() const;
        TString GetInitDeleteQuery() const;

        TString SessionId = TString();

        void RunQuery(TString query, std::unique_ptr<NYdb::TParams> params = nullptr);
        void UpdateSessionId(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev);
        void StopSession();
        void PutToSleep();

        static std::vector<TEventInfo> ConvertSelectResponseToEventList(const ::NKikimrKqp::TQueryResponse& response);

    public:
        TProcessor(
            const TString& root,
            const TString& database,
            const TDuration& retryTimeout = TDuration::Seconds(10)
        );

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
            cFunc(TEvPoisonPill::EventType, PassAway);
        )

        STRICT_STFUNC(
            StateWaitSelectResponse,
            hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleSelectResponse);
            cFunc(TEvPoisonPill::EventType, PassAway);
        )

        STRICT_STFUNC(
            StateWaitDeleteResponse,                                        // For error's tracking
            hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleDeleteResponse);
            cFunc(TEvPoisonPill::EventType, PassAway);
        )
    };

} // namespace NCloudEvents
} // namespace NKikimr::NSQS
