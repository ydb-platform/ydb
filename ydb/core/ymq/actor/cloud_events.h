#include "events.h"

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
        TString UserSID;                                    // We need
        TString UserSanitizedToken;                         // We need
        TString AuthType;                                   // We need 

        static constexpr std::string_view Permission = "ymq.queues.list";
        static constexpr std::string_view ResourceType = "message-queue";
        // ResourceId = FolderId

        TString Id;                                         // We need (generate on audit.log as guid; but in cloud we send smth like Type$Id$CreatedAt)
        TString Type;                                       // We need
        uint_fast64_t CreatedAt;                            // We need
        TString CloudId;                                    // We need
        TString FolderId;                                   // We need

        TString RemoteAddress;                              // We need
        TString RequestId;                                  // We need
        TString IdempotencyId;                              // We need

        TString Issue = "";                                 // Is always empty?

        TString Name;                                       // We need
        THashMap<TBasicString<char>, NJson::TJsonValue> Labels;        // We need it as a string
    };

    /*
        CREATE TABLE `/Root/.CloudEventsYmq` (
            Id Uint64,
            Name String,

            Type String,
            CreatedAt Uint64,
            CloudId String,
            FolderId String,
            UserSID String,
            UserSanitizedToken String,
            AuthType String,
            PeerName String,
            RequestId String,
            IdempotencyId String,
            Labels String,
            PRIMARY KEY(Id, Name)
        );
    */

    /*
    UPSERT INTO CloudEventsYmq (
    Id,
    Name,
    Type,
    CreatedAt,
    CloudId,
    FolderId,
    UserSID,
    UserSanitizedToken,
    AuthType,
    PeerName,
    RequestId,
    IdempotencyId,
    Labels
    )
    VALUES
    (
    1001,
    "test_name_1",
    "create",
    1696506100,
    "cloud123",
    "folderABC",
    "userSID_1",
    "sanitizedToken_1",
    "OAuth",
    "peer_name_1",
    "req_id_1",
    "idemp_1",
    "label_1"
    ),
    (
    1002,
    "test_name_2",
    "delete",
    1696506200,
    "cloud456",
    "folderXYZ",
    "userSID_2",
    "sanitizedToken_2",
    "IamToken",
    "peer_name_2",
    "req_id_2",
    "idemp_2",
    "label_2"
    ),
    (
    1003,
    "test_name_3",
    "update",
    1696506300,
    "cloud789",
    "folderLMN",
    "userSID_3",
    "sanitizedToken_3",
    "ApiKey",
    "peer_name_3",
    "req_id_3",
    "idemp_3",
    "label_3"
    );
    */

    template<typename TProtoEvent>
    class TFiller {
    protected:
        const TEventInfo& EventInfo;    // be careful!
        TProtoEvent& Ev;                // be careful!

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

    template void TAuditSender::Send<TCreateQueueEvent>(const TCreateQueueEvent&);
    template void TAuditSender::Send<TUpdateQueueEvent>(const TUpdateQueueEvent&);
    template void TAuditSender::Send<TDeleteQueueEvent>(const TDeleteQueueEvent&);

    class TProcessor : public NActors::TActorBootstrapped<TProcessor> {
    private:
        static constexpr std::string_view EventTableName = ".CloudEventsYmq";
        static constexpr TDuration DefaultRetryTimeout = TDuration::Seconds(10);

        std::vector<TEventInfo> EventsList;

        const TString Root;
        const TString Database;

        const TString SelectQuery;
        const TString DeleteQuery;

        TString GetFullTablePath() const;
        TString GetInitSelectQuery() const;
        TString GetInitDeleteQuery() const;

        TString SessionId = TString();

        enum ELastQueryType {
            None,
            Select,
            Delete
        } LastQuery = ELastQueryType::None;

        void RunQuery(TString query, std::unique_ptr<NYdb::TParams> params = nullptr);
        void UpdateSessionId(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev);
        void StopSession();
        void ProcessFailure();

        static std::vector<TEventInfo> ConvertSelectResponseToEventList(const ::NKikimrKqp::TQueryResponse& response);

    public:
        TProcessor(
            TString root,
            TString database
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
            StateWaitDeleteResponse,
            hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleDeleteResponse);
            cFunc(TEvPoisonPill::EventType, PassAway);
        )
    };

} // namespace NCloudEvents
} // namespace NKikimr::NSQS
