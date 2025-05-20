#include "events.h"

#include <ydb/public/api/client/yc_public/events/ymq.pb.h>
#include <ydb/public/api/client/yc_public/events/common.pb.h>

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
        std::string UserSID;                                    // We need
        std::string UserSanitizedToken;                         // We need
        std::string AuthType;                                   // We need 

        static constexpr std::string_view Permission = "ymq.queues.list";
        static constexpr std::string_view ResourceType = "message-queue";
        // ResourceId = FolderId

        std::string Id;                                         // On place
        std::string Type;                                       // We need
        uint_fast64_t Timestamp;                                // We need
        std::string CloudId;                                    // We need
        std::string FolderId;                                   // We need

        std::string RemoteAddress;                              // We need one field 'remote_address (or peer_name) for remote address and remote port'
        std::string RemotePort;
        std::string RequestId;                                  // We need
        std::string IdempotencyId;                              // We need

        std::string Issue = "";                                 // On place

        std::string Name;                                       // We need
        std::unordered_map<std::string, std::string> Labels;    // We need it as a string
    };

    template<typename TProtoEvent>
    class TFiller {
    protected:
        TEventInfo EventInfo;
        TProtoEvent& Ev; // be careful!

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

    template<typename TProtoEvent>
    class TAuditSender {
    public:
        static void Send(const TProtoEvent& ev);
    };

    template class TAuditSender<TCreateQueueEvent>;
    template class TAuditSender<TUpdateQueueEvent>;
    template class TAuditSender<TDeleteQueueEvent>;


    class TProcessor : public NActors::TActorBootstrapped<TProcessor> {
    private:
        static constexpr std::string_view EventTableName = ".CloudEventsYmq";
        static constexpr TDuration DefaultRetryTimeout = TDuration::Seconds(10);

        const std::string Root;

        constexpr std::string_view GetSelectQuery() const;
        constexpr std::string_view GetDeleteQuery() const;

        uint_fast64_t LastCookie = 0;
        std::string LastQuery = "";

        void RunQuery(std::string_view query);

    public:
        TProcessor(
            std::string root
        )
        : Root(root)
        {}

        void Bootstrap();

    private:
        void HandleWakeup(const NActors::TEvents::TEvWakeup::TPtr&);
        void HandleRetry(const NActors::TEvents::TEvUndelivered::TPtr&);
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
            hFunc(NActors::TEvents::TEvUndelivered, HandleRetry);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleSelectResponse);
            cFunc(TEvPoisonPill::EventType, PassAway);
        )

        STRICT_STFUNC(
            StateWaitDeleteResponse,
            hFunc(NActors::TEvents::TEvUndelivered, HandleRetry);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleDeleteResponse);
            cFunc(TEvPoisonPill::EventType, PassAway);
        )
    };

} // namespace NCloudEvents
} // namespace NKikimr::NSQS
