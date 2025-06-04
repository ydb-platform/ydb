#pragma once

#include <ydb/core/kqp/common/kqp.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NSQS {
namespace NCloudEvents {
    class TEventIdGenerator {
    private:
        inline static std::mt19937_64 Randomizer64 = std::mt19937_64(
            std::chrono::time_point_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now()
            ).time_since_epoch().count()
        );

    public:
        static uint64_t Generate();
    };

    struct TEventInfo {
        TString UserSID;
        TString UserSanitizedToken;
        TString AuthType;

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
        TString Labels;
    };

    class TAuditSender {
    public:
        static void Send(const TEventInfo& evInfo);
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
            const TDuration& retryTimeout
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
