#pragma once

#include <ydb/core/kqp/common/kqp.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/aclib/aclib.h>
#include <util/random/mersenne64.h>
#include <util/random/entropy.h>

namespace NKikimr::NSQS {
namespace NCloudEvents {
    class TEventIdGenerator {
    public:
        static ui64 Generate();
    };

    struct TEventInfo {
        TString UserSID;
        TString MaskedToken;
        TString AuthType;

        ui64 OriginalId;
        TString Id;
        TString Type;
        ui64 CreatedAt;
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
