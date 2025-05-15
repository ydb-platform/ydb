#include "events.h"

#include <ydb/public/api/client/yc_public/events/ymq.pb.h>
#include <ydb/public/api/client/yc_public/events/common.pb.h>

#include <ydb/core/audit/audit_log_helpers.h>

#include <ydb/core/ymq/base/events_writer_iface.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

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

    struct TEventInfo {
        NACLib::TUserToken UserToken;
        TString AuthType;
    };

    template<typename TProtoEvent>
    class TFillerBase {
    protected:
        TEventInfo EventInfo;
        TProtoEvent& Ev; // be careful!

        void FillAuthentication();
        void FillAuthorization();
        void FillEventMetadata();
        void FillRequestMetadata();
        void FillEventStatus();
        void FillDetails();
    public:
        void Fill();

        TFillerBase(const TEventInfo& eventInfo, TProtoEvent& ev)
        : EventInfo(eventInfo)
        , Ev(ev)
        {}
    };

    class TFillerCreateQueue : public TFillerBase<TCreateQueueEvent> {
    public:
        TFillerCreateQueue(const TEventInfo& eventInfo, TCreateQueueEvent& ev)
        : TFillerBase(eventInfo, ev)
        {}

        void Fill();
    };

    class TFillerUpdateQueue : public TFillerBase<TUpdateQueueEvent> {
    public:
        TFillerUpdateQueue(const TEventInfo& eventInfo, TUpdateQueueEvent& ev)
        : TFillerBase(eventInfo, ev)
        {}

        void Fill();
    };

    class TFillerDeleteQueue : public TFillerBase<TDeleteQueueEvent> {
    public:
        TFillerDeleteQueue(const TEventInfo& eventInfo, TDeleteQueueEvent& ev)
        : TFillerBase(eventInfo, ev)
        {}

        void Fill();
    };

    template<typename TProtoEvent>
    class TSender {
    public:
        void Send(const TProtoEvent& ev);
    };

    template class TSender<TCreateQueueEvent>;
    template class TSender<TUpdateQueueEvent>;
    template class TSender<TDeleteQueueEvent>;


    class TProcessor : public NActors::TActorBootstrapped<TProcessor> {
    private:
        const TDuration RescanInterval;

    public:
        TProcessor(
            const TDuration& rescanInterval
        )
        : RescanInterval(rescanInterval)
        {}

        void Bootstrap();
    
    private:
        // Try do not use TActorContext: those handlers are depreceted
        void HandleWakeup(const NActors::TEvents::TEvWakeup::TPtr& ev);
        void HandleQueryResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev);

        STRICT_STFUNC(StateFunc,
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleQueryResponse);
            cFunc(TEvPoisonPill::EventType, PassAway);
            IgnoreFunc(NKqp::TEvKqp::TEvCloseSessionResponse);
        )

        enum class EState {
            Initial,
            QueuesListingExecute,
            EventsListingExecute,
            CleanupExecute,
            Stopping
        };
        [[maybe_unused]] EState State = EState::Initial;
    };

} // namespace NCloudEvents
} // namespace NKikimr::NSQS
