#include "service.h"

#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregatorService: public TActor<TCdcAggregatorService> {
    void Handle(TEvCdcAggregatorService::TEvWriteHeartbeat::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        auto result = MakeHolder<TEvCdcAggregatorService::TEvWriteHeartbeatResult>();
        result->Status = TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus::Retry;
        Send(ev->Sender, result.Release(), 0, ev->Cookie);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CDC_AGGREGATOR_SERVICE;
    }

    TCdcAggregatorService()
        : TActor(&TThis::StateWork)
    {
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCdcAggregatorService::TEvWriteHeartbeat, Handle);
        }
    }
};

IActor* CreateCdcAggregatorService() {
    return new TCdcAggregatorService();
}

} // namespace NKikimr::NCdcAggregator
