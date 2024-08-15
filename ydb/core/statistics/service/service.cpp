#include "service.h"


#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/database/database.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/protos/statistics.pb.h>
#include <ydb/core/protos/data_events.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {
namespace NStat {

static constexpr TDuration DefaultAggregateKeepAlivePeriod = TDuration::MilliSeconds(500);
static constexpr TDuration DefaultAggregateKeepAliveTimeout = TDuration::Seconds(3);
static constexpr TDuration DefaultAggregateKeepAliveAckTimeout = TDuration::Seconds(3);
static constexpr size_t DefaultMaxInFlightTabletRequests = 5;
static constexpr size_t DefaultFanOutFactor = 5;



TStatServiceSettings::TStatServiceSettings()
    : AggregateKeepAlivePeriod(DefaultAggregateKeepAlivePeriod)
    , AggregateKeepAliveTimeout(DefaultAggregateKeepAliveTimeout)
    , AggregateKeepAliveAckTimeout(DefaultAggregateKeepAliveAckTimeout)
    , MaxInFlightTabletRequests(DefaultMaxInFlightTabletRequests)
    , FanOutFactor(DefaultFanOutFactor)
{}

NActors::TActorId MakeStatServiceID(ui32 node) {
    const char x[12] = "StatService";
    return NActors::TActorId(node, TStringBuf(x, 12));
}

} // NStat
} // NKikimr
