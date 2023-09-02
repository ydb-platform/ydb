
#include "porto_health_checker.h"

#include "porto_executor.h"
#include "private.h"
#include "config.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/fs.h>

#include <util/random/random.h>

namespace NYT::NContainers {

using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TPortoHealthChecker::TPortoHealthChecker(
    TPortoExecutorDynamicConfigPtr config,
    IInvokerPtr invoker,
    TLogger logger)
    : Config_(std::move(config))
    , Logger(std::move(logger))
    , CheckInvoker_(std::move(invoker))
    , Executor_(CreatePortoExecutor(
        Config_,
        "porto_check"))
{ }

void TPortoHealthChecker::Start()
{
    YT_LOG_DEBUG("Porto health checker started");

    PeriodicExecutor_ = New<TPeriodicExecutor>(
        CheckInvoker_,
        BIND(&TPortoHealthChecker::OnCheck, MakeWeak(this)),
        Config_->RetriesTimeout);
    PeriodicExecutor_->Start();
}

void TPortoHealthChecker::OnDynamicConfigChanged(const TPortoExecutorDynamicConfigPtr& newConfig)
{
    YT_LOG_DEBUG(
        "Porto health checker dynamic config changed (EnableTestPortoFailures: %v, StubErrorCode: %v)",
        Config_->EnableTestPortoFailures,
        Config_->StubErrorCode);

    Executor_->OnDynamicConfigChanged(newConfig);
}

void TPortoHealthChecker::OnCheck()
{
    YT_LOG_DEBUG("Run porto health check");

    auto result = WaitFor(Executor_->ListVolumePaths().AsVoid());
    if (result.IsOK()) {
        Success_.Fire();
    } else {
        Failed_.Fire(result);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
