#include "row_dispatcher.h"
#include "coordinator.h"

#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/string/strip.h>

namespace NFq {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRowDispatcher : public TActorBootstrapped<TRowDispatcher> {
    NConfig::TRowDispatcherConfig Config;
    NConfig::TCommonConfig CommonConfig;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;
    TActorId CoordinatorActorId;

public:
    explicit TRowDispatcher(
        const NConfig::TRowDispatcherConfig& config,
        const NConfig::TCommonConfig& commonConfig,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_ROW_DISPATCHER";

private:
    // STRICT_STFUNC(StateFunc,
    //     hFunc(NYql::NDq::TEvDqCompute::TEvGetTaskState, Handle);
    // )

    // void Handle(NYql::NDq::TEvDqCompute::TEvGetTaskState::TPtr& ev);
};

TRowDispatcher::TRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
    : Config(config)
    , CommonConfig(commonConfig)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources) {
    //LOG_YQ_ROW_DISPATCHER_DEBUG("Successfully bootstrapped row dispatcher, id  111 1 " << SelfId());
}

void TRowDispatcher::Bootstrap() {
    LOG_YQ_ROW_DISPATCHER_DEBUG("Successfully bootstrapped row dispatcher, id " << SelfId());

    if (Config.GetCoordinator().GetEnabled()) {
        const auto& config = Config.GetCoordinator();
        CoordinatorActorId = Register(NewRDCoordinator(config, CredentialsProviderFactory, YqSharedResources).release());
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    return std::unique_ptr<NActors::IActor>(new TRowDispatcher(config, commonConfig, credentialsProviderFactory, yqSharedResources));
}

} // namespace NFq
