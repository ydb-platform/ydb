#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_io_actors_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NYql::NDq {

void RegisterYdbReadActorFactory(NYql::NDq::TDqSourceFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

}
