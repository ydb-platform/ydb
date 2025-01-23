#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NYql::NDq {

void RegisterYdbReadActorFactory(NYql::NDq::TDqAsyncIoFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

}
