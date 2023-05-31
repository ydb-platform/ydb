#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

namespace NYql::NDq {
    void RegisterGenericReadActorFactory(TDqAsyncIoFactory& factory,
                                         ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
                                         NYql::Connector::IClient::TPtr genericClient);
}
