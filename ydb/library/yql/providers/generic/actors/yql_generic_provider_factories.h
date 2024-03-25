#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

namespace NYql::NDq {
    void RegisterGenericProviderFactories(TDqAsyncIoFactory& factory,
                                          ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
                                          NYql::NConnector::IClient::TPtr genericClient);
}
