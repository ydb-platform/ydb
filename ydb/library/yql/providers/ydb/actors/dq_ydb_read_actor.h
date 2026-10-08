#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <functional>

namespace NYql::NDq {

// The getter lazily supplies independently constructed TLS/plaintext drivers,
// with MaxInboundMessageSize set to NYdb::MaxInboundMessageBytes.
using TYdbDriverFactory = std::function<::NYdb::TDriver(bool useTls)>;
void RegisterYdbReadActorFactory(TDqAsyncIoFactory& factory,
    TYdbDriverFactory driverFactory, IStructuredTokenCredentialsFactory::TPtr credentialsFactory);

void RegisterYdbReadActorFactory(TDqAsyncIoFactory& factory,
    const ::NYdb::TDriver& driver, const ::NYdb::TDriver& tlsDriver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
