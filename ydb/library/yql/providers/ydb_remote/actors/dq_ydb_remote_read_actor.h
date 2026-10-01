#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <functional>

namespace NYql::NDq {

// The getter lazily supplies independently constructed TLS/plaintext drivers,
// with MaxInboundMessageSize set to NYdbRemote::MaxInboundMessageBytes.
using TYdbRemoteDriverFactory = std::function<NYdb::TDriver(bool useTls)>;
void RegisterYdbRemoteReadActorFactory(TDqAsyncIoFactory& factory,
    TYdbRemoteDriverFactory driverFactory, IStructuredTokenCredentialsFactory::TPtr credentialsFactory);

void RegisterYdbRemoteReadActorFactory(TDqAsyncIoFactory& factory,
    const NYdb::TDriver& driver, const NYdb::TDriver& tlsDriver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
