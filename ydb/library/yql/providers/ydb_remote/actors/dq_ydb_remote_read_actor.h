#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYql::NDq {

// The dedicated drivers must be independently constructed (not copies) to
// isolate plaintext/TLS channel caches, with MaxInboundMessageSize <= 8 MiB.
void RegisterYdbRemoteReadActorFactory(TDqAsyncIoFactory& factory,
    const NYdb::TDriver& driver, const NYdb::TDriver& tlsDriver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
