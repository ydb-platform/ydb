#pragma once

#include <ydb/library/workload/scheme/runner.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <memory>

namespace NYdb::NConsoleClient {

std::unique_ptr<NYdbWorkload::NScheme::IClient> CreateSchemeWorkloadClient(
    const TDriver& driver, const TString& database, ui32 concurrency);

} // namespace NYdb::NConsoleClient
