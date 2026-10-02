#pragma once

#include <ydb/core/fq/libs/ydb/table_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NFq {

IYdbTableClient::TPtr CreateSdkTableClient(
    const NYdb::TDriver& driver,
    const NYdb::NTable::TClientSettings& settings);

} // namespace NFq
