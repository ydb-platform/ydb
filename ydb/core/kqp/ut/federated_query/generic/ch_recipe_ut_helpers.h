#pragma once

#include <library/cpp/clickhouse/client/client.h>

namespace NTestUtils {

    TString GetChHost();
    ui32 GetChPort();
    TString GetChUser();
    TString GetChPassword();
    TString GetChDatabase();

    NClickHouse::TClient CreateClickhouseClient();

} // namespace NTestUtils
