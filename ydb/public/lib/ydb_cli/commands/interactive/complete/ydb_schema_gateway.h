#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <yql/essentials/sql/v1/complete/name/object/schema_gateway.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::NConsoleClient {

    NSQLComplete::ISchemaGateway::TPtr MakeYDBSchemaGateway(TDriver driver, TString database);

} // namespace NYdb::NConsoleClient
