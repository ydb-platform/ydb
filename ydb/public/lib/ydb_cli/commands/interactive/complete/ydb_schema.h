#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::NConsoleClient {

    NSQLComplete::ISimpleSchema::TPtr MakeYDBSchema(TDriver driver, TString database, bool isVerbose);

} // namespace NYdb::NConsoleClient
