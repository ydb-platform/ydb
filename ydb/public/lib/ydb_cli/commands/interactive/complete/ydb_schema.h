#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/lazy_driver.h>

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>

namespace NYdb::NConsoleClient {

    NSQLComplete::ISimpleSchema::TPtr MakeYDBSchema(TLazyDriver::TPtr lazyDriver, TString database, bool isVerbose);

} // namespace NYdb::NConsoleClient
