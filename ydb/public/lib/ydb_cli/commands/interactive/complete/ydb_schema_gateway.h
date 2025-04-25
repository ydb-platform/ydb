#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <yql/essentials/sql/v1/complete/name/object/schema_gateway.h>

namespace NYdb::NConsoleClient {

    NSQLComplete::ISchemaGateway::TPtr MakeYDBSchemaGateway(TClientCommand::TConfig& config);

} // namespace NYdb::NConsoleClient
