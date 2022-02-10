#pragma once

#include <ydb/public/lib/ydb_cli/common/common.h>
#include <ydb/core/driver_lib/cli_config_base/config_base.h>
#include <util/string/builder.h>

namespace NKikimr {

namespace NDriverClient {
    int NewLiteClient(int argc, char** argv);
}
}
