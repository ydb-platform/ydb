#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>

#include <library/cpp/logger/log.h>

namespace NYdb {

TLogFormatter GetPrefixLogFormatter(const TString& prefix);
TStringType GetDatabaseLogPrefix(const TStringType& database);

} // namespace NYdb
