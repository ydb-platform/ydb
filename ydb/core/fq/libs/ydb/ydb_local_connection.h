#pragma once

#include <ydb/core/fq/libs/ydb/ydb_connection.h>

namespace NFq {

IYdbConnection::TPtr CreateLocalYdbConnection(const TString& tablePathPrefix);

} // namespace NFq
