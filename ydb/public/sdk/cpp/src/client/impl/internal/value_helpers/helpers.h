#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

namespace NYdb::inline Dev {

bool TypesEqual(const Ydb::Type& t1, const Ydb::Type& t2);

} // namespace NYdb
