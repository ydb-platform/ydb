#pragma once

#include <src/client/impl/ydb_internal/internal_header.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <ydb-cpp-sdk/type_switcher.h>

namespace NYdb::inline V3 {

bool TypesEqual(const NYdbProtos::Type& t1, const NYdbProtos::Type& t2);

} // namespace NYdb
