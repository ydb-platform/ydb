#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <google/protobuf/text_format.h>

#include <cstdint>
#include <string>

inline NYdb::TResultSet MakeSingleInt32ResultSet(int32_t value) {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { type_id: INT32 }\n"
        "}\n"
        "rows {\n"
        "  items { int32_value: " +
        std::to_string(value) +
        " }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(text, &proto);
    return NYdb::TResultSet(std::move(proto));
}
