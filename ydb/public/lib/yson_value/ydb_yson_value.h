#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fatal_error_handlers/handlers.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yson/writer.h>

namespace NYdb {

void FormatValueYson(const TValue& value, NYson::TYsonWriter& writer);

TString FormatValueYson(const TValue& value, NYson::EYsonFormat ysonFormat = NYson::EYsonFormat::Text);

void FormatResultSetYson(const TResultSet& result, NYson::TYsonWriter& writer);

TString FormatResultSetYson(const TResultSet& result, NYson::EYsonFormat ysonFormat = NYson::EYsonFormat::Text);

} // namespace NYdb
