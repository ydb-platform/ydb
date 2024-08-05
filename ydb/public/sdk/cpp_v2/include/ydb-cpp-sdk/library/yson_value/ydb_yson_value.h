#pragma once

#include <ydb-cpp-sdk/client/result/result.h>
#include <ydb-cpp-sdk/client/value/value.h>
#include <ydb-cpp-sdk/client/types/fatal_error_handlers/handlers.h>

#include <library/cpp/yson/writer.h>

namespace NYdb {

void FormatValueYson(const TValue& value, NYson::TYsonWriter& writer);

std::string FormatValueYson(const TValue& value, NYson::EYsonFormat ysonFormat = NYson::EYsonFormat::Text);

void FormatResultSetYson(const TResultSet& result, NYson::TYsonWriter& writer);

std::string FormatResultSetYson(const TResultSet& result, NYson::EYsonFormat ysonFormat = NYson::EYsonFormat::Text);

} // namespace NYdb
