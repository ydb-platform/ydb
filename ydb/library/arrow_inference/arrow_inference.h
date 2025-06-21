#pragma once

#include "config.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/hash.h>

#include <variant>
#include <memory>

namespace NYdb::NArrowInference {

using ArrowField = std::shared_ptr<arrow::Field>;
using ArrowFields = std::vector<ArrowField>;

std::variant<ArrowFields, TString> InferTypes(const std::vector<std::shared_ptr<arrow::io::InputStream>>& inputs, std::shared_ptr<TFormatConfig> config);
bool ArrowToYdbType(Ydb::Type& result, const arrow::DataType& type, std::shared_ptr<TFormatConfig> config);

} // namespace NYdb 