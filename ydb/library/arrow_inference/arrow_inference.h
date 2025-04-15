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

class TArrowInference {
public:
    static std::variant<ArrowFields, TString> InferTypes(std::shared_ptr<arrow::io::RandomAccessFile> file, std::shared_ptr<FormatConfig> config);
    static Ydb::Type ArrowToYdbType(const std::shared_ptr<arrow::DataType>& arrowType, std::shared_ptr<FormatConfig> config);
};

} // namespace NYdb 