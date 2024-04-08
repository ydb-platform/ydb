#pragma once

#include <yt/cpp/mapreduce/interface/common.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateYTTableSchemaFromArrowSchema(const std::shared_ptr<arrow::Schema>& arrowSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
