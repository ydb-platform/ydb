#pragma once

#include <yt/yt/client/table_client/public.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr CreateYTTableSchemaFromArrowSchema(
    const std::shared_ptr<arrow::Schema>& arrowSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
