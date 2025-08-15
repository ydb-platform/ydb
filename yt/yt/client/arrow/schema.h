#pragma once

#include <yt/yt/client/table_client/public.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr CreateYTTableSchemaFromArrowSchema(
    const std::shared_ptr<arrow20::Schema>& arrowSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
