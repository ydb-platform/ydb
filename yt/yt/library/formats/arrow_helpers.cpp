#include "arrow_helpers.h"

#include "arrow_metadata_constants.h"

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> GetArrowMetadataYTType(const std::shared_ptr<arrow20::Field>& schemaField)
{
    auto columnMetadata = schemaField->metadata();
    if (!columnMetadata) {
        return std::nullopt;
    }
    auto valueResult = columnMetadata->Get(YTTypeMetadataKey);
    if (valueResult.ok()) {
        return *valueResult;
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
