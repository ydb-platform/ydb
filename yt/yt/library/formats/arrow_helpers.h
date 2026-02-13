#pragma once

#include <yt/yt/client/formats/public.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type_fwd.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> GetArrowMetadataYTType(const std::shared_ptr<arrow20::Field>& schemaField);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
