#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TArrowParserOptions
{
    //! Caps Arrow internal allocations and record-batch size checks.
    //! std::nullopt means no limit. Set to a reasonable value (e.g. 512 MB) in
    //! fuzz tests to convert OOM into a catchable exception.
    std::optional<i64> MaxAllocationBytes;
};

std::unique_ptr<IParser> CreateParserForArrow(
    NTableClient::IValueConsumer* consumer,
    const TArrowParserOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
