#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TArrowParserOptions
{
    //! If true, Arrow internal allocations are capped at a fixed limit per
    //! single allocation. Enable this in fuzz tests to convert OOM into
    //! a catchable exception instead of a process kill.
    bool EnableMemoryLimit = false;
};

std::unique_ptr<IParser> CreateParserForArrow(
    NTableClient::IValueConsumer* consumer,
    const TArrowParserOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
