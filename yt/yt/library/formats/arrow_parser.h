#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForArrow(NTableClient::IValueConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
