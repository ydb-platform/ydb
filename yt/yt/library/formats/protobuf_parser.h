#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForProtobuf(
    NTableClient::IValueConsumer* consumer,
    TProtobufFormatConfigPtr config,
    int tableIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
