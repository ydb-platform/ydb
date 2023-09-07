#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamr(
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = New<TYamrFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

void ParseYamr(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = New<TYamrFormatConfig>());

void ParseYamr(
    TStringBuf data,
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = New<TYamrFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
