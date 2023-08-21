#pragma once

#include "public.h"
#include "config.h"

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamredDsv(
    NYson::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = nullptr);

////////////////////////////////////////////////////////////////////////////////

void ParseYamredDsv(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = nullptr);

void ParseYamredDsv(
    TStringBuf data,
    NYson::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

