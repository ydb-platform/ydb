#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSchemafulDsv(
    NYson::IYsonConsumer* consumer,
    TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

void ParseSchemafulDsv(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

void ParseSchemafulDsv(
    TStringBuf data,
    NYson::IYsonConsumer* consumer,
    TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

