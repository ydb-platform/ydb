#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \param wrapWithMap If True then the parser wraps values with calls to
 *  #IYsonConsumer::OnBeginMap and #IYsonConsumer::OnEndMap.
 */
std::unique_ptr<IParser> CreateParserForDsv(
    NYson::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = New<TDsvFormatConfig>(),
    bool wrapWithMap = true);

////////////////////////////////////////////////////////////////////////////////

void ParseDsv(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

void ParseDsv(
    TStringBuf data,
    NYson::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
