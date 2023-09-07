#pragma once

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/parser.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TFormatsConsumerBase
    : public virtual NYson::IFlushableYsonConsumer
{
public:
    TFormatsConsumerBase();

    // This method has standard implementation for YAMR, DSV and YAMRED DSV formats.
    void OnRaw(TStringBuf yson, NYson::EYsonType type) override;

    void Flush() override;

private:
    NYson::TStatelessYsonParser Parser;
};

////////////////////////////////////////////////////////////////////////////////

void WriteUnversionedValue(const NTableClient::TUnversionedValue& value, IOutputStream* output, const TEscapeTable& escapeTable);

bool IsTrivialIntermediateSchema(const NTableClient::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
