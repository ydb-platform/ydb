#pragma once

#include "escape.h"
#include "helpers.h"
#include "schemaless_writer_adapter.h"

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TDsvWriterBase
{
public:
    explicit TDsvWriterBase(TDsvFormatConfigPtr config);

protected:
    const TDsvFormatConfigPtr Config_;
    TEscapeTable KeyEscapeTable_;
    TEscapeTable ValueEscapeTable_;
};

////////////////////////////////////////////////////////////////////////////////

// YsonNode is written as follows:
//  * Each element of list is ended with RecordSeparator
//  * Items in map are separated with FieldSeparator
//  * Key and Values in map are separated with KeyValueSeparator
class TDsvNodeConsumer
    : public TDsvWriterBase
    , public TFormatsConsumerBase
{
public:
    explicit TDsvNodeConsumer(
        IOutputStream* stream,
        TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

    // IYsonConsumer overrides.
    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;

private:
    IOutputStream* const Stream_;

    bool AllowBeginList_ = true;
    bool AllowBeginMap_ = true;

    bool BeforeFirstMapItem_ = true;
    bool BeforeFirstListItem_ = true;
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForDsv(
    TDsvFormatConfigPtr config,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int /* keyColumnCount */);

ISchemalessFormatWriterPtr CreateSchemalessWriterForDsv(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int /* keyColumnCount */);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
