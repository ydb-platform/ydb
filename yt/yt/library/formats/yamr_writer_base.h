#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

#include "helpers.h"
#include "escape.h"
#include "schemaless_writer_adapter.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/blob_output.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForYamrBase
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessWriterForYamrBase(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TYamrFormatConfigBasePtr config);

protected:
    TYamrFormatConfigBasePtr Config_;
    bool TableIndexWasWritten_ = false;
    int CurrentTableIndex_ = 0;

    void WriteInLenvalMode(TStringBuf value);

    void WriteTableIndex(i64 tableIndex) override;
    void WriteRangeIndex(i64 rangeIndex) override;
    void WriteRowIndex(i64 rowIndex) override;
};

DEFINE_REFCOUNTED_TYPE(TSchemalessWriterForYamrBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
