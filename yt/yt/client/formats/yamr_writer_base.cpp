#include "yamr_writer_base.h"
#include "yamr_writer.h"

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/format.h>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterForYamrBase::TSchemalessWriterForYamrBase(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount,
    TYamrFormatConfigBasePtr config)
    : TSchemalessFormatWriterBase(
        nameTable,
        std::move(output),
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount)
    , Config_(config)
{ }

void TSchemalessWriterForYamrBase::WriteInLenvalMode(TStringBuf value)
{
    auto* stream = GetOutputStream();
    WritePod(*stream, static_cast<ui32>(value.size()));
    stream->Write(value);
}

void TSchemalessWriterForYamrBase::WriteTableIndex(i64 tableIndex)
{
    TableIndexWasWritten_ = true;
    CurrentTableIndex_ = tableIndex;

    auto* stream = GetOutputStream();

    if (!Config_->EnableTableIndex) {
        // Silently ignore table switches.
        return;
    }

    if (Config_->Lenval) {
        WritePod(*stream, static_cast<ui32>(-1));
        WritePod(*stream, static_cast<ui32>(tableIndex));
    } else {
        stream->Write(ToString(tableIndex));
        stream->Write(Config_->RecordSeparator);
    }
}

void TSchemalessWriterForYamrBase::WriteRangeIndex(i64 rangeIndex)
{
    YT_VERIFY(Config_->Lenval);

    auto* stream = GetOutputStream();
    WritePod(*stream, static_cast<ui32>(-3));
    WritePod(*stream, static_cast<ui32>(rangeIndex));
}

void TSchemalessWriterForYamrBase::WriteRowIndex(i64 rowIndex)
{
    auto* stream = GetOutputStream();
    if (Config_->Lenval) {
        WritePod(*stream, static_cast<ui32>(-4));
        WritePod(*stream, static_cast<ui64>(rowIndex));
    } else {
        if (!TableIndexWasWritten_) {
            stream->Write(ToString(CurrentTableIndex_));
            stream->Write(Config_->RecordSeparator);
        }
        stream->Write(ToString(rowIndex));
        stream->Write(Config_->RecordSeparator);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
