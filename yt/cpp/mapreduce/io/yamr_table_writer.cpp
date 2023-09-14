#include "yamr_table_writer.h"

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TYaMRTableWriter::TYaMRTableWriter(THolder<IProxyOutput> output)
    : Output_(std::move(output))
{ }

TYaMRTableWriter::~TYaMRTableWriter()
{ }

size_t TYaMRTableWriter::GetBufferMemoryUsage() const
{
    return Output_->GetBufferMemoryUsage();
}

size_t TYaMRTableWriter::GetTableCount() const
{
    return Output_->GetStreamCount();
}

void TYaMRTableWriter::FinishTable(size_t tableIndex) {
    Output_->GetStream(tableIndex)->Finish();
}

void TYaMRTableWriter::AddRow(const TYaMRRow& row, size_t tableIndex)
{
    auto* stream = Output_->GetStream(tableIndex);

    auto writeField = [&stream] (const TStringBuf& field) {
        i32 length = static_cast<i32>(field.length());
        stream->Write(&length, sizeof(length));
        stream->Write(field.data(), field.length());
    };

    writeField(row.Key);
    writeField(row.SubKey);
    writeField(row.Value);

    Output_->OnRowFinished(tableIndex);
}

void TYaMRTableWriter::AddRow(TYaMRRow&& row, size_t tableIndex) {
    TYaMRTableWriter::AddRow(row, tableIndex);
}

void TYaMRTableWriter::Abort()
{
    Output_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
