#include "yamr_table_writer.h"

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void WriteRow(IOutputStream* output, const TYaMRRow& row)
{
    auto writeField = [&output] (const TStringBuf& field) {
        i32 length = static_cast<i32>(field.length());
        output->Write(&length, sizeof(length));
        output->Write(field.data(), field.length());
    };

    writeField(row.Key);
    writeField(row.SubKey);
    writeField(row.Value);
}

////////////////////////////////////////////////////////////////////////////////

TYaMRTableWriter::TYaMRTableWriter(THolder<IProxyOutput> output)
    : Output_(output.Release())
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

    WriteRow(stream, row);

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

TYaMRTableFragmentWriter::TYaMRTableFragmentWriter(std::unique_ptr<IOutputStreamWithResponse> output)
    : Output_(std::move(output))
{ }

TWriteTableFragmentResult TYaMRTableFragmentWriter::GetWriteFragmentResult() const
{
    return TWriteTableFragmentResult(NodeFromYsonString(Output_->GetResponse()));
}

void TYaMRTableFragmentWriter::AddRow(const TYaMRRow& row)
{
    WriteRow(Output_.get(), row);
}

void TYaMRTableFragmentWriter::Finish()
{
    Output_->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
