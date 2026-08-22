#include "node_table_writer.h"

#include <yt/cpp/mapreduce/common/node_visitor.h>

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yson/writer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void WriteRow(::NYson::TYsonWriter* writer, const TNode& row)
{
    if (row.HasAttributes()) {
        ythrow TIOException() << "Row cannot have attributes";
    }

    static const TNode emptyMap = TNode::CreateMap();
    const TNode* outRow = &emptyMap;
    if (row.GetType() != TNode::Undefined) {
        if (!row.IsMap()) {
            ythrow TIOException() << "Row should be a map node";
        } else {
            outRow = &row;
        }
    }

    writer->OnListItem();

    TNodeVisitor visitor(writer);
    visitor.Visit(*outRow);
}

TNodeTableWriter::TNodeTableWriter(THolder<IProxyOutput> output, NYson::EYsonFormat format)
    : Output_(output.Release())
{
    for (size_t i = 0; i < Output_->GetStreamCount(); ++i) {
        Writers_.push_back(
            std::make_unique<::NYson::TYsonWriter>(Output_->GetStream(i), format, NYT::NYson::EYsonType::ListFragment));
    }
}

TNodeTableWriter::~TNodeTableWriter()
{ }

size_t TNodeTableWriter::GetBufferMemoryUsage() const
{
    return Output_->GetBufferMemoryUsage();
}

size_t TNodeTableWriter::GetTableCount() const
{
    return Output_->GetStreamCount();
}

void TNodeTableWriter::FinishTable(size_t tableIndex) {
    Output_->GetStream(tableIndex)->Finish();
}

void TNodeTableWriter::AddRow(const TNode& row, size_t tableIndex)
{
    auto* writer = Writers_[tableIndex].get();

    WriteRow(writer, row);

    Output_->OnRowFinished(tableIndex);
}

void TNodeTableWriter::AddRow(TNode&& row, size_t tableIndex) {
    AddRow(row, tableIndex);
}

void TNodeTableWriter::Abort()
{
    Output_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

TNodeTableFragmentWriter::TNodeTableFragmentWriter(std::unique_ptr<IOutputStreamWithResponse> output, ::NYson::EYsonFormat format)
    : Output_(std::move(output))
    , Writer_(std::make_unique<::NYson::TYsonWriter>(Output_.get(), format, NYT::NYson::EYsonType::ListFragment))
{ }

TWriteTableFragmentResult TNodeTableFragmentWriter::GetWriteFragmentResult() const
{
    return TWriteTableFragmentResult(NodeFromYsonString(Output_->GetResponse()));
}

void TNodeTableFragmentWriter::AddRow(const TNode& row)
{
    WriteRow(Writer_.get(), row);
}

void TNodeTableFragmentWriter::Finish()
{
    Output_->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
