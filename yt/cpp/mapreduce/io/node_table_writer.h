#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

#include <library/cpp/yson/public.h>

namespace NYT {

class IProxyOutput;
class IOutputStreamWithResponse;

////////////////////////////////////////////////////////////////////////////////

class TNodeTableWriter
    : public INodeWriterImpl
{
public:
    explicit TNodeTableWriter(THolder<IProxyOutput> output, ::NYson::EYsonFormat format = ::NYson::EYsonFormat::Binary);
    ~TNodeTableWriter() override;

    void AddRow(const TNode& row, size_t tableIndex) override;
    void AddRow(TNode&& row, size_t tableIndex) override;

    size_t GetBufferMemoryUsage() const override;
    size_t GetTableCount() const override;
    void FinishTable(size_t) override;
    void Abort() override;

private:
    std::unique_ptr<IProxyOutput> Output_;
    TVector<std::unique_ptr<::NYson::TYsonWriter>> Writers_;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeTableFragmentWriter
    : public ITableFragmentWriter<TNode>
{
public:
    explicit TNodeTableFragmentWriter(std::unique_ptr<IOutputStreamWithResponse> output, ::NYson::EYsonFormat format = ::NYson::EYsonFormat::Binary);

    TWriteTableFragmentResult GetWriteFragmentResult() const override;

    void AddRow(const TNode& row) override;

    void Finish() override;

private:
    std::unique_ptr<IOutputStreamWithResponse> Output_;
    std::unique_ptr<::NYson::TYsonWriter> Writer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
