#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

class IProxyOutput;
class IOutputStreamWithResponse;

////////////////////////////////////////////////////////////////////////////////

class TYaMRTableWriter
    : public IYaMRWriterImpl
{
public:
    explicit TYaMRTableWriter(THolder<IProxyOutput> output);
    ~TYaMRTableWriter() override;

    void AddRow(const TYaMRRow& row, size_t tableIndex) override;
    void AddRow(TYaMRRow&& row, size_t tableIndex) override;

    size_t GetBufferMemoryUsage() const override;
    size_t GetTableCount() const override;
    void FinishTable(size_t) override;
    void Abort() override;

private:
    std::unique_ptr<IProxyOutput> Output_;
};

////////////////////////////////////////////////////////////////////////////////

class TYaMRTableFragmentWriter
    : public ITableFragmentWriter<TYaMRRow>
{
public:
    explicit TYaMRTableFragmentWriter(std::unique_ptr<IOutputStreamWithResponse> output);

    TWriteTableFragmentResult GetWriteFragmentResult() const override;

    void AddRow(const TYaMRRow& row) override;

    void Finish() override;

private:
    std::unique_ptr<IOutputStreamWithResponse> Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
