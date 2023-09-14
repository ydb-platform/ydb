#pragma once

#include <yt/cpp/mapreduce/interface/io.h>
#include <library/cpp/yson/public.h>

namespace NYT {

class IProxyOutput;

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
    THolder<IProxyOutput> Output_;
    TVector<THolder<::NYson::TYsonWriter>> Writers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
