#include "stream_log_writer.h"

#include "log_writer_detail.h"
#include "log_writer_factory.h"
#include "formatter.h"

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NLogging {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriter
    : public TStreamLogWriterBase
{
public:
    TStreamLogWriter(
        std::unique_ptr<ILogFormatter> formatter,
        TString name,
        IOutputStream* stream)
        : TStreamLogWriterBase(
            std::move(formatter),
            std::move(name))
        , Stream_(stream)
    { }

private:
    IOutputStream* const Stream_;

    IOutputStream* GetOutputStream() const noexcept override
    {
        return Stream_;
    }
};

ILogWriterPtr CreateStreamLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    TString name,
    IOutputStream* stream)
{
    return New<TStreamLogWriter>(
        std::move(formatter),
        std::move(name),
        stream);
}

////////////////////////////////////////////////////////////////////////////////

ILogWriterPtr CreateStderrLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    TString name)
{
    return CreateStreamLogWriter(
        std::move(formatter),
        std::move(name),
        &Cerr);
}

////////////////////////////////////////////////////////////////////////////////

class TStderrLogWriterFactory
    : public ILogWriterFactory
{
public:
    void ValidateConfig(
        const NYTree::IMapNodePtr& configNode) override
    {
        ConvertTo<TStderrLogWriterConfigPtr>(configNode);
    }

    ILogWriterPtr CreateWriter(
        std::unique_ptr<ILogFormatter> formatter,
        TString name,
        const NYTree::IMapNodePtr& /*configNode*/,
        ILogWriterHost* /*host*/) noexcept override
    {
        return CreateStderrLogWriter(
            std::move(formatter),
            std::move(name));
    }
};

////////////////////////////////////////////////////////////////////////////////

ILogWriterFactoryPtr GetStderrLogWriterFactory()
{
    return LeakyRefCountedSingleton<TStderrLogWriterFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
