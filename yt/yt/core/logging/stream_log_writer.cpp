#include "stream_log_writer.h"

#include "log_writer_detail.h"
#include "log_writer_factory.h"
#include "formatter.h"
#include "system_log_event_provider.h"

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
        std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
        TString name,
        TLogWriterConfigPtr config,
        IOutputStream* stream)
        : TStreamLogWriterBase(
            std::move(formatter),
            std::move(systemEventProvider),
            std::move(name),
            std::move(config))
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
    std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
    TString name,
    TLogWriterConfigPtr config,
    IOutputStream* stream)
{
    return New<TStreamLogWriter>(
        std::move(formatter),
        std::move(systemEventProvider),
        std::move(name),
        std::move(config),
        stream);
}

////////////////////////////////////////////////////////////////////////////////

ILogWriterPtr CreateStderrLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
    TString name,
    TStderrLogWriterConfigPtr config)
{
    return CreateStreamLogWriter(
        std::move(formatter),
        std::move(systemEventProvider),
        std::move(name),
        std::move(config),
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
        ParseConfig(configNode);
    }

    ILogWriterPtr CreateWriter(
        std::unique_ptr<ILogFormatter> formatter,
        TString name,
        const NYTree::IMapNodePtr& configNode,
        ILogWriterHost* /*host*/) noexcept override
    {
        auto config = ParseConfig(configNode);
        return CreateStderrLogWriter(
            std::move(formatter),
            CreateDefaultSystemLogEventProvider(config),
            std::move(name),
            std::move(config));
    }

private:
    static TStderrLogWriterConfigPtr ParseConfig(const NYTree::IMapNodePtr& configNode)
    {
        return ConvertTo<TStderrLogWriterConfigPtr>(configNode);
    }
};

////////////////////////////////////////////////////////////////////////////////

ILogWriterFactoryPtr GetStderrLogWriterFactory()
{
    return LeakyRefCountedSingleton<TStderrLogWriterFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
