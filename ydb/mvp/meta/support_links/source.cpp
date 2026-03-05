#include "source.h"

#include <mutex>

#include <util/generic/yexception.h>

namespace NMVP {

namespace {

THashMap<TString, TLinkSourceFactory>& LinkSourceFactories() {
    static THashMap<TString, TLinkSourceFactory> factories;
    return factories;
}

std::mutex& LinkSourceFactoriesMutex() {
    static std::mutex mutex;
    return mutex;
}

} // namespace

class TUnsupportedLinkSource final : public ILinkSource {
public:
    explicit TUnsupportedLinkSource(TSupportLinkEntryConfig config)
        : SourceConfig(std::move(config))
    {}

    const TSupportLinkEntryConfig& Config() const override {
        return SourceConfig;
    }

    TResolveOutput Resolve(const TResolveInput&) const override {
        TResolveOutput output;
        output.Name = SourceConfig.GetSource();
        output.Ready = true;
        output.Errors.emplace_back(NSupportLinks::TSupportError{
            .Source = SourceConfig.GetSource(),
            .Message = TStringBuilder() << "unsupported support_links source: " << SourceConfig.GetSource(),
        });
        return output;
    }

private:
    TSupportLinkEntryConfig SourceConfig;
};

void RegisterLinkSource(TString source, TLinkSourceFactory factory) {
    if (source.empty()) {
        ythrow yexception() << "support_links source name is required";
    }
    if (!factory) {
        ythrow yexception() << "support_links source factory is required for " << source;
    }

    std::lock_guard guard(LinkSourceFactoriesMutex());
    LinkSourceFactories()[std::move(source)] = factory;
}

std::shared_ptr<ILinkSource> MakeLinkSource(TSupportLinkEntryConfig config) {
    if (config.GetSource().empty()) {
        ythrow yexception() << "source is required";
    }

    TLinkSourceFactory factory = nullptr;
    {
        std::lock_guard guard(LinkSourceFactoriesMutex());
        if (const auto it = LinkSourceFactories().find(config.GetSource()); it != LinkSourceFactories().end()) {
            factory = it->second;
        }
    }
    if (factory) {
        return factory(std::move(config));
    }

    return std::make_shared<TUnsupportedLinkSource>(std::move(config));
}

} // namespace NMVP
