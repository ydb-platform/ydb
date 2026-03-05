#include "source.h"

#include <util/generic/yexception.h>

namespace NMVP {

class TUnsupportedLinkSource final : public ILinkSource {
public:
    TUnsupportedLinkSource(size_t place, TSupportLinkEntryConfig config)
        : Place_(place)
        , Config_(std::move(config))
    {}

    size_t Place() const override {
        return Place_;
    }

    const TSupportLinkEntryConfig& Config() const override {
        return Config_;
    }

    TResolveOutput Resolve(const TResolveInput&) const override {
        TResolveOutput output;
        output.Name = Config_.GetSource();
        output.Ready = true;
        output.Errors.emplace_back(NSupportLinks::TSupportError{
            .Source = Config_.GetSource(),
            .Message = TStringBuilder() << "unsupported support_links source: " << Config_.GetSource(),
        });
        return output;
    }

private:
    size_t Place_;
    TSupportLinkEntryConfig Config_;
};

std::shared_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntryConfig config) {
    if (config.GetSource().empty()) {
        ythrow yexception() << "source is required";
    }
    return std::make_shared<TUnsupportedLinkSource>(place, std::move(config));
}

} // namespace NMVP
