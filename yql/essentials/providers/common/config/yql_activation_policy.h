#pragma once

#include "yql_config_qplayer.h"

#include <utility>

namespace NYql::NCommon {

template <typename TFilter, typename TObserver>
class TActivationSelectionPolicy {
public:
    TActivationSelectionPolicy(TFilter filter, TObserver observer)
        : Filter_(std::move(filter))
        , Observer_(std::move(observer))
    {
    }

    template <typename TAttribute, typename TContainer>
    TVector<TAttribute> SelectAndSave(
        const TString& activationLabel,
        const TQContext& qContext,
        const TContainer& source,
        bool hasProviderName) const {
        auto flags = SelectAndSaveActivatedFlags<TAttribute>(
            activationLabel,
            qContext,
            source,
            Filter_,
            hasProviderName);

        for (const auto& flag : flags) {
            if (flag.HasActivation()) {
                Observer_(flag.GetName());
            }
        }

        return flags;
    }

private:
    TFilter Filter_;
    TObserver Observer_;
};

} // namespace NYql::NCommon
