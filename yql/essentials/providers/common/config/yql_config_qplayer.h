#pragma once

#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/vector.h>

namespace NYql::NCommon {

inline const TString ActivationComponent = "Activation";

template <typename TAttribute>
TMaybe<TVector<TAttribute>> LoadActivatedFlagsFromQContext(const TString& activationLabel, const TQContext& qContext) {
    TMaybe<TVector<TAttribute>> loadedFlags;
    if (!qContext.CanRead()) {
        return loadedFlags;
    }
    if (auto loaded = qContext.GetReader()->Get({.Component = ActivationComponent, .Label = activationLabel}).GetValueSync()) {
        auto flagsNode = NYT::NodeFromYsonString(loaded->Value);
        TVector<TAttribute> flags;
        for (const auto& [flagName, flagValue] : flagsNode.AsMap()) {
            TAttribute flag;
            YQL_ENSURE(flag.ParseFromString(flagValue.AsString()));
            flags.emplace_back(std::move(flag));
        }
        loadedFlags = std::move(flags);
        YQL_CLOG(INFO, ProviderConfig) << activationLabel << " activated flags are loaded at replay mode";
    }
    return loadedFlags;
}

template <typename TAttribute>
void SaveActivatedFlagsToQContext(const TVector<TAttribute>& flags, const TString& activationLabel, const TQContext& qContext) {
    if (!qContext.CanWrite()) {
        return;
    }
    auto flagsNode = NYT::TNode::CreateMap();
    for (const auto& flag : flags) {
        TString data;
        YQL_ENSURE(flag.SerializeToString(&data));
        flagsNode[flag.GetName()] = std::move(data);
    }
    auto flagsYson = NYT::NodeToYsonString(flagsNode, NYT::NYson::EYsonFormat::Binary);
    qContext.GetWriter()->Put({.Component = ActivationComponent, .Label = activationLabel}, flagsYson).GetValueSync();
    YQL_CLOG(INFO, ProviderConfig) << activationLabel << " activated flags are saved to QStorage";
}

} // namespace NYql::NCommon
