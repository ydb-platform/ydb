#pragma once

#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/overloaded.h>

#include <variant>

namespace NYql::NCommon {

namespace NPrivate {

inline constexpr TStringBuf QplayerActivationComponent = "Activation";
inline constexpr TStringBuf QplayerActivationVersion2 = "v2";
inline constexpr TStringBuf QplayerActivationVersion1 = "v1";

inline constexpr TStringBuf QPlayerActivationVersionKey = "version";
inline constexpr TStringBuf QPlayerActivationIndexesKey = "indexes";

enum class EQContextFlagsVersion {
    V1,
    V2
};

inline EQContextFlagsVersion EQContextFlagsVersionFromString(TStringBuf versionStr) {
    if (versionStr == QplayerActivationVersion1) {
        return EQContextFlagsVersion::V1;
    }
    if (versionStr == QplayerActivationVersion2) {
        return EQContextFlagsVersion::V2;
    }
    ythrow yexception() << "Unknown activation storage version: " << versionStr;
}

template <typename TAttribute>
using TActivationLoadResult = std::variant<TVector<TAttribute>, THashSet<size_t>>;

template <typename TAttribute>
TMaybe<TActivationLoadResult<TAttribute>> LoadActivatedFlagsFromQContext(const TString& activationLabel, const TQContext& qContext) {
    if (!qContext.CanRead()) {
        return Nothing();
    }
    auto loaded = qContext.GetReader()->Get({.Component = TString(QplayerActivationComponent), .Label = activationLabel}).GetValueSync();
    if (!loaded) {
        return Nothing();
    }
    auto flagsNode = NYT::NodeFromYsonString(loaded->Value);
    EQContextFlagsVersion version = EQContextFlagsVersion::V1;
    if (flagsNode.HasKey(QPlayerActivationVersionKey)) {
        version = EQContextFlagsVersionFromString(flagsNode[QPlayerActivationVersionKey].AsString());
    }

    switch (version) {
        case EQContextFlagsVersion::V1: {
            TVector<TAttribute> storedFlags;
            for (const auto& [flagName, flagValue] : flagsNode.AsMap()) {
                TAttribute flag;
                YQL_ENSURE(flag.ParseFromString(flagValue.AsString()));
                storedFlags.emplace_back(std::move(flag));
            }
            return {std::move(storedFlags)};
        }

        case EQContextFlagsVersion::V2: {
            THashSet<size_t> activatedIndexes;
            for (const auto& indexNode : flagsNode[QPlayerActivationIndexesKey].AsList()) {
                activatedIndexes.insert(static_cast<size_t>(indexNode.AsInt64()));
            }
            return {std::move(activatedIndexes)};
        }
    }
}

inline void SaveActivatedFlagsToQContext(const TVector<size_t>& activatedIndexes, const TString& activationLabel, const TQContext& qContext) {
    if (!qContext.CanWrite()) {
        return;
    }
    auto node = NYT::TNode::CreateMap();
    node[QPlayerActivationVersionKey] = QplayerActivationVersion2;
    auto indexList = NYT::TNode::CreateList();
    for (size_t index : activatedIndexes) {
        indexList.Add(static_cast<i64>(index));
    }
    node[QPlayerActivationIndexesKey] = std::move(indexList);
    auto yson = NYT::NodeToYsonString(node, NYT::NYson::EYsonFormat::Binary);
    qContext.GetWriter()->Put({.Component = TString(QplayerActivationComponent), .Label = activationLabel}, yson).GetValueSync();
    YQL_CLOG(INFO, ProviderConfig) << activationLabel << " activated flags are saved to QStorage";
}

template <typename TAttribute, typename TContainer>
TVector<TAttribute> CollectFlagsForReplayV2(const TContainer& source, const THashSet<size_t>& activatedIndexes) {
    TVector<TAttribute> flags;
    for (size_t idx = 0; idx < static_cast<size_t>(source.size()); ++idx) {
        if (!source[idx].HasActivation() || activatedIndexes.contains(idx)) {
            flags.emplace_back(source[idx]);
        }
    }
    return flags;
}

template <typename TAttribute, typename TContainer, typename TFilter>
TVector<TAttribute> FilterSourceAndSaveIndexes(
    const TContainer& source,
    const TFilter& filter,
    const TString& activationLabel,
    const TQContext& qContext,
    bool hasProviderName)
{
    TVector<TAttribute> flags;
    TVector<size_t> activatedIndexes;
    for (size_t idx = 0; idx < static_cast<size_t>(source.size()); ++idx) {
        if (filter(source[idx])) {
            flags.emplace_back(source[idx]);
            activatedIndexes.push_back(idx);
        }
    }
    if (hasProviderName) {
        SaveActivatedFlagsToQContext(activatedIndexes, activationLabel, qContext);
    }
    return flags;
}

} // namespace NPrivate

template <typename TAttribute, typename TContainer, typename TFilter>
TVector<TAttribute> SelectAndSaveActivatedFlags(
    const TString& activationLabel,
    const TQContext& qContext,
    const TContainer& source,
    const TFilter& filter,
    bool hasProviderName)
{
    if (auto loaded = NPrivate::LoadActivatedFlagsFromQContext<TAttribute>(activationLabel, qContext)) {
        YQL_CLOG(INFO, ProviderConfig) << activationLabel << " activated flags are loaded at replay mode";
        return std::visit(TOverloaded{
                              [](const TVector<TAttribute>& flags) {
                                  return flags;
                              },
                              [&](const THashSet<size_t>& indexes) {
                                  return NPrivate::CollectFlagsForReplayV2<TAttribute>(source, indexes);
                              }}, *loaded);
    }
    return NPrivate::FilterSourceAndSaveIndexes<TAttribute>(source, filter, activationLabel, qContext, hasProviderName);
}

} // namespace NYql::NCommon
