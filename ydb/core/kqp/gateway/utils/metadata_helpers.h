#pragma once

#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NKqp {

class IFeatureFlagExtractor : TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFeatureFlagExtractor>;

    virtual ~IFeatureFlagExtractor() = default;

    virtual bool IsEnabled(const NKikimrConfig::TFeatureFlags& flags) const = 0;

    virtual bool IsEnabled(const TFeatureFlags& flags) const = 0;

    virtual TString GetMessageOnDisabled() const = 0;
};

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> CheckFeatureFlag(
    ui32 nodeId, IFeatureFlagExtractor::TPtr extractor,
    const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> ChainFeatures(
    NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> lastFeature,
    std::function<NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus>()> callback);

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> SendSchemeRequest(
    const NKikimrSchemeOp::TModifyScheme& schemeTx, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

}  // namespace NKikimr::NKqp
