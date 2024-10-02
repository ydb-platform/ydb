#pragma once
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NModifications {

class IPreprocessingController {
public:
    using TPtr = std::shared_ptr<IPreprocessingController>;

    virtual void OnPreprocessingFinished(NYql::TObjectSettingsImpl result) = 0;
    virtual void OnPreprocessingProblem(const TString& errorMessage) = 0;

    virtual ~IPreprocessingController() = default;
};

class TSchemeObjectOperationsManager: public IOperationsManager {
private:
    using IOperationsManager::TYqlConclusionStatus;

public:
    using TPtr = std::shared_ptr<TSchemeObjectOperationsManager>;

    using TInternalModificationContext = typename IOperationsManager::TInternalModificationContext;
    using TExternalModificationContext = typename IOperationsManager::TExternalModificationContext;
    using TOperationParsingResult = TOperationParsingResult;
    using IPreprocessingController = IPreprocessingController;
    using TObjectDependencies = std::vector<TPathId>;

protected:
    virtual void DoPreprocessSettings(
        const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IPreprocessingController::TPtr controller) const = 0;
    virtual TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings, NSchemeShard::TSchemeShard& context) const = 0;
    virtual TConclusion<TObjectDependencies> DoValidateOperation(
        const TString& objectId, const TBaseObject::TPtr& object, EActivityType activity, NSchemeShard::TSchemeShard& context) const = 0;

public:
    TOperationParsingResult BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings, NSchemeShard::TSchemeShard& context) const {
        TOperationParsingResult result = DoBuildPatchFromSettings(settings, context);
        if (result.IsSuccess()) {
            if (!settings.GetFeaturesExtractor().IsFinished()) {
                return TConclusionStatus::Fail("undefined params: " + settings.GetFeaturesExtractor().GetRemainedParamsString());
            }
        }
        return result;
    }

    TConclusion<TObjectDependencies> ValidateOperation(
        const TString& objectId, const TBaseObject::TPtr& object, EActivityType activity, NSchemeShard::TSchemeShard& context) const {
        return DoValidateOperation(objectId, object, activity, context);
    }

protected:
    NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 /*nodeId*/,
        const IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override;

    TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& /*schemeOperation*/, const NYql::TObjectSettingsImpl& settings,
        const IClassBehaviour::TPtr& /*manager*/, TInternalModificationContext& /*context*/) const override {
        return TYqlConclusionStatus::Fail("Prepare operation is not supported for " + settings.GetTypeId() + " objects.");
    }

    NThreading::TFuture<TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& /*schemeOperation*/, const ui32 /*nodeId*/,
        const IClassBehaviour::TPtr& /*manager*/, const TExternalModificationContext& /*context*/) const override {
        return NThreading::MakeFuture(TYqlConclusionStatus::Fail("Execute prepared operation is not supported for this object."));
    }
};

}   // namespace NKikimr::NMetadata::NModifications
