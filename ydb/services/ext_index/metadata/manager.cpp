#include "manager.h"
#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/ds_table/scheme_describe.h>

namespace NKikimr::NMetadata::NCSIndex {

class TPreparationController: public NProvider::ISchemeDescribeController {
private:
    NModifications::IAlterPreparationController<TObject>::TPtr ExtController;
    TObject Object;
public:
    TPreparationController(NModifications::IAlterPreparationController<TObject>::TPtr extController, TObject&& object)
        : ExtController(extController)
        , Object(std::move(object))
    {

    }

    virtual void OnDescriptionFailed(const TString& errorMessage, const TString& /*requestId*/) override {
        ExtController->OnPreparationProblem(errorMessage);
    }
    virtual void OnDescriptionSuccess(NMetadata::NProvider::TTableInfo&& result, const TString& /*requestId*/) override {
        if (!result->ColumnTableInfo) {
            ExtController->OnPreparationProblem("we cannot use this indexes for data-shard tables");
        } else if (!Object.TryProvideTtl(result->ColumnTableInfo->Description, nullptr)) {
            ExtController->OnPreparationProblem("unavailable cs-table ttl type for index construction");
        } else {
            ExtController->OnPreparationFinished({ std::move(Object) });
        }
    }
};

void TManager::DoPrepareObjectsBeforeModification(std::vector<TObject>&& patchedObjects,
    NModifications::IAlterPreparationController<TObject>::TPtr controller,
    const TInternalModificationContext& /*context*/, const NMetadata::NModifications::TAlterOperationContext& /*alterContext*/) const {
    if (patchedObjects.size() != 1) {
        controller->OnPreparationProblem("modification possible for one object only");
        return;
    }
    const TString path = patchedObjects.front().GetTablePath();
    std::shared_ptr<TPreparationController> pController = std::make_shared<TPreparationController>(controller, std::move(patchedObjects.front()));
    TActivationContext::Register(new NProvider::TSchemeDescriptionActor(pController, "", path));
}

NModifications::TOperationParsingResult TManager::DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
    TInternalModificationContext& context) const {
    if (context.GetActivityType() == IOperationsManager::EActivityType::Alter) {
        return TConclusionStatus::Fail("index modification currently unsupported");
    } else {
        NInternal::TTableRecord result;
        TStringBuf sb(settings.GetObjectId().data(), settings.GetObjectId().size());
        TStringBuf l;
        TStringBuf r;
        if (!sb.TrySplit(':', l, r)) {
            return TConclusionStatus::Fail("incorrect objectId format (path:index_id)");
        }
        result.SetColumn(TObject::TDecoder::TablePath, NInternal::TYDBValue::Utf8(l));
        result.SetColumn(TObject::TDecoder::IndexId, NInternal::TYDBValue::Utf8(r));

        if (context.GetActivityType() == IOperationsManager::EActivityType::Drop) {
            context.SetActivityType(IOperationsManager::EActivityType::Alter);
            result.SetColumn(TObject::TDecoder::Delete, NInternal::TYDBValue::Bool(true));
        } else if (context.GetActivityType() == IOperationsManager::EActivityType::Create) {
            if (auto dValue = settings.GetFeaturesExtractor().Extract<bool>(TObject::TDecoder::Delete, false)) {
                result.SetColumn(TObject::TDecoder::Delete, NInternal::TYDBValue::Bool(*dValue));
            } else {
                return TConclusionStatus::Fail("'delete' flag is incorrect");
            }

            if (auto extractorStr = settings.GetFeaturesExtractor().Extract(TObject::TDecoder::Extractor)) {
                TInterfaceContainer<IIndexExtractor> object;
                if (!object.DeserializeFromJson(*extractorStr)) {
                    return TConclusionStatus::Fail("cannot parse extractor info");
                }
                result.SetColumn(TObject::TDecoder::Extractor, NInternal::TYDBValue::Utf8(object.SerializeToJson().GetStringRobust()));
            } else {
                return TConclusionStatus::Fail("cannot found extractor info");
            }
            if (auto aValue = settings.GetFeaturesExtractor().Extract<bool>(TObject::TDecoder::Active, false)) {
                result.SetColumn(TObject::TDecoder::Active, NInternal::TYDBValue::Bool(*aValue));
            } else {
                return TConclusionStatus::Fail("'active' flag is incorrect");
            }
            if (!settings.GetFeaturesExtractor().IsFinished()) {
                return TConclusionStatus::Fail("undefined parameters: " + settings.GetFeaturesExtractor().GetRemainedParamsString());
            }
        }

        return result;
    }
}

}
