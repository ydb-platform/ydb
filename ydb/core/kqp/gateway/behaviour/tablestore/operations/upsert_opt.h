#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NKqp {

class TUpsertOptionsOperation : public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "UPSERT_OPTIONS";
    }

    static inline const auto Registrator = TFactory::TRegistrator<TUpsertOptionsOperation>(GetTypeName());
private:
    bool SchemeNeedActualization = false;
    std::optional<bool> ExternalGuaranteeExclusivePK;
    NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer CompactionPlannerConstructor;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

