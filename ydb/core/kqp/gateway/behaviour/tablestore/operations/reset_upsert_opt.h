#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NKqp {

class TResetUpsertOptionsOperation: public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "RESET_UPSERT_OPTIONS";
    }

    static inline const auto Registrator = TFactory::TRegistrator<TResetUpsertOptionsOperation>(GetTypeName());
private:
    NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer CompactionPlannerConstructor;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}
