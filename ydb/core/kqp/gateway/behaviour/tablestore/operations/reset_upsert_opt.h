#include "abstract.h"
#include <ydb/core/tx/columnshard/data_accessor/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NKqp {

class TResetUpsertOptionsOperation: public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "RESET_UPSERT_OPTIONS";
    }

    static inline const auto Registrator = TFactory::TRegistrator<TResetUpsertOptionsOperation>(GetTypeName());

    enum class EResetTarget : ui8 {
        None = 0,
        CompactionPlanner,
        MetadataMemoryManager,
        SchemeNeedActualization,
        ScanReaderPolicyName,
    };

private:
    EResetTarget Target = EResetTarget::None;
    NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer CompactionPlannerConstructor;
    NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer MetadataManagerConstructor;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}
