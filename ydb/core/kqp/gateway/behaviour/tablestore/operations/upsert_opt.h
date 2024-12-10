#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/data_accessor/abstract/constructor.h>

namespace NKikimr::NKqp {

class TUpsertOptionsOperation: public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "UPSERT_OPTIONS";
    }

    static inline const auto Registrator = TFactory::TRegistrator<TUpsertOptionsOperation>(GetTypeName());
private:
    bool SchemeNeedActualization = false;
    std::optional<TString> ScanReaderPolicyName;
    NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer CompactionPlannerConstructor;
    NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer MetadataManagerConstructor;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

