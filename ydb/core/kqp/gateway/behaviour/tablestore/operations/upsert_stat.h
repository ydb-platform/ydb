#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/scheme/statistics/abstract/constructor.h>

namespace NKikimr::NKqp {

class TUpsertStatOperation : public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "UPSERT_STAT";
    }

    static inline const auto Registrator = TFactory::TRegistrator<TUpsertStatOperation>(GetTypeName());
private:
    TString Name;
    NOlap::NStatistics::TConstructorContainer Constructor;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

