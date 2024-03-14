#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/scheme/statistics/abstract/constructor.h>

namespace NKikimr::NKqp {

class TUpsertOptionsOperation : public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "UPSERT_OPTIONS";
    }

    static inline const auto Registrator = TFactory::TRegistrator<TUpsertOptionsOperation>(GetTypeName());
private:
    bool SchemeNeedActualization = false;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

