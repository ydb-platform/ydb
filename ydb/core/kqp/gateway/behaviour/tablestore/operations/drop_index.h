#include "abstract.h"

namespace NKikimr::NKqp {

class TDropIndexOperation : public ITableStoreOperation {
    static TString GetTypeName() {
        return "DROP_INDEX";
    }

    static inline auto Registrator = TFactory::TRegistrator<TDropIndexOperation>(GetTypeName());
private:
    TString IndexName;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;
    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

