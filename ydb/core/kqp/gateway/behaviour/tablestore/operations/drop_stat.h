#include "abstract.h"

namespace NKikimr::NKqp {

class TDropStatOperation : public ITableStoreOperation {
    static TString GetTypeName() {
        return "DROP_STAT";
    }

    static inline auto Registrator = TFactory::TRegistrator<TDropStatOperation>(GetTypeName());
private:
    TString Name;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;
    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

