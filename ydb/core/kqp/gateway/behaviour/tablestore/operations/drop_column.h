#include "abstract.h"

namespace NKikimr::NKqp {

class TDropColumnOperation : public ITableStoreOperation {
    static TString GetTypeName() {
        return "DROP_COLUMN";
    }

    static inline auto Registrator = TFactory::TRegistrator<TDropColumnOperation>(GetTypeName());
private:
    TString ColumnName;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;
    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

