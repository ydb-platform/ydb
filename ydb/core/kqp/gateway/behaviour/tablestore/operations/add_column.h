#include "abstract.h"

namespace NKikimr::NKqp {

class TAddColumnOperation : public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "NEW_COLUMN";
    }

    static inline auto Registrator = TFactory::TRegistrator<TAddColumnOperation>(GetTypeName());
private:
    TString ColumnName;
    TString ColumnType;
    std::optional<TString> StorageId;
    bool NotNull = false;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

