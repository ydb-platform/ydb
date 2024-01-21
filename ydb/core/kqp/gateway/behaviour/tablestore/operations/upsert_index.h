#include "abstract.h"

namespace NKikimr::NKqp {

class TUpsertIndexOperation : public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "UPSERT_INDEX";
    }

    static inline auto Registrator = TFactory::TRegistrator<TUpsertIndexOperation>(GetTypeName());
private:
    TString IndexName;
    TInterfaceProtoContainer<NOlap::IIndexMetaConstructor> IndexMetaConstructor;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

