#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/constructor.h>

namespace NKikimr::NKqp {

class TUpsertIndexOperation : public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "UPSERT_INDEX";
    }

    static inline auto Registrator = TFactory::TRegistrator<TUpsertIndexOperation>(GetTypeName());
private:
    TString IndexName;
    NBackgroundTasks::TInterfaceProtoContainer<NOlap::NIndexes::IIndexMetaConstructor> IndexMetaConstructor;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

