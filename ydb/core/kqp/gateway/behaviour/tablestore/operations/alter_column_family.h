#include "abstract.h"

#include <ydb/core/formats/arrow/accessor/abstract/request.h>
#include <ydb/core/formats/arrow/dictionary/diff.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

namespace NKikimr::NKqp::NColumnshard {

class TAlterColumnFamilyOperation: public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "ALTER_FAMILY";
    }

    static inline auto Registrator = TFactory::TRegistrator<TAlterColumnFamilyOperation>(GetTypeName());

    TString ColumnFamilyName;
    NArrow::NAccessor::TRequestedConstructorContainer AccessorConstructor;

public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}
