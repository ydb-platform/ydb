#include "abstract.h"
#include <ydb/core/formats/arrow/accessor/abstract/request.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/dictionary/diff.h>

namespace NKikimr::NKqp::NColumnshard {

class TAlterColumnOperation : public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "ALTER_COLUMN";
    }

    static inline auto Registrator = TFactory::TRegistrator<TAlterColumnOperation>(GetTypeName());

    TString ColumnName;
    std::optional<TString> StorageId;

    NArrow::NSerialization::TSerializerContainer Serializer;
    NArrow::NDictionary::TEncodingDiff DictionaryEncodingDiff;
    std::optional<TString> DefaultValue;
    NArrow::NAccessor::TRequestedConstructorContainer AccessorConstructor;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}

