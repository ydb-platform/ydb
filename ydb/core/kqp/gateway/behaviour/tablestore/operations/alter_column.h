#include "abstract.h"
#include <ydb/core/formats/arrow/compression/diff.h>

namespace NKikimr::NKqp::NColumnshard {

class TAlterColumnOperation : public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "ALTER_COLUMN";
    }

    static inline auto Registrator = TFactory::TRegistrator<TAlterColumnOperation>(GetTypeName());

    TString ColumnName;

    NArrow::TCompressionDiff CompressionDiff;
    std::optional<bool> LowCardinality;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchemaPreset& presetProto) const override;
};

}

