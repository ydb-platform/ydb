#include "drop_index.h"
#include <util/string/type.h>

namespace NKikimr::NKqp {

TConclusionStatus TDropIndexOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("NAME");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find parameter NAME");
        }
        IndexName = *fValue;
    }
    return TConclusionStatus::Success();
}

void TDropIndexOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    *schemaData.AddDropIndexes() = IndexName;
}

}
