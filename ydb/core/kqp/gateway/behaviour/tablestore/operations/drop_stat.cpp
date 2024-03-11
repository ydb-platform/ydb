#include "drop_stat.h"
#include <util/string/type.h>

namespace NKikimr::NKqp {

TConclusionStatus TDropStatOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("NAME");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find parameter NAME");
        }
        Name = *fValue;
    }
    return TConclusionStatus::Success();
}

void TDropStatOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    *schemaData.AddDropStatistics() = Name;
}

}
