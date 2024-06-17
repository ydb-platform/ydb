#include "upsert_opt.h"
#include <util/string/type.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp {

TConclusionStatus TUpsertOptionsOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    auto value = features.Extract<bool>("SCHEME_NEED_ACTUALIZATION", false);
    if (!value) {
        TConclusionStatus::Fail("Incorrect value for SCHEME_NEED_ACTUALIZATION: cannot parse as boolean");
    }
    SchemeNeedActualization = *value;
    return TConclusionStatus::Success();
}

void TUpsertOptionsOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    schemaData.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
}

}
