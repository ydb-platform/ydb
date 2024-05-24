#include "alter_sharding.h"
#include <util/string/type.h>

namespace NKikimr::NKqp {

TConclusionStatus TAlterShardingOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    const std::optional<TString> modification = features.Extract<TString>("MODIFICATION");
    if (!modification) {
        return TConclusionStatus::Fail("modification type not specified in request");
    }
    if (*modification == "SPLIT") {
        Increase = true;
    } else if (*modification == "MERGE") {
        return TConclusionStatus::Fail("modification is impossible yet");
    } else {
        return TConclusionStatus::Fail("undefined modification: \"" + *modification + "\"");
    }
    return TConclusionStatus::Success();
}

void TAlterShardingOperation::DoSerializeScheme(NKikimrSchemeOp::TModifyScheme& scheme, const bool isStandalone) const {
    AFL_VERIFY(!isStandalone);
    scheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnTable);
    scheme.MutableAlterColumnTable()->SetName(GetStoreName());
    *scheme.MutableAlterColumnTable()->MutableReshardColumnTable() = NKikimrSchemeOp::TReshardColumnTable();
}

}
