#include "schema.h"

namespace NKikimr::NSchemeShard {

bool TOlapOptionsDescription::ApplyUpdate(const TOlapOptionsUpdate& schemaUpdate, IErrorCollector& /*errors*/) {
    SchemeNeedActualization = schemaUpdate.GetSchemeNeedActualization();
    if (!!schemaUpdate.GetExternalGuaranteeExclusivePK()) {
        ExternalGuaranteeExclusivePK = *schemaUpdate.GetExternalGuaranteeExclusivePK();
    }
    return true;
}

void TOlapOptionsDescription::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    SchemeNeedActualization = tableSchema.GetOptions().GetSchemeNeedActualization();
    if (tableSchema.GetOptions().HasExternalGuaranteeExclusivePK()) {
        ExternalGuaranteeExclusivePK = tableSchema.GetOptions().GetExternalGuaranteeExclusivePK();
    }
}

void TOlapOptionsDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    tableSchema.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
    if (ExternalGuaranteeExclusivePK) {
        tableSchema.MutableOptions()->SetExternalGuaranteeExclusivePK(ExternalGuaranteeExclusivePK);
    }
}

bool TOlapOptionsDescription::Validate(const NKikimrSchemeOp::TColumnTableSchema& /*opSchema*/, IErrorCollector& /*errors*/) const {
    return true;
}

}
