#include "schema.h"

namespace NKikimr::NSchemeShard {

bool TOlapOptionsDescription::ApplyUpdate(const TOlapOptionsUpdate& schemaUpdate, IErrorCollector& /*errors*/) {
    SchemeNeedActualization = schemaUpdate.GetSchemeNeedActualization();
    if (!!schemaUpdate.GetExternalGuaranteeExclusivePK()) {
        ExternalGuaranteeExclusivePK = *schemaUpdate.GetExternalGuaranteeExclusivePK();
    }
    if (schemaUpdate.GetCompactionPlannerConstructor().HasObject()) {
        CompactionPlannerConstructor = schemaUpdate.GetCompactionPlannerConstructor();
    }
    return true;
}

void TOlapOptionsDescription::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    SchemeNeedActualization = tableSchema.GetOptions().GetSchemeNeedActualization();
    if (tableSchema.GetOptions().HasExternalGuaranteeExclusivePK()) {
        ExternalGuaranteeExclusivePK = tableSchema.GetOptions().GetExternalGuaranteeExclusivePK();
    }
    if (tableSchema.GetOptions().HasCompactionPlannerConstructor()) {
        AFL_VERIFY(CompactionPlannerConstructor.DeserializeFromProto(tableSchema.GetOptions().GetCompactionPlannerConstructor()));
    }
}

void TOlapOptionsDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    tableSchema.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
    if (ExternalGuaranteeExclusivePK) {
        tableSchema.MutableOptions()->SetExternalGuaranteeExclusivePK(ExternalGuaranteeExclusivePK);
    }
    if (CompactionPlannerConstructor.HasObject()) {
        CompactionPlannerConstructor.SerializeToProto(*tableSchema.MutableOptions()->MutableCompactionPlannerConstructor());
    }
}

bool TOlapOptionsDescription::Validate(const NKikimrSchemeOp::TColumnTableSchema& /*opSchema*/, IErrorCollector& /*errors*/) const {
    return true;
}

}
