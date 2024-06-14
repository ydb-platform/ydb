#pragma once
#include "update.h"

namespace NKikimr::NSchemeShard {

class TOlapSchema;

class TOlapOptionsDescription {
private:
    YDB_READONLY(bool, SchemeNeedActualization, false);
    YDB_READONLY(bool, ExternalGuaranteeExclusivePK, false);
    YDB_READONLY_DEF(NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer, CompactionPlannerConstructor);
public:
    bool ApplyUpdate(const TOlapOptionsUpdate& schemaUpdate, IErrorCollector& errors);

    void Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
    void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
    bool Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
};
}
