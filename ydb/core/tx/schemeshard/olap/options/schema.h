#pragma once
#include "update.h"

namespace NKikimr::NSchemeShard {

class TOlapSchema;

class TOlapOptionsDescription {
private:
    YDB_READONLY(bool, SchemeNeedActualization, false);
    YDB_READONLY_DEF(std::optional<TString>, ScanReaderPolicyName);
    YDB_READONLY_DEF(NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer, CompactionPlannerConstructor);
    YDB_READONLY_DEF(NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer, MetadataManagerConstructor);
public:
    bool ApplyUpdate(const TOlapOptionsUpdate& schemaUpdate, IErrorCollector& errors);

    void Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
    void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
    bool ValidateForStore(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
};
}
