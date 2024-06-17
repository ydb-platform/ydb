#pragma once
#include "update.h"

namespace NKikimr::NSchemeShard {

class TOlapSchema;

class TOlapOptionsDescription {
private:
    YDB_READONLY(bool, SchemeNeedActualization, false);
public:
    bool ApplyUpdate(const TOlapOptionsUpdate& schemaUpdate, IErrorCollector& errors);

    void Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
    void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
    bool Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
};
}
