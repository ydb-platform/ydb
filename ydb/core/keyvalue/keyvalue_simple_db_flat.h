#pragma once
#include "defs.h"
#include "keyvalue_simple_db.h"
#include <library/cpp/actors/core/actor.h>

namespace NKikimr {

namespace NTable {
    class TDatabase;
}

namespace NKeyValue {

class TSimpleDbFlat : public ISimpleDb {
protected:
    NTable::TDatabase &Db;
public:
    TSimpleDbFlat(NTable::TDatabase &db);
    void Erase(const TString &key, const TActorContext &ctx) override;
    void Update(const TString &key, const TString &value, const TActorContext &ctx) override;
};

} // NKeyValue
} // NKikimr
