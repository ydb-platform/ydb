#pragma once
#include "update.h"

#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/evolution.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreCommonEvolution: public ISSEntityEvolution {
private:
    using TBase = ISSEntityEvolution;

public:
    using TBase::TBase;
};

}