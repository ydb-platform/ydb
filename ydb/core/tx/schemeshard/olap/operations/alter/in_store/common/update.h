#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/common/update.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreTableUpdate: public TColumnTableUpdate {
private:
    using TBase = TColumnTableUpdate;

    virtual TConclusionStatus DoStartImpl(const TUpdateStartContext& context) override final;
    virtual TConclusionStatus DoStartInStoreImpl(const TUpdateStartContext& /*context*/) {
        return TConclusionStatus::Success();
    }
public:
};

}