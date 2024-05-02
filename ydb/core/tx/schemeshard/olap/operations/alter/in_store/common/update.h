#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/object.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreCommonUpdate: public ISSEntityUpdate {
private:
    using TBase = ISSEntityUpdate;
protected:
    std::shared_ptr<TInStoreTable> OriginalInStoreTable;
public:
    TInStoreCommonUpdate(const std::shared_ptr<ISSEntity>& originalEntity, const NKikimrSchemeOp::TModifyScheme& request)
        : TBase(originalEntity, request) {
        OriginalInStoreTable = dynamic_pointer_cast<TInStoreTable>(originalEntity);
        AFL_VERIFY(OriginalInStoreTable);
    }
};

}