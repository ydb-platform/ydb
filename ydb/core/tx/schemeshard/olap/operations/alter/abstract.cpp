#include "abstract.h"
#include "standalone.h"
#include "in_store.h"

namespace NKikimr::NSchemeShard::NOlap::NAlter {

std::shared_ptr<NKikimr::NSchemeShard::NOlap::NAlter::ITableInfoConstructor> ITableInfoConstructor::Construct(const TTablesStorage::TTableReadGuard& tableInfo, const TInitializationContext& iContext, IErrorCollector& errors) {
    std::shared_ptr<ITableInfoConstructor> patcher;
    if (tableInfo->IsStandalone()) {
        patcher = std::make_shared<TStandaloneTableInfoConstructor>();
    } else {
        patcher = std::make_shared<TInStoreTableInfoConstructor>();
    }
    if (!patcher->Initialize(tableInfo, iContext, errors)) {
        return nullptr;
    }
    return patcher;
}

}