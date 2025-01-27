#include "constructor.h"

#include <ydb/core/tx/columnshard/data_accessor/local_db/constructor.h>

namespace NKikimr::NOlap::NDataAccessorControl {

std::shared_ptr<IManagerConstructor> IManagerConstructor::BuildDefault() {
    return NLocalDB::TManagerConstructor::BuildDefault();
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
