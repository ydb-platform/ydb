#include "constructor.h"

#include <util/system/yassert.h>

namespace NKikimr::NOlap::NDataAccessorControl {

std::shared_ptr<IManagerConstructor> IManagerConstructor::BuildDefault() {
    // local_db is created via the factory: a direct reference would link in its manager
    std::shared_ptr<IManagerConstructor> result(TFactory::Construct("local_db"));
    Y_ABORT_UNLESS(result);
    TProto proto;
    proto.MutableLocalDB()->SetMemoryCacheSize(Max<ui32>());
    proto.MutableLocalDB()->SetFetchOnStart(true);
    Y_ABORT_UNLESS(result->DeserializeFromProto(proto));
    return result;
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
