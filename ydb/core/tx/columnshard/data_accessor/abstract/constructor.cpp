#include "constructor.h"

namespace NKikimr::NOlap::NDataAccessorControl {

std::shared_ptr<IManagerConstructor> IManagerConstructor::BuildDefault() {
    std::shared_ptr<IManagerConstructor> result(TFactory::Construct("in_mem"));
    AFL_VERIFY(!!result);
    return result;
}

}