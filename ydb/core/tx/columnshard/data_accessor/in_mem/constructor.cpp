#include "constructor.h"
#include "manager.h"

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {

TConclusion<std::shared_ptr<IMetadataMemoryManager>> TManagerConstructor::DoBuild(const TManagerConstructionContext& /*context*/) const {
    return std::make_shared<TManager>();
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NInMem
