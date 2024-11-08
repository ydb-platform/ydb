#include "constructor.h"
#include "manager.h"

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

TConclusion<std::shared_ptr<IMetadataMemoryManager>> TManagerConstructor::DoBuild(const TManagerConstructionContext& context) const {
    return std::make_shared<TManager>(context.GetTabletActorId());
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
