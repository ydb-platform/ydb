#include "ddl_resource_pool.h"

#include "object_processing.h"

namespace NSQLTranslationV1 {

TNodePtr BuildCreateResourcePool(
    TPosition pos,
    const TDeferredAtom& id,
    TObjectFeatureNodePtr features,
    const TObjectOperatorContext& context)
{
    return BuildCreateObjectOperation(pos, id, "RESOURCE_POOL", /*existingOk=*/false, /*replaceIfExists=*/false, features, context);
}

TNodePtr BuildAlterResourcePool(
    TPosition pos,
    const TDeferredAtom& id,
    TObjectFeatureNodePtr features,
    std::set<TString>&& featuresToReset,
    const TObjectOperatorContext& context)
{
    return BuildAlterObjectOperation(pos, id, "RESOURCE_POOL", /*missingOk=*/false, features, std::move(featuresToReset), context);
}

TNodePtr BuildDropResourcePool(
    TPosition pos,
    const TDeferredAtom& id,
    const TObjectOperatorContext& context)
{
    return BuildDropObjectOperation(pos, id, "RESOURCE_POOL", /*missingOk=*/false, {}, context);
}

TNodePtr BuildCreateResourcePoolClassifier(
    TPosition pos,
    const TDeferredAtom& id,
    TObjectFeatureNodePtr features,
    const TObjectOperatorContext& context)
{
    return BuildCreateObjectOperation(pos, id, "RESOURCE_POOL_CLASSIFIER", /*existingOk=*/false, /*replaceIfExists=*/false, features, context);
}

TNodePtr BuildAlterResourcePoolClassifier(
    TPosition pos,
    const TDeferredAtom& id,
    TObjectFeatureNodePtr features,
    std::set<TString>&& featuresToReset,
    const TObjectOperatorContext& context)
{
    return BuildAlterObjectOperation(pos, id, "RESOURCE_POOL_CLASSIFIER", /*missingOk=*/false, features, std::move(featuresToReset), context);
}

TNodePtr BuildDropResourcePoolClassifier(
    TPosition pos,
    const TDeferredAtom& id,
    const TObjectOperatorContext& context)
{
    return BuildDropObjectOperation(pos, id, "RESOURCE_POOL_CLASSIFIER", /*missingOk=*/false, {}, context);
}

} // namespace NSQLTranslationV1
