#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

TNodePtr BuildCreateResourcePool(
    TPosition pos,
    const TDeferredAtom& id,
    TObjectFeatureNodePtr features,
    const TObjectOperatorContext& context);
TNodePtr BuildAlterResourcePool(
    TPosition pos,
    const TDeferredAtom& id,
    TObjectFeatureNodePtr features,
    std::set<TString>&& featuresToReset,
    const TObjectOperatorContext& context);
TNodePtr BuildDropResourcePool(
    TPosition pos,
    const TDeferredAtom& id,
    const TObjectOperatorContext& context);

TNodePtr BuildCreateResourcePoolClassifier(
    TPosition pos,
    const TDeferredAtom& id,
    TObjectFeatureNodePtr features,
    const TObjectOperatorContext& context);
TNodePtr BuildAlterResourcePoolClassifier(
    TPosition pos,
    const TDeferredAtom& id,
    TObjectFeatureNodePtr features,
    std::set<TString>&& featuresToReset,
    const TObjectOperatorContext& context);
TNodePtr BuildDropResourcePoolClassifier(
    TPosition pos,
    const TDeferredAtom& id,
    const TObjectOperatorContext& context);

} // namespace NSQLTranslationV1
