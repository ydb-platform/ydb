#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

struct TStreamingQuerySettings {
    inline static constexpr char RESERVED_FEATURE_PREFIX[] = "__";
    inline static constexpr char QUERY_TEXT_FEATURE[] = "__query_text";
    inline static constexpr char QUERY_AST_FEATURE[] = "__query_ast";

    TObjectFeatureNodePtr Features;
};

TNodePtr BuildCreateStreamingQuery(TPosition pos, const TDeferredAtom& objectId,
                                   bool existingOk, bool replaceIfExists,
                                   TObjectFeatureNodePtr features,
                                   const TObjectOperatorContext& context);
TNodePtr BuildAlterStreamingQuery(TPosition pos, const TDeferredAtom& objectId,
                                  bool missingOk, TObjectFeatureNodePtr features,
                                  const TObjectOperatorContext& context);
TNodePtr BuildDropStreamingQuery(TPosition pos, const TDeferredAtom& objectId,
                                 bool missingOk, const TObjectOperatorContext& context);

} // namespace NSQLTranslationV1
