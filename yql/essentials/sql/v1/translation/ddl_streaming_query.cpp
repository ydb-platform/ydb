#include "ddl_streaming_query.h"

#include "object_processing.h"

namespace NSQLTranslationV1 {

TNodePtr BuildCreateStreamingQuery(TPosition pos, const TDeferredAtom& objectId,
                                   bool existingOk, bool replaceIfExists,
                                   TObjectFeatureNodePtr features,
                                   const TObjectOperatorContext& context) {
    return BuildCreateObjectOperation(pos, objectId, "STREAMING_QUERY", existingOk, replaceIfExists,
                                      std::move(features), context);
}

TNodePtr BuildAlterStreamingQuery(TPosition pos, const TDeferredAtom& objectId,
                                  bool missingOk, TObjectFeatureNodePtr features,
                                  const TObjectOperatorContext& context) {
    return BuildAlterObjectOperation(pos, objectId, "STREAMING_QUERY", missingOk,
                                     std::move(features), {}, context);
}

TNodePtr BuildDropStreamingQuery(TPosition pos, const TDeferredAtom& objectId,
                                 bool missingOk, const TObjectOperatorContext& context) {
    return BuildDropObjectOperation(pos, objectId, "STREAMING_QUERY", missingOk, {}, context);
}

} // namespace NSQLTranslationV1
