#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

TNodePtr BuildCreateTransfer(TPosition pos, const TString& id, const TString& source, const TString& target,
                             const TString& transformLambda,
                             std::map<TString, TNodePtr>&& settings,
                             const TObjectOperatorContext& context);
TNodePtr BuildAlterTransfer(TPosition pos, const TString& id, std::optional<TString>&& transformLambda,
                            std::map<TString, TNodePtr>&& settings,
                            const TObjectOperatorContext& context);
TNodePtr BuildDropTransfer(TPosition pos, const TString& id, bool cascade, const TObjectOperatorContext& context);

} // namespace NSQLTranslationV1
