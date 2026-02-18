#pragma once
#include "defs.h"
#include "mkql_node.h"
#include "mkql_node_visitor.h"

namespace NKikimr::NMiniKQL {

TString SerializeNode(TNode* node, std::vector<TNode*>& nodeStack) noexcept;
TString SerializeRuntimeNode(TRuntimeNode node, std::vector<TNode*>& nodeStack) noexcept;
TString SerializeRuntimeNode(TExploringNodeVisitor& explorer, TRuntimeNode node, std::vector<TNode*>& nodeStack) noexcept;

// Deprecated function. Should be removed after YDB sync.
TString SerializeNode(TNode* node, const TTypeEnvironment& env) noexcept;
TString SerializeRuntimeNode(TRuntimeNode node, const TTypeEnvironment& env) noexcept;
TString SerializeRuntimeNode(TExploringNodeVisitor& explorer, TRuntimeNode node, const TTypeEnvironment& env) noexcept;
// End of deprecated

TNode* DeserializeNode(const TStringBuf& buffer, const TTypeEnvironment& env);
TRuntimeNode DeserializeRuntimeNode(const TStringBuf& buffer, const TTypeEnvironment& env);

} // namespace NKikimr::NMiniKQL
