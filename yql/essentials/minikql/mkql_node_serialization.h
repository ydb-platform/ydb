#pragma once
#include "defs.h"
#include "mkql_node.h"
#include "mkql_node_visitor.h"

namespace NKikimr {
namespace NMiniKQL {

TString SerializeNode(TNode* node, const TTypeEnvironment& env) noexcept;
TString SerializeRuntimeNode(TRuntimeNode node, const TTypeEnvironment& env) noexcept;
TString SerializeRuntimeNode(TExploringNodeVisitor& explorer, TRuntimeNode node, const TTypeEnvironment& env) noexcept;
TNode* DeserializeNode(const TStringBuf& buffer, const TTypeEnvironment& env);
TRuntimeNode DeserializeRuntimeNode(const TStringBuf& buffer, const TTypeEnvironment& env);

}
}
