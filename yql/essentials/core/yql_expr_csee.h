#pragma once

#include "yql_graph_transformer.h"
#include "yql_type_annotation.h"

#include <util/digest/murmur.h>

namespace NYql {

IGraphTransformer::TStatus EliminateCommonSubExpressions(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    bool forSubGraph, const TColumnOrderStorage& coStore);
IGraphTransformer::TStatus UpdateCompletness(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);

// Calculate order between two given nodes. Must be used only after CSEE pass or UpdateCompletness.
// 0 may not mean equality of nodes because we cannot distinguish order of external arguments in some cases.
int CompareNodes(const TExprNode& left, const TExprNode& right);

inline ui64 CseeHash(const void* data, size_t size, ui64 initHash) {
    return MurmurHash<ui64>(data, size, initHash);
}

template<typename T, std::enable_if_t<std::is_integral<T>::value>* = nullptr>
inline ui64 CseeHash(T value, ui64 initHash) {
    // workaround Coverity warning for Murmur when sizeof(T) < 8
    ui64 val = static_cast<ui64>(value);
    return MurmurHash<ui64>(&val, sizeof(val), initHash);
}

}
