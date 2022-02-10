#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

struct TSerializedExprGraphComponents {
    enum : ui16 {
        Graph = 0x00,
        Positions = 0x01
    };
};

TString SerializeGraph(const TExprNode& node, TExprContext& ctx, ui16 components = TSerializedExprGraphComponents::Graph);
TExprNode::TPtr DeserializeGraph(TPositionHandle pos, TStringBuf buffer, TExprContext& ctx);
TExprNode::TPtr DeserializeGraph(TPosition pos, TStringBuf buffer, TExprContext& ctx);

} // namespace NYql

