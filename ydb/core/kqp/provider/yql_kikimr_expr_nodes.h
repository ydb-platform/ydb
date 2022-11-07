#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.gen.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {
namespace NNodes {

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.decl.inl.h>

class TKiDataSource : public NGenerated::TKiDataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TKiDataSource(const TExprNode* node)
        : TKiDataSourceStub(node) {}

    explicit TKiDataSource(const TExprNode::TPtr& node)
        : TKiDataSourceStub(node) {}

    static bool Match(const TExprNode* node) {
        if (!TKiDataSourceStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != KikimrProviderName) {
            return false;
        }

        return true;
    }
};

class TKiDataSink : public NGenerated::TKiDataSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TKiDataSink(const TExprNode* node)
        : TKiDataSinkStub(node) {}

    explicit TKiDataSink(const TExprNode::TPtr& node)
        : TKiDataSinkStub(node) {}

    static bool Match(const TExprNode* node) {
        if (!TKiDataSinkStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != KikimrProviderName) {
            return false;
        }

        return true;
    }
};

class TKiReadTable : public NGenerated::TKiReadTableStub<TExprBase, TKiReadBase, TCoNameValueTupleList> {
public:
    explicit TKiReadTable(const TExprNode* node)
        : TKiReadTableStub(node) {}

    explicit TKiReadTable(const TExprNode::TPtr& node)
        : TKiReadTableStub(node) {}

    TString GetTable(TExprContext& ctx) const;
    TCoAtomList GetSelectColumns(TExprContext& ctx, const TKikimrTablesData& tablesData,
        bool withVirtualColumns = false) const;
    TCoAtomList GetSelectColumns(TExprContext& ctx, const TKikimrTableDescription& tableData,
        bool withVirtualColumns = false) const;
};

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
