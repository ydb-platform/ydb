#pragma once

#include <ydb/library/yql/providers/ydb/query/common/provider_names.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/ydb/query/expr_nodes/yql_ydb_expr_nodes.gen.h>

namespace NYql {
    namespace NNodes {

#include <ydb/library/yql/providers/ydb/query/expr_nodes/yql_ydb_expr_nodes.decl.inl.h>

        class TYdbQueryDataSource: public NGenerated::TYdbQueryDataSourceStub<TExprBase, TCallable, TCoAtom> {
        public:
            explicit TYdbQueryDataSource(const TExprNode* node)
                : TYdbQueryDataSourceStub(node)
            {
            }

            explicit TYdbQueryDataSource(const TExprNode::TPtr& node)
                : TYdbQueryDataSourceStub(node)
            {
            }

            static bool Match(const TExprNode* node) {
                if (!TYdbQueryDataSourceStub::Match(node)) {
                    return false;
                }

                if (node->Child(0)->Content() != YdbQueryProviderName) {
                    return false;
                }

                return true;
            }
        };

#include <ydb/library/yql/providers/ydb/query/expr_nodes/yql_ydb_expr_nodes.defs.inl.h>

    } // namespace NNodes
} // namespace NYql
