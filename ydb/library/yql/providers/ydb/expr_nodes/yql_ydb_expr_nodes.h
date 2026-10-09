#pragma once

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.gen.h>

namespace NYql {
    namespace NNodes {

#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.decl.inl.h>

        class TYdbDataSource: public NGenerated::TYdbDataSourceStub<TExprBase, TCallable, TCoAtom> {
        public:
            explicit TYdbDataSource(const TExprNode* node)
                : TYdbDataSourceStub(node)
            {
            }

            explicit TYdbDataSource(const TExprNode::TPtr& node)
                : TYdbDataSourceStub(node)
            {
            }

            static bool Match(const TExprNode* node) {
                if (!TYdbDataSourceStub::Match(node)) {
                    return false;
                }

                if (node->Child(0)->Content() != YdbProviderName) {
                    return false;
                }

                return true;
            }
        };

#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.defs.inl.h>

    } // namespace NNodes
} // namespace NYql
