#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/ydb_remote/expr_nodes/yql_ydb_remote_expr_nodes.gen.h>

namespace NYql {
    namespace NNodes {

#include <ydb/library/yql/providers/ydb_remote/expr_nodes/yql_ydb_remote_expr_nodes.decl.inl.h>

        class TYdbRemoteDataSource: public NGenerated::TYdbRemoteDataSourceStub<TExprBase, TCallable, TCoAtom> {
        public:
            explicit TYdbRemoteDataSource(const TExprNode* node)
                : TYdbRemoteDataSourceStub(node)
            {
            }

            explicit TYdbRemoteDataSource(const TExprNode::TPtr& node)
                : TYdbRemoteDataSourceStub(node)
            {
            }

            static bool Match(const TExprNode* node) {
                if (!TYdbRemoteDataSourceStub::Match(node)) {
                    return false;
                }

                if (node->Child(0)->Content() != YdbRemoteProviderName) {
                    return false;
                }

                return true;
            }
        };

#include <ydb/library/yql/providers/ydb_remote/expr_nodes/yql_ydb_remote_expr_nodes.defs.inl.h>

    } // namespace NNodes
} // namespace NYql
