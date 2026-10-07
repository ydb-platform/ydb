#pragma once

#include <ydb/library/yql/providers/ydb_external/common/provider_names.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/ydb_external/expr_nodes/yql_ydb_external_expr_nodes.gen.h>

namespace NYql {
    namespace NNodes {

#include <ydb/library/yql/providers/ydb_external/expr_nodes/yql_ydb_external_expr_nodes.decl.inl.h>

        class TYdbExternalDataSource: public NGenerated::TYdbExternalDataSourceStub<TExprBase, TCallable, TCoAtom> {
        public:
            explicit TYdbExternalDataSource(const TExprNode* node)
                : TYdbExternalDataSourceStub(node)
            {
            }

            explicit TYdbExternalDataSource(const TExprNode::TPtr& node)
                : TYdbExternalDataSourceStub(node)
            {
            }

            static bool Match(const TExprNode* node) {
                if (!TYdbExternalDataSourceStub::Match(node)) {
                    return false;
                }

                if (node->Child(0)->Content() != YdbExternalProviderName) {
                    return false;
                }

                return true;
            }
        };

#include <ydb/library/yql/providers/ydb_external/expr_nodes/yql_ydb_external_expr_nodes.defs.inl.h>

    } // namespace NNodes
} // namespace NYql
