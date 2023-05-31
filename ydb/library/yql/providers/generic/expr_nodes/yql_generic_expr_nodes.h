#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.gen.h>

namespace NYql {
    namespace NNodes {

#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.decl.inl.h>

        class TGenDataSource: public NGenerated::TGenDataSourceStub<TExprBase, TCallable, TCoAtom> {
        public:
            explicit TGenDataSource(const TExprNode* node)
                : TGenDataSourceStub(node)
            {
            }

            explicit TGenDataSource(const TExprNode::TPtr& node)
                : TGenDataSourceStub(node)
            {
            }

            static bool Match(const TExprNode* node) {
                if (!TGenDataSourceStub::Match(node)) {
                    return false;
                }

                if (node->Child(0)->Content() != GenericProviderName) {
                    return false;
                }

                return true;
            }
        };

#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.defs.inl.h>

    }
}
