#include "kqp_ne_helper.h" 
 
#include <ydb/core/kqp/common/kqp_yql.h>
 
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>
 
namespace NKikimr { 
namespace NKqp { 
 
using namespace NYql; 
using namespace NYql::NNodes; 
 
bool CanExecuteWithNewEngine(const TKiProgram& program, TExprContext& ctx) { 
    bool allow = true; 
    TStringBuf forbiddenCallable; 
 
    VisitExpr( 
        program.Ptr(), 
        [&allow](const TExprNode::TPtr&) { 
            return allow; 
        }, 
        [&allow, &forbiddenCallable](const TExprNode::TPtr& node) { 
 
#define FORBID_CALLABLE(x) if (x::Match(node.Get())) { allow = false; forbiddenCallable = x::CallableName(); return true; } 
 
            FORBID_CALLABLE(TKiUpdateRow); 
            FORBID_CALLABLE(TKiEraseRow); 
            FORBID_CALLABLE(TKiDeleteTable); 
            FORBID_CALLABLE(TKiEffects); 
            FORBID_CALLABLE(TKiSelectIndexRange); 
            FORBID_CALLABLE(TCoEquiJoin); 
            FORBID_CALLABLE(TCoJoin); 
            FORBID_CALLABLE(TCoMapJoinCore); 
            FORBID_CALLABLE(TCoJoinDict); 
            FORBID_CALLABLE(TCoSqlIn); 
 
#undef FORBID_CALLABLE 
 
            if (auto selectRange = TMaybeNode<TKiSelectRange>(node)) { 
                if (selectRange.Cast().Table().Path().Value().ends_with("/indexImplTable")) { 
                    forbiddenCallable = "KiSelectIndexRange"; 
                    allow = false; 
                    return true; 
                } 
            } 
 
            return true; 
        }); 
 
    if (allow) { 
        YQL_CLOG(NOTICE, ProviderKqp) << "Query " << KqpExprToPrettyString(program.Ref(), ctx) 
            << " can be executed with NewEngine"; 
    } else { 
        YQL_CLOG(NOTICE, ProviderKqp) << "Query " << KqpExprToPrettyString(program.Ref(), ctx) 
            << " cannot be executed with NewEngine (blocked by '" << forbiddenCallable << "')"; 
    } 
 
    return allow; 
} 
 
} // namespace NKqp 
} // namespace NKikimr 
