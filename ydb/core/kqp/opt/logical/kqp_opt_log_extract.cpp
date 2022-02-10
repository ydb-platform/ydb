#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h> 
#include <ydb/library/yql/dq/opt/dq_opt_log.h> 
#include <ydb/library/yql/providers/common/provider/yql_table_lookup.h> 

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;


TExprBase KqpApplyExtractMembersToReadTable(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoExtractMembers>()) {
        return node;
    }

    auto extract = node.Cast<TCoExtractMembers>();
    auto input = extract.Input();

    if (!input.Maybe<TKqlReadTableBase>()) {
        return node;
    }

    auto read = extract.Input().Cast<TKqlReadTableBase>();

    if (auto maybeIndexRead = read.Maybe<TKqlReadTableIndex>()) {
        auto indexRead = maybeIndexRead.Cast();

        return Build<TKqlReadTableIndex>(ctx, extract.Pos())
            .Table(indexRead.Table())
            .Range(indexRead.Range())
            .Columns(extract.Members())
            .Index(indexRead.Index())
            .Settings(indexRead.Settings())
            .Done();
    }

    return Build<TKqlReadTableBase>(ctx, extract.Pos())
        .CallableName(read.CallableName())
        .Table(read.Table())
        .Range(read.Range())
        .Columns(extract.Members())
        .Settings(read.Settings())
        .Done();
}

TExprBase KqpApplyExtractMembersToReadTableRanges(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoExtractMembers>()) {
        return node;
    }

    auto extract = node.Cast<TCoExtractMembers>();
    auto input = extract.Input();

    if (!input.Maybe<TKqlReadTableRangesBase>()) {
        return node;
    }

    // TKqpReadOlapTableRangesBase is derived from TKqlReadTableRangesBase, but should be handled separately
    if (input.Maybe<TKqpReadOlapTableRangesBase>()) {
        return node;
    }

    auto read = extract.Input().Cast<TKqlReadTableRangesBase>();

    return Build<TKqlReadTableRangesBase>(ctx, extract.Pos())
        .CallableName(read.CallableName())
        .Table(read.Table())
        .Ranges(read.Ranges())
        .Columns(extract.Members())
        .Settings(read.Settings())
        .ExplainPrompt(read.ExplainPrompt())
        .Done();
}

TExprBase KqpApplyExtractMembersToReadOlapTable(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoExtractMembers>()) {
        return node;
    }

    auto extract = node.Cast<TCoExtractMembers>();
    auto input = extract.Input();

    if (!input.Maybe<TKqlReadTableRangesBase>()) {
        return node;
    }

    auto read = extract.Input().Cast<TKqpReadOlapTableRangesBase>();

    // When process is set it may use columns in read.Columns() but those columns may not be present
    // in the results. Thus do not apply extract members if process is not empty lambda
    if (read.Process().Body().Raw() != read.Process().Args().Arg(0).Raw()) {
        return node;
    }

    return Build<TKqpReadOlapTableRangesBase>(ctx, extract.Pos())
        .CallableName(read.CallableName())
        .Table(read.Table())
        .Ranges(read.Ranges())
        .Columns(extract.Members())
        .Settings(read.Settings())
        .ExplainPrompt(read.ExplainPrompt())
        .Process(read.Process())
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

