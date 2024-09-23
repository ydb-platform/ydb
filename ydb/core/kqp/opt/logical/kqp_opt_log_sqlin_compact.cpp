#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/common_opt/yql_co_sqlin.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;

TExprBase KqpRewriteSqlInCompactToJoin(const TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }

    const auto flatMap = node.Cast<TCoFlatMap>();
    const auto lambdaBody = flatMap.Lambda().Body();

    if (!lambdaBody.Maybe<TCoOptionalIf>()) {
        return node;
    }

    auto optionalIf = lambdaBody.Cast<TCoOptionalIf>();

    auto sqlInPtr = FindNode(
        lambdaBody.Ptr(),
        [](const TExprNode::TPtr &x) {
            return TCoSqlIn::Match(x.Get());
        }
    );

    if (!sqlInPtr) {
        return node;
    }

    auto sqlIn = TCoSqlIn(sqlInPtr);

    if (!HasSetting(sqlIn.Options().Ref(), "isCompact")) {
        return node;
    }

    // Check both inputs (Flatmap and SqlIn) are union all (rewrite to shuffle?)
    auto leftInput = flatMap.Input();
    auto rightInput = sqlIn.Collection();

    if (!leftInput.Maybe<TDqCnUnionAll>()) {
        return node;
    }

    if (!rightInput.Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto rightUnion = rightInput.Cast<TDqCnUnionAll>();
    auto leftUnion = leftInput.Cast<TDqCnUnionAll>();

    auto extractColumns = [&ctx, &node](const TDqOutput& output) {
        TVector<TCoAtom> renames;

        TExprBase connection = output.Stage().Program().Body();

        if (connection.Maybe<TDqReplicate>()) {
            auto replicate = connection.Cast<TDqReplicate>();
            auto index = FromString<uint32_t>(output.Index().Value());
            // Index 0 is replicate input, others - are lambdas.
            connection = replicate.Args().Get(index + 1);
        }

        const auto& itemType = GetSeqItemType(*connection.Ptr()->GetTypeAnn());
        YQL_ENSURE(itemType.GetKind() == ETypeAnnotationKind::Struct,
            "Expected Struct, got " << itemType.GetKind()
        );
        renames.reserve(itemType.Cast<TStructExprType>()->GetSize());

        for (const auto& column : itemType.Cast<TStructExprType>()->GetItems()) {
            renames.emplace_back(
                Build<TCoAtom>(ctx, node.Pos())
                    .Value(column->GetName())
                    .Done()
            );
        }
        return renames;
    };

    auto rightColumns = extractColumns(rightUnion.Output());
    auto leftColumns = extractColumns(leftUnion.Output());

    if (rightColumns.empty() || leftColumns.empty()) {
        return node;
    }

    // Extract right column for join
    if (rightColumns.size() != 1) {
        return node;
    }

    auto rightColumn = rightColumns[0];

    // Extract left column to join
    if (!sqlIn.Lookup().Maybe<TCoMember>()) {
        return node;
    }

    auto leftMember = sqlIn.Lookup().Cast<TCoMember>();
    auto leftColumn = leftMember.Name();

    TString rightLabelStr = "rightLabel";
    TString leftLabelStr = "leftLabel";

    // Join (left semi) both incoming tables by SqlIn parameter
    auto rightLabel = Build<TCoAtom>(ctx, node.Pos())
        .Value(rightLabelStr)
        .Done().Ptr();
    auto leftLabel = Build<TCoAtom>(ctx, node.Pos())
        .Value(leftLabelStr)
        .Done().Ptr();

    TVector<TCoAtom> leftJoinKeyNames;
    TVector<TCoAtom> rightJoinKeyNames;

    leftJoinKeyNames.emplace_back(leftColumn);
    rightJoinKeyNames.emplace_back(rightColumn);

    auto joinKeys = Build<TDqJoinKeyTupleList>(ctx, node.Pos())
        .Add<TDqJoinKeyTuple>()
            .LeftLabel(leftLabel)
            .LeftColumn(leftColumn)
            .RightLabel(rightLabel)
            .RightColumn(rightColumn)
            .Build()
        .Done();

    TCoArgument leftArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("leftArg")
        .Done();

    TCoArgument rightArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("rightArg")
        .Done();

    TVector<TCoArgument> args = {leftArg, rightArg};
    TVector<TExprBase> inputs = {leftInput, rightInput};

    auto join = Build<TDqPhyMapJoin>(ctx, node.Pos())
        .LeftInput(leftArg)
        .LeftLabel(leftLabel)
        .RightInput(rightArg)
        .RightLabel(rightLabel)
        .JoinType<TCoAtom>()
            .Value("LeftSemi")
            .Build()
        .JoinKeys(joinKeys)
        .LeftJoinKeyNames()
            .Add(leftJoinKeyNames)
            .Build()
        .RightJoinKeyNames()
            .Add(rightJoinKeyNames)
            .Build()
        .Done();

    // Convert column names back, i.e. leftLabel.ColumnName -> ColumnName
    TVector<TExprBase> requiredMembers;
    TCoArgument convertArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("convertArg")
        .Done();

    // If there is AsStruct in OptionalIf - get column names from it, otherwise get them from incoming stream
    if (optionalIf.Value().Maybe<TCoAsStruct>()) {
        for (const auto& item: optionalIf.Value().Cast<TCoAsStruct>().Ptr()->Children()) {
            auto tuple = TCoNameValueTuple(item);

            if (!tuple.Value().Maybe<TCoMember>()) {
                return node;
            }

            auto columnName = TString(tuple.Value().Cast<TCoMember>().Name());
            auto newTuple = Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name(tuple.Name())
                .Value<TCoMember>()
                    .Struct(convertArg)
                    .Name<TCoAtom>()
                        .Value(Join(".", leftLabelStr, columnName))
                        .Build()
                    .Build()
                .Done();

            requiredMembers.emplace_back(std::move(newTuple));
        }
    } else {
        for (const auto& columnName: leftColumns) {
            auto newTuple = Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name(columnName)
                .Value<TCoMember>()
                    .Struct(convertArg)
                    .Name<TCoAtom>()
                        .Value(Join(".", leftLabelStr, columnName.Value()))
                        .Build()
                    .Build()
                .Done();

            requiredMembers.emplace_back(std::move(newTuple));
        }
    }

    auto convert = Build<TCoFlatMap>(ctx, node.Pos())
        .Input<TCoToStream>()
            .Input(join)
            .Build()
        .Lambda()
            .Args({convertArg})
            .Body<TCoJust>()
                .Input<TCoAsStruct>()
                    .Add(requiredMembers)
                    .Build()
                .Build()
            .Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add(inputs)
                    .Build()
                .Program()
                    .Args(args)
                    .Body(convert)
                    .Build()
                .Settings()
                    .Build()
                .Build()
            .Index().Build("0")
            .Build()
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

