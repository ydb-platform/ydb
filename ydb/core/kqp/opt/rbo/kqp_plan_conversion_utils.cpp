#include "kqp_plan_conversion_utils.h"

namespace NKikimr
{
namespace NKqp
{

using namespace NYql;
using namespace NNodes;

TOpRoot PlanConverter::ConvertRoot(TExprNode::TPtr node) {
    auto opRoot = TKqpOpRoot(node);
    auto rootInput = ExprNodeToOperator(opRoot.Input().Ptr());
    auto res = TOpRoot(rootInput);
    res.Node = node;
    return res;
}

std::shared_ptr<IOperator> PlanConverter::ExprNodeToOperator(TExprNode::TPtr node) {
    if (Converted.contains(node.Get())) {
        return Converted.at(node.Get());
    }

    std::shared_ptr<IOperator> result;

    if (NYql::NNodes::TKqpOpEmptySource::Match(node.Get()))
    {
        result = std::make_shared<TOpEmptySource>();
    }
    else if (NYql::NNodes::TKqpOpRead::Match(node.Get()))
    {
        result = std::make_shared<TOpRead>(node);
    }
    else if (NYql::NNodes::TKqpOpMap::Match(node.Get()))
    {
        result = ConvertTKqpOpMap(node);
    }
    else if (NYql::NNodes::TKqpOpFilter::Match(node.Get()))
    {
        result = ConvertTKqpOpFilter(node);
    }
    else if (NYql::NNodes::TKqpOpJoin::Match(node.Get()))
    {
        result = ConvertTKqpOpJoin(node);
    }
    else if (NYql::NNodes::TKqpOpLimit::Match(node.Get()))
    {
        result = ConvertTKqpOpLimit(node);
    }
    else if (NYql::NNodes::TKqpOpProject::Match(node.Get()))
    {
        result = ConvertTKqpOpProject(node);
    }
    else
    {
        YQL_ENSURE(false, "Unknown operator node");
    }
    Converted[node.Get()] = result;
    return result;
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpMap(TExprNode::TPtr node) {
    auto opMap = TKqpOpMap(node);
    auto input = ExprNodeToOperator(opMap.Input().Ptr());
    auto project = opMap.Project().IsValid();
    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> mapElements;

    for (auto mapElement : opMap.MapElements())
    {
        auto iu = TInfoUnit(mapElement.Variable().StringValue());
        if (mapElement.Maybe<TKqpOpMapElementRename>()) {
            auto element = mapElement.Cast<TKqpOpMapElementRename>();
            auto fromIU = TInfoUnit(element.From().StringValue());
            mapElements.push_back(std::make_pair(iu, fromIU));
        }
        else {
            auto element = mapElement.Cast<TKqpOpMapElementLambda>();
            if (element.Lambda().Body().Maybe<TCoMember>().Name().Maybe<TCoAtom>()) {
                auto member = element.Lambda().Body().Cast<TCoMember>();
                auto name = member.Name().Cast<TCoAtom>();
                auto fromIU = TInfoUnit(name.StringValue());
                mapElements.push_back(std::make_pair(iu, fromIU));
            }
            else {
                mapElements.push_back(std::make_pair(iu, element.Lambda().Ptr()));
            }
        }
    }
    return std::make_shared<TOpMap>(input, mapElements, project);
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpFilter(TExprNode::TPtr node) {
    auto opFilter = TKqpOpFilter(node);
    auto input = ExprNodeToOperator(opFilter.Input().Ptr());
    return std::make_shared<TOpFilter>(input, opFilter.Lambda().Ptr());
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpJoin(TExprNode::TPtr node) {
    auto opJoin = TKqpOpJoin(node);

    auto leftInput = ExprNodeToOperator(opJoin.LeftInput().Ptr());
    auto rightInput = ExprNodeToOperator(opJoin.RightInput().Ptr());

    auto joinKind = opJoin.JoinKind().StringValue();
    TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
    for (auto k : opJoin.JoinKeys())
    {
        TInfoUnit leftKey(k.LeftLabel().StringValue(), k.LeftColumn().StringValue());
        TInfoUnit rightKey(k.RightLabel().StringValue(), k.RightColumn().StringValue());

        joinKeys.push_back(std::make_pair(leftKey, rightKey));
    }
    return std::make_shared<TOpJoin>(leftInput, rightInput, joinKind, joinKeys);
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpLimit(TExprNode::TPtr node) {
    auto opLimit = TKqpOpLimit(node);
    auto input = ExprNodeToOperator(opLimit.Input().Ptr());
    return std::make_shared<TOpLimit>(input, opLimit.Count().Ptr());
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpProject(TExprNode::TPtr node) {
    auto opProject = TKqpOpProject(node);
    auto input = ExprNodeToOperator(opProject.Input().Ptr());

    TVector<TInfoUnit> projectList;

    for (auto p : opProject.ProjectList())
    {
        projectList.push_back(TInfoUnit(p.StringValue()));
    }
    return std::make_shared<TOpProject>(input, projectList);
}

void ExprNodeRebuilder::RebuildExprNodes(TOpRoot & root) {
    for (auto it : root) {
        YQL_CLOG(TRACE, CoreDq) << "Rebuilding: " << it.Current->ToString();
        RebuildExprNode(it.Current);
    }

    // clang-format off
    auto node = Build<TKqpOpRoot>(Ctx, Pos)
        .Input(root.GetInput()->Node)
    .Done().Ptr();
    // clang-format on
    root.Node = node;
}

void ExprNodeRebuilder::RebuildExprNode(std::shared_ptr<IOperator> op) {
    if (RebuiltNodes.contains(op)) {
        op->Node = RebuiltNodes.at(op);
        return;
    }

    TExprNode::TPtr newNode;

    switch(op->Kind) {
        case EOperator::EmptySource:
            newNode = RebuildEmptySource();
            break;
        case EOperator::Source:
            newNode = op->Node;
            break;
        case EOperator::Map:
            newNode = RebuildMap(op);
            break;
        case EOperator::Project:
            newNode = RebuildProject(op);
            break;
        case EOperator::Filter:
            newNode = RebuildFilter(op);
            break;
        case EOperator::Join:
            newNode = RebuildJoin(op);
            break;
        case EOperator::Limit:
            newNode = RebuildLimit(op);
            break;
        default:
            YQL_ENSURE(false, "Unknown operator");
    }

    op->Node = newNode;
}

TExprNode::TPtr ExprNodeRebuilder::RebuildEmptySource() {
    // clang-format off
    return Build<TKqpOpEmptySource>(Ctx, Pos)
            .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr ExprNodeRebuilder::RebuildMap(std::shared_ptr<IOperator> op) {
    auto map = CastOperator<TOpMap>(op);

    TVector<TExprNode::TPtr> newMapElements;
    TExprNode::TPtr newInput = map->GetInput()->Node;

    for (auto & [iu, body] : map->MapElements)
    {
        if (std::holds_alternative<TInfoUnit>(body))
        {
            // clang-format off
            newMapElements.push_back(Build<TKqpOpMapElementRename>(Ctx, Pos)
                .Input(newInput)
                .Variable().Build(iu.GetFullName())
                .From().Build(std::get<TInfoUnit>(body).GetFullName())
            .Done().Ptr());
            // clang-format on
        }
        else
        {
            // clang-format off
            newMapElements.push_back(Build<TKqpOpMapElementLambda>(Ctx, Pos)
                .Input(newInput)
                .Variable().Build(iu.GetFullName())
                .Lambda(std::get<TExprNode::TPtr>(body))
            .Done().Ptr());
            // clang-format on
        }
    }

    TExprNode::TPtr result;
    if (!map->Project)
    {
        // clang-format off
        result = Build<TKqpOpMap>(Ctx, Pos)
            .Input(newInput)
            .MapElements()
                .Add(newMapElements)
            .Build()
        .Done().Ptr();
        // clang-format on
    }
    else
    {
        // clang-format off
        result = Build<TKqpOpMap>(Ctx, Pos)
            .Input(newInput)
            .MapElements()
                .Add(newMapElements)
            .Build()
            .Project().Value("true").Build()
        .Done().Ptr();
        // clang-format on
    }
    return result;
}

TExprNode::TPtr ExprNodeRebuilder::RebuildProject(std::shared_ptr<IOperator> op) {
    auto project = CastOperator<TOpProject>(op);
    TVector<TExprNode::TPtr> projectList;

    for (auto iu : project->ProjectList) {
        projectList.push_back(Build<TCoAtom>(Ctx, Pos).Value(iu.GetFullName()).Done().Ptr());
    }

    // clang-format off
    return Build<TKqpOpProject>(Ctx, Pos)
                .Input(project->GetInput()->Node)
                .ProjectList().Add(projectList).Build()
            .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr ExprNodeRebuilder::RebuildFilter(std::shared_ptr<IOperator> op) {
    auto filter = CastOperator<TOpFilter>(op);

    // clang-format off
    return Build<TKqpOpFilter>(Ctx, Pos)
        .Input(filter->GetInput()->Node)
        .Lambda(filter->FilterLambda)
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr ExprNodeRebuilder::RebuildJoin(std::shared_ptr<IOperator> op) {
    auto join = CastOperator<TOpJoin>(op);

    TVector<TDqJoinKeyTuple> keys;
    for (auto k : join->JoinKeys)
    {
        // clang-format off
        keys.push_back(Build<TDqJoinKeyTuple>(Ctx, Pos)
            .LeftLabel()
                .Value(k.first.Alias)
            .Build()
            .LeftColumn()
                .Value(k.first.ColumnName)
            .Build()
            .RightLabel()
                .Value(k.second.Alias)
            .Build()
            .RightColumn()
                .Value(k.second.ColumnName)
            .Build()
        .Done());
        // clang-format on
    }

    auto joinKeys = Build<TDqJoinKeyTupleList>(Ctx, Pos).Add(keys).Done();

    // clang-format off
    return Build<TKqpOpJoin>(Ctx, Pos)
        .LeftInput(join->GetLeftInput()->Node)
        .RightInput(join->GetRightInput()->Node)
        .JoinKind().Value(join->JoinKind).Build()
        .JoinKeys(joinKeys)
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr ExprNodeRebuilder::RebuildLimit(std::shared_ptr<IOperator> op) {
    auto limit = CastOperator<TOpLimit>(op);

    // clang-format off
    return Build<TKqpOpLimit>(Ctx, Pos)
        .Input(limit->GetInput()->Node)
        .Count(limit->LimitCond)
    .Done().Ptr();
    // clang-format on
}

}
}