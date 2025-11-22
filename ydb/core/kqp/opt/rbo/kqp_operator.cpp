#include "kqp_operator.h"
#include <yql/essentials/core/yql_expr_optimize.h>

namespace {
using namespace NKikimr;
using namespace NKqp;
using namespace NYql;
using namespace NNodes;

void DFS(int vertex, TVector<int> &sortedStages, THashSet<int> &visited, const THashMap<int, TVector<int>> &stageInputs) {
    visited.emplace(vertex);

    for (auto u : stageInputs.at(vertex)) {
        if (!visited.contains(u)) {
            DFS(u, sortedStages, visited, stageInputs);
        }
    }

    sortedStages.push_back(vertex);
}

TExprNode::TPtr AddRenames(TExprNode::TPtr input, TExprContext &ctx, TVector<TInfoUnit> renames) {
    TVector<TExprBase> items;
    auto arg = Build<TCoArgument>(ctx, input->Pos()).Name("arg").Done().Ptr();

    for (auto iu : renames) {
        // clang-format off
        auto tuple = Build<TCoNameValueTuple>(ctx, input->Pos())
            .Name().Build(iu.GetFullName())
            .Value<TCoMember>()
                .Struct(arg)
                .Name().Build(iu.ColumnName)
            .Build()
        .Done();
        // clang-format on
        items.push_back(tuple);
    }

    /*
    return Build<TCoFlatMap>(ctx, input->Pos())
        .Input(input)
        .Lambda<TCoLambda>()
            .Args({arg})
            .Body<TCoJust>()
                .Input<TCoAsStruct>()
                    .Add(items)
                .Build()
            .Build()
        .Build()
        .Done().Ptr();
    */

    // clang-format off
    return Build<TCoMap>(ctx, input->Pos())
        .Input(input)
        .Lambda<TCoLambda>()
            .Args({arg})
            .Body<TCoAsStruct>()
                .Add(items)
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr BuildSourceStage(TExprNode::TPtr dqsource, TExprContext &ctx) {
    auto arg = Build<TCoArgument>(ctx, dqsource->Pos()).Name("arg").Done().Ptr();
    // clang-format off
    return Build<TDqPhyStage>(ctx, dqsource->Pos())
        .Inputs()
            .Add({dqsource})
        .Build()
        .Program()
            .Args({arg})
            .Body(arg)
        .Build()
        .Settings().Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr RenameMembers(TExprNode::TPtr input, const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap,
                              TExprContext &ctx) {
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        auto memberName = member.Name();
        if (renameMap.contains(TInfoUnit(memberName.StringValue()))) {
            auto renamed = renameMap.at(TInfoUnit(memberName.StringValue()));
            // clang-format off
             memberName = Build<TCoAtom>(ctx, input->Pos()).Value(renamed.GetFullName()).Done();
            // clang-format on
        }
        // clang-format off
        return Build<TCoMember>(ctx, input->Pos())
            .Struct(member.Struct())
            .Name(memberName)
        .Done().Ptr();
        // clang-format on
    } else if (input->IsCallable()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(RenameMembers(c, renameMap, ctx));
        }
        // clang-format off
            return ctx.Builder(input->Pos())
                .Callable(input->Content())
                    .Add(std::move(newChildren))
                    .Seal()
                .Build();
        // clang-format on
    } else if (input->IsList()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(RenameMembers(c, renameMap, ctx));
        }
        // clang-format off
            return ctx.Builder(input->Pos())
                .List()
                    .Add(std::move(newChildren))
                    .Seal()
                .Build();
        // clang-format on
    } else {
        return input;
    }
}

} // namespace

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

TString PrintRBOExpression(TExprNode::TPtr expr, TExprContext & ctx) {
    try {
        TConvertToAstSettings settings;
        settings.AllowFreeArgs = true;
 
        auto ast = ConvertToAst(*expr, ctx, settings);
        TStringStream exprStream;
        YQL_ENSURE(ast.Root);
        ast.Root->PrintTo(exprStream);

        TString exprText = exprStream.Str();

        return exprText;
    } catch (const std::exception& e) {
        return TStringBuilder() << "Failed to render expression to pretty string: " << e.what();
    }
}

/**
 * Scan expression and retrieve all members
 */
void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit> &IUs) {
    if (node->IsCallable("Member")) {
        auto member = TCoMember(node);
        auto iu = TInfoUnit(member.Name().StringValue());
        IUs.push_back(TInfoUnit(member.Name().StringValue()));
        return;
    }

    for (auto c : node->Children()) {
        GetAllMembers(c, IUs);
    }
}

/**
 * Scan expression and retrieve all members while respecting scalar context variables:
 *   If `withScalarContext` is true - retrieve all members including all scalar context IUs
 */
void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit> &IUs, TPlanProps& props, bool withScalarContext) {
    if (node->IsCallable("Member")) {
        auto member = TCoMember(node);
        auto iu = TInfoUnit(member.Name().StringValue());
        if (props.ScalarSubplans.PlanMap.contains(iu)){
            if (withScalarContext) {
                iu.ScalarContext = true;
                IUs.push_back(iu);
            }
        }
        else {
            IUs.push_back(iu);
        }
        return;
    }

    for (auto c : node->Children()) {
        GetAllMembers(c, IUs, props, withScalarContext);
    }
}

TInfoUnit::TInfoUnit(TString name, bool scalarContext) : ScalarContext(scalarContext) {
    if (auto idx = name.find('.'); idx != TString::npos) {
        Alias = name.substr(0, idx);
        if (Alias.StartsWith("_alias_")) {
            Alias = Alias.substr(7);
        }
        ColumnName = name.substr(idx + 1);
    } else {
        Alias = "";
        ColumnName = name;
    }
}

TExprNode::TPtr TBroadcastConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                                      TExprContext &ctx) {
    if (FromSourceStage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }
    // clang-format off
    return Build<TDqCnBroadcast>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TMapConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                                TExprContext &ctx) {
    if (FromSourceStage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }
    // clang-format off
    return Build<TDqCnMap>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TUnionAllConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                                     TExprContext &ctx) {
    if (FromSourceStage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }
    // clang-format off
    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TShuffleConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                                    TExprContext &ctx) {
    TVector<TCoAtom> keyColumns;

    if (FromSourceStage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }

    for (auto k : Keys) {
        TString columnName;
        if (FromSourceStage || k.Alias == "") {
            columnName = k.ColumnName;
        } else {
            columnName = "_alias_" + k.Alias + "." + k.ColumnName;
        }
        auto atom = Build<TCoAtom>(ctx, pos).Value(columnName).Done();
        keyColumns.push_back(atom);
    }

    // clang-format off
    return Build<TDqCnHashShuffle>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
        .KeyColumns()
            .Add(keyColumns)
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TMergeConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                                TExprContext &ctx) {
    if (FromSourceStage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }

    TVector<TExprNode::TPtr> sortColumns;
    for (auto sortElement : Order) {
        sortColumns.push_back(Build<TDqSortColumn>(ctx, pos)
            .Column<TCoAtom>().Build(sortElement.SortColumn.GetFullName())
            .SortDirection().Build(sortElement.Ascending ? TTopSortSettings::AscendingSort : TTopSortSettings::DescendingSort)
            .Done().Ptr());
    }

    // clang-format off
    return Build<TDqCnMerge>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
        .SortColumns()
            .Add(sortColumns)
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TSourceConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                                   TExprContext &ctx) {
    Y_UNUSED(pos);
    Y_UNUSED(newStage);
    Y_UNUSED(ctx);
    return inputStage;
}

std::pair<TExprNode::TPtr, TExprNode::TPtr> TStageGraph::GenerateStageInput(int &stageInputCounter, TExprNode::TPtr &node,
                                                                            TExprContext &ctx, int fromStage) {
    TString inputName = "input_arg" + std::to_string(stageInputCounter++);
    YQL_CLOG(TRACE, CoreDq) << "Created stage argument " << inputName;
    auto arg = Build<TCoArgument>(ctx, node->Pos()).Name(inputName).Done().Ptr();
    auto output = arg;

    if (IsSourceStage(fromStage)) {
        output = AddRenames(arg, ctx, StageAttributes.at(fromStage));
    }

    return std::make_pair(arg, output);
}

void TStageGraph::TopologicalSort() {
    TVector<int> sortedStages;
    THashSet<int> visited;

    for (auto id : StageIds) {
        if (!visited.contains(id)) {
            DFS(id, sortedStages, visited, StageInputs);
        }
    }

    StageIds = sortedStages;
}

std::pair<TExprNode::TPtr, TVector<TExprNode::TPtr>> BuildSortKeySelector(TVector<TSortElement> sortElements, TExprContext &ctx, TPositionHandle pos) {

    auto arg = Build<TCoArgument>(ctx, pos).Name("arg").Done().Ptr();
    TVector<TExprNode::TPtr> directions;
    TVector<TExprNode::TPtr> members;

    for (auto el : sortElements) {
        // clang-format off
        members.push_back(Build<TCoMember>(ctx, pos)
            .Struct(arg)
            .Name().Build(el.SortColumn.GetFullName())
            .Done().Ptr());
        // clang-format on

        directions.push_back(Build<TCoBool>(ctx, pos).Literal().Build(el.Ascending ? "true" : "false").Done().Ptr());
    }

    TExprNode::TPtr selector;

    if (sortElements.size()==1) {
        // clang-format off
        selector = Build<TCoLambda>(ctx, pos)
            .Args({arg})
            .Body(members[0])
            .Done().Ptr();
        // clang-format on
    }
    else {
        // clang-format off
        selector = Build<TCoLambda>(ctx, pos)
            .Args({arg})
            .Body<TExprList>().Add(members).Build()
            .Done().Ptr();
        // clang-format on
    }

    return std::make_pair(selector, directions);
}


/**
 * Base class Operator methods
 */

void IOperator::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) {
    Y_UNUSED(renameMap);
    Y_UNUSED(ctx);
}

const TTypeAnnotationNode* IOperator::GetIUType(TInfoUnit iu) {
    auto structType = Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    return structType->FindItemType(iu.GetFullName());
}

/**
 * EmptySource operator methods
 */

TString TOpEmptySource::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx); 
    return "EmptySource"; 
}

/**
 * OpRead operator methods
 */

TOpRead::TOpRead(TExprNode::TPtr node) : IOperator(EOperator::Source, node->Pos()) {
    auto opSource = TKqpOpRead(node);

    auto alias = opSource.Alias().StringValue();
    for (auto c : opSource.Columns()) {
        Columns.push_back(c.StringValue());
    }

    Alias = alias;
    TableCallable = opSource.Table().Ptr();
    Pos = node->Pos();
}

TVector<TInfoUnit> TOpRead::GetOutputIUs() {
    TVector<TInfoUnit> res;

    for (auto c : Columns) {
        res.push_back(TInfoUnit(Alias, c));
    }
    return res;
}

TString TOpRead::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    auto res = TStringBuilder();
    res << "Read (" << TKqpTable(TableCallable).Path().StringValue() << "," << Alias << ", [";

    for (size_t i=0; i<Columns.size(); i++) {
        res << Columns[i];
        if (i != Columns.size()-1) {
            res << ", ";
        }
    }
    res << "])";
    return res;
}

/**
 * OpMap operator methods
 */

TOpMap::TOpMap(std::shared_ptr<IOperator> input, TPositionHandle pos, TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> mapElements,
               bool project)
    : IUnaryOperator(EOperator::Map, pos, input), MapElements(mapElements), Project(project) {}

TVector<TInfoUnit> TOpMap::GetOutputIUs() {
    TVector<TInfoUnit> res;
    if (!Project) {
        res = GetInput()->GetOutputIUs();
    }
    for (auto &[k, v] : MapElements) {
        res.push_back(k);
    }

    return res;
}

TVector<TExprNode::TPtr> TOpMap::GetLambdas() {
    TVector<TExprNode::TPtr> result;
    for (auto &[_,body] : MapElements) {
        if (std::holds_alternative<TExprNode::TPtr>(body)) {
            result.push_back(std::get<TExprNode::TPtr>(body));
        }
    }
    return result;
}

TVector<TInfoUnit> TOpMap::GetScalarSubplanIUs(TPlanProps& props) {
    TVector<TInfoUnit> allVars;
    TVector<TInfoUnit> res;

    for (auto &[ui, body] : MapElements) {
        if (std::holds_alternative<TExprNode::TPtr>(body)) {
            auto lambda = std::get<TExprNode::TPtr>(body);
            auto lambdaBody = TCoLambda(lambda).Body();
            GetAllMembers(lambdaBody.Ptr(), allVars, props, true);
        }
    }

    for ( auto iu : allVars) {
        if (iu.ScalarContext) {
            res.push_back(iu);
        }
    }
    return res;
}

bool TOpMap::HasRenames() const {
    for (auto &[iu, body] : MapElements) {
        if (std::holds_alternative<TInfoUnit>(body)) {
            return true;
        }
    }
    return false;
}

TVector<std::pair<TInfoUnit, TInfoUnit>> TOpMap::GetRenames() const {
    TVector<std::pair<TInfoUnit, TInfoUnit>> result;
    for (auto &[iu, body] : MapElements) {
        if (std::holds_alternative<TInfoUnit>(body)) {
            result.push_back(std::make_pair(iu, std::get<TInfoUnit>(body)));
        }
    }
    return result;
}

void TOpMap::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) {
    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> newMapElements;

    for (auto &el : MapElements) {

        TInfoUnit newIU = el.first;
        if (renameMap.contains(el.first)) {
            newIU = renameMap.at(el.first);
        }

        std::variant<TInfoUnit, TExprNode::TPtr> newBody = el.second;

        if (std::holds_alternative<TInfoUnit>(el.second)) {
            auto from = std::get<TInfoUnit>(el.second);
            if (renameMap.contains(from)) {
                newBody = renameMap.at(from);
            }
        } else {
            auto lambda = std::get<TExprNode::TPtr>(el.second);
            newBody = RenameMembers(lambda, renameMap, ctx);
        }

        // el.first = newIU;
        // el.second = newBody;
        newMapElements.push_back(std::make_pair(newIU, newBody));
    }
    MapElements = newMapElements;
}

void TOpMap::ApplyReplaceMap(TNodeOnNodeOwnedMap map, TRBOContext & ctx) {
    TOptimizeExprSettings settings(&ctx.TypeCtx);
    for (size_t i=0; i<MapElements.size(); i++) {
        auto & body = MapElements[i].second;
        if (std::holds_alternative<TExprNode::TPtr>(body)) {
            auto bodyLambda = std::get<TExprNode::TPtr>(body);
            RemapExpr(bodyLambda, bodyLambda, map, ctx.ExprCtx, settings);
            MapElements[i].second = std::variant<TInfoUnit,TExprNode::TPtr>(bodyLambda);
        }
    }
}


TString TOpMap::ToString(TExprContext& ctx) {
    auto res = TStringBuilder();
    res << "Map [";
    for (size_t i=0; i<MapElements.size(); i++) {
        auto & [k,v] = MapElements[i];

        res << k.GetFullName() << ":";
        if (std::holds_alternative<TInfoUnit>(v)) {
            res << std::get<TInfoUnit>(v).GetFullName();
        } else {
            res << PrintRBOExpression(std::get<TExprNode::TPtr>(v), ctx);
        }
        if (i != MapElements.size()-1) {
            res << ", ";
        }
    }
    res << "]";
    if (Project) {
        res << " Project";
    }
    return res;
}

/**
 * OpProject methods
 */

TOpProject::TOpProject(std::shared_ptr<IOperator> input, TPositionHandle pos, TVector<TInfoUnit> projectList)
    : IUnaryOperator(EOperator::Project, pos, input), ProjectList(projectList) {}

TVector<TInfoUnit> TOpProject::GetOutputIUs() {
    TVector<TInfoUnit> res;

    for (auto p : ProjectList) {
        res.push_back(p);
    }
    return res;
}

void TOpProject::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) {
    Y_UNUSED(ctx);

    for (auto &p : ProjectList) {
        if (renameMap.contains(p)) {
            p = renameMap.at(p);
        }
    }
}

TString TOpProject::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    auto res = TStringBuilder();
    res << "Project [";
    for (size_t i = 0; i < ProjectList.size(); i++) {
        auto iu = ProjectList[i];
        res << iu.GetFullName();
        if (i != ProjectList.size()-1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

/**
 * OpFilter operator methods
 */

TOpFilter::TOpFilter(std::shared_ptr<IOperator> input, TPositionHandle pos, TExprNode::TPtr filterLambda)
    : IUnaryOperator(EOperator::Filter, pos, input), FilterLambda(filterLambda) {}

TVector<TInfoUnit> TOpFilter::GetOutputIUs() { return GetInput()->GetOutputIUs(); }

void TOpFilter::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) {
    FilterLambda = RenameMembers(FilterLambda, renameMap, ctx);
}

TVector<TExprNode::TPtr> TOpFilter::GetLambdas() {
    return {FilterLambda};
}

void TOpFilter::ApplyReplaceMap(TNodeOnNodeOwnedMap map, TRBOContext & ctx) {
    TOptimizeExprSettings settings(&ctx.TypeCtx);
    RemapExpr(FilterLambda, FilterLambda, map, ctx.ExprCtx, settings);
}

TVector<TInfoUnit> TOpFilter::GetFilterIUs(TPlanProps& props) const {
    TVector<TInfoUnit> res;

    auto lambdaBody = TCoLambda(FilterLambda).Body();
    GetAllMembers(lambdaBody.Ptr(), res, props, true);
    return res;
}

TVector<TInfoUnit> TOpFilter::GetScalarSubplanIUs(TPlanProps& props) {
    TVector<TInfoUnit> res;
    for (auto iu : GetFilterIUs(props)) {
        if (iu.ScalarContext) {
            res.push_back(iu);
        }
    }
    return res;
}

bool TestAndExtractEqualityPredicate(TExprNode::TPtr pred, TExprNode::TPtr& leftArg, TExprNode::TPtr& rightArg) {
    if (pred->IsCallable("PgResolvedOp") && pred->Child(0)->Content() == "=") {
        leftArg = pred->Child(2);
        rightArg = pred->Child(3);
        return true;
    } else if (pred->IsCallable("==")) {
        leftArg = pred->Child(0);
        rightArg = pred->Child(1);
        return true;
    }
    return false;
}

TConjunctInfo TOpFilter::GetConjunctInfo(TPlanProps& props) const {
    TConjunctInfo res;

    auto lambdaBody = TCoLambda(FilterLambda).Body().Ptr();
    if (lambdaBody->IsCallable("ToPg")) {
        lambdaBody = lambdaBody->ChildPtr(0);
    }

    if (lambdaBody->IsCallable("And")) {
        for (auto conj : lambdaBody->Children()) {
            auto conjObj = conj;
            bool fromPg = false;

            if (conj->IsCallable("FromPg")) {
                conjObj = conj->ChildPtr(0);
                fromPg = true;
            }

            TExprNode::TPtr leftArg;
            TExprNode::TPtr rightArg;
            if (TestAndExtractEqualityPredicate(conjObj, leftArg, rightArg)) {
                TVector<TInfoUnit> conjIUs;
                GetAllMembers(conj, conjIUs, props);

                if (leftArg->IsCallable("Member") && rightArg->IsCallable("Member") && conjIUs.size() >= 2) {
                    TVector<TInfoUnit> leftIUs;
                    TVector<TInfoUnit> rightIUs;
                    GetAllMembers(leftArg, leftIUs, props);
                    GetAllMembers(rightArg, rightIUs, props);
                    res.JoinConditions.push_back(TJoinConditionInfo(conjObj, leftIUs[0], rightIUs[0]));
                }
                else {
                    TVector<TInfoUnit> conjIUs;
                    GetAllMembers(conj, conjIUs);
                    res.Filters.push_back(TFilterInfo(conj, conjIUs, fromPg));
                }
            } else {
                TVector<TInfoUnit> conjIUs;
                GetAllMembers(conj, conjIUs, props);
                res.Filters.push_back(TFilterInfo(conj, conjIUs, fromPg));
            }
        }
    } else {
        TVector<TInfoUnit> filterIUs;
        GetAllMembers(lambdaBody, filterIUs, props);
        res.Filters.push_back(TFilterInfo(lambdaBody, filterIUs));
    }

    return res;
}

TString TOpFilter::ToString(TExprContext& ctx) {
    return TStringBuilder() << "Filter :" << PrintRBOExpression(FilterLambda, ctx);
}

/**
 * OpJoin operator methods
 */

TOpJoin::TOpJoin(std::shared_ptr<IOperator> leftInput, std::shared_ptr<IOperator> rightInput, TPositionHandle pos, TString joinKind,
                 TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys)
    : IBinaryOperator(EOperator::Join, pos, leftInput, rightInput), JoinKind(joinKind), JoinKeys(joinKeys) {}

TVector<TInfoUnit> TOpJoin::GetOutputIUs() {
    auto res = GetLeftInput()->GetOutputIUs();
    auto rightInputIUs = GetRightInput()->GetOutputIUs();

    res.insert(res.end(), rightInputIUs.begin(), rightInputIUs.end());
    return res;
}

void TOpJoin::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) {
    Y_UNUSED(ctx);

    for (auto &k : JoinKeys) {
        if (renameMap.contains(k.first)) {
            k.first = renameMap.at(k.first);
        }
        if (renameMap.contains(k.second)) {
            k.second = renameMap.at(k.second);
        }
    }
}

TString TOpJoin::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << "Join [";
    for (size_t i = 0; i < JoinKeys.size(); i++) {
        auto [x,y] = JoinKeys[i];
        res << x.GetFullName() + "=" + y.GetFullName();
        if (i != JoinKeys.size() - 1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

/**
 * OpUnionAll operator methods
 */

TOpUnionAll::TOpUnionAll(std::shared_ptr<IOperator> leftInput, std::shared_ptr<IOperator> rightInput, TPositionHandle pos, bool ordered)
    : IBinaryOperator(EOperator::UnionAll, pos, leftInput, rightInput), Ordered(ordered) {}

TVector<TInfoUnit> TOpUnionAll::GetOutputIUs() {
    return GetLeftInput()->GetOutputIUs();
}

TString TOpUnionAll::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx); 
    return "UnionAll"; 
}

/**
 * OpLimit operator methods
 */

TOpLimit::TOpLimit(std::shared_ptr<IOperator> input, TPositionHandle pos, TExprNode::TPtr limitCond)
    : IUnaryOperator(EOperator::Limit, pos, input), LimitCond(limitCond) {}

TVector<TInfoUnit> TOpLimit::GetOutputIUs() { return GetInput()->GetOutputIUs(); }

void TOpLimit::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) {
    LimitCond = RenameMembers(LimitCond, renameMap, ctx);
}

TString TOpLimit::ToString(TExprContext& ctx) {
    return TStringBuilder() << "Limit: " << PrintRBOExpression(LimitCond, ctx); 
}

/**
 * OpAggregate operator methods
 */

TOpAggregate::TOpAggregate(std::shared_ptr<IOperator> input, TVector<TOpAggregationTraits>& aggTraitsList, TVector<TInfoUnit>& keyColumns,
                           EAggregationPhase aggPhase, bool distinctAll, TPositionHandle pos)
    : IUnaryOperator(EOperator::Aggregate, pos, input), AggregationTraitsList(aggTraitsList), KeyColumns(keyColumns),
      AggregationPhase(aggPhase), DistinctAll(distinctAll) {}

TVector<TInfoUnit> TOpAggregate::GetOutputIUs() {
    // We assume that aggregation returns column is order [keys, states]
    TVector<TInfoUnit> outputIU = KeyColumns;
    for (const auto& aggTraits : AggregationTraitsList) {
        outputIU.push_back(aggTraits.ResultColName);
    }
    return outputIU;
}

TString TOpAggregate::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);

    TStringBuilder strBuilder;
    strBuilder << "Aggregate (";
    for (ui32 i = 0; i < AggregationTraitsList.size(); ++i) {
        strBuilder << AggregationTraitsList[i].AggFunction << "(" << AggregationTraitsList[i].OriginalColName.GetFullName() << ")";
        if (i + 1 != AggregationTraitsList.size()) {
            strBuilder << ", ";
        }
    }

    strBuilder << " [";
    for (ui32 i = 0; i < KeyColumns.size(); ++i) {
        strBuilder << KeyColumns[i].GetFullName();
        if (i + 1 != KeyColumns.size()) {
            strBuilder << ", ";
        }
    }
    strBuilder << "] : ";
    if (AggregationPhase == EAggregationPhase::Intermediate) {
        strBuilder << "Initial";
    } else {
        strBuilder << "Final";
    }
    strBuilder << ")";
    return strBuilder;
}

/**
 * OpRoot operator methods
 */

TOpRoot::TOpRoot(std::shared_ptr<IOperator> input, TPositionHandle pos, TVector<TString> columnOrder) : 
    IUnaryOperator(EOperator::Root, pos, input), 
    ColumnOrder(columnOrder) {}

TVector<TInfoUnit> TOpRoot::GetOutputIUs() { return GetInput()->GetOutputIUs(); }

void ComputeParentsRec(std::shared_ptr<IOperator> op, std::shared_ptr<IOperator> parent) {
    if (parent) {
        auto f = std::find_if(op->Parents.begin(), op->Parents.end(),
                              [&parent](const std::weak_ptr<IOperator> &p) { return p.lock() == parent; });
        if (f == op->Parents.end()) {
            op->Parents.push_back(parent);
        }
    }
    for (auto &c : op->Children) {
        ComputeParentsRec(c, op);
    }
}

void TOpRoot::ComputeParents() {
    for (auto it : *this) {
        it.Current->Parents.clear();
    }
    std::shared_ptr<TOpRoot> noParent;
    ComputeParentsRec(GetInput(), noParent);

    for (auto subplan : PlanProps.ScalarSubplans.Get()) {
        ComputeParentsRec(subplan, noParent);
    }
}

TString TOpRoot::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    return "Root"; 
}

TString TOpRoot::PlanToString(TExprContext& ctx) {
    auto builder = TStringBuilder();
    PlanToStringRec(GetInput(), ctx, builder, 0);
    return builder;
}

void TOpRoot::PlanToStringRec(std::shared_ptr<IOperator> op, TExprContext& ctx, TStringBuilder &builder, int tabs) {
    for (int i = 0; i < tabs; i++) {
        builder << "  ";
    }
    builder << op->ToString(ctx) << "\n";
    for (auto c : op->Children) {
        PlanToStringRec(c, ctx, builder, tabs + 1);
    }
}

TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right) {
    TVector<TInfoUnit> res;

    for (auto &unit : left) {
        if (std::find(right.begin(), right.end(), unit) == right.end()) {
            res.push_back(unit);
        }
    }
    return res;
}
} // namespace NKqp
} // namespace NKikimr