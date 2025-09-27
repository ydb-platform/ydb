#include "kqp_operator.h"

namespace
{
    using namespace NKikimr;
    using namespace NKqp;
    using namespace NYql;
    using namespace NNodes;

    void DFS(int vertex, TVector<int> &sortedStages, THashSet<int> &visited, const THashMap<int, TVector<int>> &stageInputs)
    {
        visited.emplace(vertex);

        for (auto u : stageInputs.at(vertex))
        {
            if (!visited.contains(u))
            {
                DFS(u, sortedStages, visited, stageInputs);
            }
        }

        sortedStages.push_back(vertex);
    }

    TExprNode::TPtr AddRenames(TExprNode::TPtr input, TExprContext &ctx, TVector<TInfoUnit> renames)
    {
        TVector<TExprBase> items;
        auto arg = Build<TCoArgument>(ctx, input->Pos()).Name("arg").Done().Ptr();

        for (auto iu : renames)
        {
            // clang-format off
        auto tuple = Build<TCoNameValueTuple>(ctx, input->Pos())
            .Name().Build("_alias_" + iu.Alias + "." + iu.ColumnName)
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

    TExprNode::TPtr BuildSourceStage(TExprNode::TPtr dqsource, TExprContext &ctx)
    {
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

    TExprNode::TPtr RenameMembers(TExprNode::TPtr input, const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> & renameMap, TExprContext& ctx) {
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
        }
        else if (input->IsCallable()){
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
        }
        else if(input->IsList()){
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
        }
        else {
            return input;
        }
    }
}

namespace NKikimr
{
    namespace NKqp
    {

        using namespace NYql;
        using namespace NNodes;

        void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit> &IUs)
        {
            if (node->IsCallable("Member"))
            {
                auto member = TCoMember(node);
                IUs.push_back(TInfoUnit(member.Name().StringValue()));
                return;
            }

            for (auto c : node->Children())
            {
                GetAllMembers(c, IUs);
            }
        }

        TInfoUnit::TInfoUnit(TString name)
        {
            if (auto idx = name.find('.'); idx != TString::npos)
            {
                Alias = name.substr(0, idx);
                if (Alias.StartsWith("_alias_"))
                {
                    Alias = Alias.substr(7);
                }
                ColumnName = name.substr(idx + 1);
            }
            else
            {
                Alias = "";
                ColumnName = name;
            }
        }

        TExprNode::TPtr TBroadcastConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr &node, TExprNode::TPtr &newStage, TExprContext &ctx)
        {
            if (FromSourceStage)
            {
                inputStage = BuildSourceStage(inputStage, ctx);
                newStage = inputStage;
            }
            // clang-format off
    return Build<TDqCnBroadcast>(ctx, node->Pos())
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
    .Done().Ptr();
            // clang-format on
        }

        TExprNode::TPtr TMapConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr &node, TExprNode::TPtr &newStage, TExprContext &ctx)
        {
            if (FromSourceStage)
            {
                inputStage = BuildSourceStage(inputStage, ctx);
                newStage = inputStage;
            }
            // clang-format off
    return Build<TDqCnMap>(ctx, node->Pos())
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
    .Done().Ptr();
            // clang-format on
        }

        TExprNode::TPtr TUnionAllConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr &node, TExprNode::TPtr &newStage, TExprContext &ctx)
        {
            if (FromSourceStage)
            {
                inputStage = BuildSourceStage(inputStage, ctx);
                newStage = inputStage;
            }
            // clang-format off
    return Build<TDqCnUnionAll>(ctx, node->Pos())
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
    .Done().Ptr();
            // clang-format on
        }

        TExprNode::TPtr TShuffleConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr &node, TExprNode::TPtr &newStage, TExprContext &ctx)
        {
            TVector<TCoAtom> keyColumns;

            if (FromSourceStage)
            {
                inputStage = BuildSourceStage(inputStage, ctx);
                newStage = inputStage;
            }

            for (auto k : Keys)
            {
                TString columnName;
                if (FromSourceStage || k.Alias == "")
                {
                    columnName = k.ColumnName;
                }
                else
                {
                    columnName = "_alias_" + k.Alias + "." + k.ColumnName;
                }
                auto atom = Build<TCoAtom>(ctx, node->Pos()).Value(columnName).Done();
                keyColumns.push_back(atom);
            }

            // clang-format off
    return Build<TDqCnHashShuffle>(ctx, node->Pos())
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

        TExprNode::TPtr TSourceConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr &node, TExprNode::TPtr &newStage, TExprContext &ctx)
        {
            Y_UNUSED(node);
            Y_UNUSED(newStage);
            Y_UNUSED(ctx);
            return inputStage;
        }

        std::pair<TExprNode::TPtr, TExprNode::TPtr> TStageGraph::GenerateStageInput(int &stageInputCounter, TExprNode::TPtr &node, TExprContext &ctx, int fromStage)
        {
            TString inputName = "input_arg" + std::to_string(stageInputCounter++);
            YQL_CLOG(TRACE, CoreDq) << "Created stage argument " << inputName;
            auto arg = Build<TCoArgument>(ctx, node->Pos()).Name(inputName).Done().Ptr();
            auto output = arg;

            if (IsSourceStage(fromStage))
            {
                output = AddRenames(arg, ctx, StageAttributes.at(fromStage));
            }

            return std::make_pair(arg, output);
        }

        void TStageGraph::TopologicalSort()
        {
            TVector<int> sortedStages;
            THashSet<int> visited;

            for (auto id : StageIds)
            {
                if (!visited.contains(id))
                {
                    DFS(id, sortedStages, visited, StageInputs);
                }
            }

            StageIds = sortedStages;
        }

        void IOperator::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> & renameMap, TExprContext & ctx) {
            Y_UNUSED(renameMap);
            Y_UNUSED(ctx);
        }

        TOpRead::TOpRead(TExprNode::TPtr node) : IOperator(EOperator::Source, node)
        {
            auto opSource = TKqpOpRead(node);

            auto alias = opSource.Alias().StringValue();
            for (auto c : opSource.Columns())
            {
                Columns.push_back(c.StringValue());
            }

            Alias = alias;
        }

        TVector<TInfoUnit> TOpRead::GetOutputIUs() {
            TVector<TInfoUnit> res;

            for (auto c : Columns)
            {
                res.push_back(TInfoUnit(Alias, c));
            }
            return res;
        }

        TString TOpRead::ToString() {
            return "Read (" + Alias + ")";
        }

        TOpMap::TOpMap(std::shared_ptr<IOperator> input, TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> mapElements, bool project) :
            IUnaryOperator(EOperator::Map, input), MapElements(mapElements), Project(project) {
        }

        TVector<TInfoUnit> TOpMap::GetOutputIUs() {
            TVector<TInfoUnit> res;
            if (!Project) {
                res = GetInput()->GetOutputIUs();
            }
            for (auto & [k, v] : MapElements) {
                res.push_back(k);
            }

            return res;
        }


        bool TOpMap::HasRenames() const {
            for (auto & [iu, body] : MapElements) {
                if (std::holds_alternative<TInfoUnit>(body)) {
                    return true;
                }
            }
            return false;
        }

        TVector<std::pair<TInfoUnit, TInfoUnit>> TOpMap::GetRenames() const {
            TVector<std::pair<TInfoUnit, TInfoUnit>> result;
            for (auto & [iu, body] : MapElements) {
                if (std::holds_alternative<TInfoUnit>(body)) {
                    result.push_back(std::make_pair(iu, std::get<TInfoUnit>(body)));
                }
            }
            return result;
        }

        void TOpMap::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> & renameMap, TExprContext & ctx) {
            TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> newMapElements;

            for (auto & el : MapElements) {

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
                }
                else {
                    auto lambda = std::get<TExprNode::TPtr>(el.second);
                    newBody = RenameMembers(lambda, renameMap, ctx);
                }

                //el.first = newIU;
                //el.second = newBody;
                newMapElements.push_back(std::make_pair(newIU, newBody));
            }
            MapElements = newMapElements;
        }

        TString TOpMap::ToString() {
            auto res = TStringBuilder() << "Map [";
            for (auto & [k,v] : MapElements) {
                res << k.GetFullName() << ",";
            }
            res << "]";
            return res;
        }

        TOpProject::TOpProject(std::shared_ptr<IOperator> input, TVector<TInfoUnit> projectList) : IUnaryOperator(EOperator::Project, input),
            ProjectList(projectList)
        {}

        TVector<TInfoUnit> TOpProject::GetOutputIUs() {
            TVector<TInfoUnit> res;

            for (auto p : ProjectList ) {
                res.push_back(p);
            }
            return res;
        }

        void TOpProject::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> & renameMap, TExprContext & ctx) {
            Y_UNUSED(ctx);

            for (auto & p : ProjectList) {
                if (renameMap.contains(p)) {
                    p = renameMap.at(p);
                }
            }
        }

        TString TOpProject::ToString() {
            auto res = TStringBuilder() << "Project [";
            for (auto iu : ProjectList) {
                res << iu.GetFullName() << ",";
            }
            res << "]";
            return res;
        }

        TOpFilter::TOpFilter(std::shared_ptr<IOperator> input, TExprNode::TPtr filterLambda) : IUnaryOperator(EOperator::Filter, input),
            FilterLambda(filterLambda) {
        }

        TVector<TInfoUnit> TOpFilter::GetOutputIUs() {
            return GetInput()->GetOutputIUs();
        }

        void TOpFilter::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> & renameMap, TExprContext & ctx) {
            FilterLambda = RenameMembers(FilterLambda, renameMap, ctx);
        }

        TVector<TInfoUnit> TOpFilter::GetFilterIUs() const
        {
            TVector<TInfoUnit> res;

            auto lambdaBody = TCoLambda(FilterLambda).Body();
            GetAllMembers(lambdaBody.Ptr(), res);
            return res;
        }

        TConjunctInfo TOpFilter::GetConjunctInfo() const
        {
            TConjunctInfo res;

            auto lambdaBody = TCoLambda(FilterLambda).Body().Ptr();
            if (lambdaBody->IsCallable("ToPg"))
            {
                lambdaBody = lambdaBody->ChildPtr(0);
            }

            if (lambdaBody->IsCallable("And"))
            {
                for (auto conj : lambdaBody->Children())
                {
                    auto conjObj = conj;
                    bool fromPg = false;

                    if (conj->IsCallable("FromPg"))
                    {
                        conjObj = conj->ChildPtr(0);
                        fromPg = true;
                    }

                    if (conjObj->IsCallable("PgResolvedOp") && conjObj->Child(0)->Content() == "=")
                    {
                        auto leftArg = conjObj->Child(2);
                        auto rightArg = conjObj->Child(3);

                        if (!leftArg->IsCallable("Member") || !rightArg->IsCallable("Member"))
                        {
                            TVector<TInfoUnit> conjIUs;
                            GetAllMembers(conj, conjIUs);
                            res.Filters.push_back(TFilterInfo(conj, conjIUs, fromPg ));
                        }
                        else
                        {
                            TVector<TInfoUnit> leftIUs;
                            TVector<TInfoUnit> rightIUs;
                            GetAllMembers(leftArg, leftIUs);
                            GetAllMembers(rightArg, rightIUs);
                            res.JoinConditions.push_back(TJoinConditionInfo(conjObj, leftIUs[0], rightIUs[0]));
                        }
                    }
                    else
                    {
                        TVector<TInfoUnit> conjIUs;
                        GetAllMembers(conj, conjIUs);
                        res.Filters.push_back(TFilterInfo(conj, conjIUs, fromPg ));
                    }
                }
            }
            else
            {
                TVector<TInfoUnit> filterIUs;
                GetAllMembers(lambdaBody, filterIUs);
                res.Filters.push_back(TFilterInfo(lambdaBody, filterIUs));
            }

            return res;
        }

        TString TOpFilter::ToString() {
            return "Filter";
        }

        TOpJoin::TOpJoin(std::shared_ptr<IOperator> leftInput, std::shared_ptr<IOperator> rightInput, TString joinKind, TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys) :
            IBinaryOperator(EOperator::Join, leftInput, rightInput),
            JoinKind(joinKind),
            JoinKeys(joinKeys) {
        }

        TVector<TInfoUnit> TOpJoin::GetOutputIUs() {
            auto res = GetLeftInput()->GetOutputIUs();
            auto rightInputIUs = GetRightInput()->GetOutputIUs();

            res.insert(res.end(), rightInputIUs.begin(), rightInputIUs.end());
            return res;
        }

        void TOpJoin::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> & renameMap, TExprContext & ctx) {
            Y_UNUSED(ctx);

            for (auto & k : JoinKeys ) {
                if (renameMap.contains(k.first)) {
                    k.first = renameMap.at(k.first);
                }
                if (renameMap.contains(k.second)) {
                    k.second = renameMap.at(k.second);
                }
            }
        }

        TString TOpJoin::ToString() {
            return "Join";
        }

        TOpLimit::TOpLimit(std::shared_ptr<IOperator> input, TExprNode::TPtr limitCond) : IUnaryOperator(EOperator::Limit, input),
            LimitCond(limitCond) {
        }

        TVector<TInfoUnit> TOpLimit::GetOutputIUs() {
            return GetInput()->GetOutputIUs();
        }

        void TOpLimit::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> & renameMap, TExprContext & ctx) {
            LimitCond = RenameMembers(LimitCond, renameMap, ctx);
        }

        TString TOpLimit::ToString() {
            return "Limit";
        }

        TOpRoot::TOpRoot(std::shared_ptr<IOperator> input) : IUnaryOperator(EOperator::Root, input) {
        }

        TVector<TInfoUnit> TOpRoot::GetOutputIUs() {
            return GetInput()->GetOutputIUs();
        }

        void ComputeParentsRec(std::shared_ptr<IOperator> op, std::shared_ptr<IOperator> parent) {
            if (parent) {
                auto f = std::find_if(op->Parents.begin(), op->Parents.end(), [&parent](const std::weak_ptr<IOperator>& p) {
                    return p.lock() == parent;
                });
                if (f == op->Parents.end())
                {
                    op->Parents.push_back(parent);
                }
            }
            for (auto & c : op->Children) {
                ComputeParentsRec(c, op);
            }
        }

        void TOpRoot::ComputeParents() {
            for ( auto it : *this ) {
                it.Current->Parents.clear();
            }
            std::shared_ptr<TOpRoot> noParent;
            ComputeParentsRec(GetInput(), noParent);
        }

        TString TOpRoot::ToString() {
            return "Root";
        }

        TString TOpRoot::PlanToString() {
            auto builder = TStringBuilder();
            PlanToStringRec(GetInput(), builder, 0);
            return builder;
        }

        void TOpRoot::PlanToStringRec(std::shared_ptr<IOperator> op, TStringBuilder & builder, int tabs) {
            for (int i=0; i<tabs; i++) {
                builder << "  ";
            }
            builder << op->ToString() << "\n";
            for ( auto c : op->Children) {
                PlanToStringRec(c, builder, tabs+1);
            }
        }

        TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right)
        {
            TVector<TInfoUnit> res;

            for (auto &unit : left)
            {
                if (std::find(right.begin(), right.end(), unit) == right.end())
                {
                    res.push_back(unit);
                }
            }
            return res;
        }
    }
}