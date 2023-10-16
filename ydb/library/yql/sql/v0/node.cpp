#include "node.h"
#include "context.h"

#include <ydb/library/yql/ast/yql_ast_escaping.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/charset/ci_string.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

using namespace NYql;

namespace NSQLTranslationV0 {

TString ErrorDistinctWithoutCorrelation(const TString& column) {
    return TStringBuilder() << "DISTINCT columns for JOIN in SELECT should have table aliases (correlation name),"
        " add it if necessary to FROM section over 'AS <alias>' keyword and put it like '<alias>." << column << "'";
}

TString ErrorDistinctByGroupKey(const TString& column) {
    return TStringBuilder() << "Unable to use DISTINCT by grouping column: " << column << ". You should leave one of them.";
}

TTableRef::TTableRef(const TString& refName, const TString& cluster, TNodePtr keys)
    : RefName(refName)
    , Cluster(cluster)
    , Keys(keys)
{
}

TTableRef::TTableRef(const TTableRef& tr)
    : RefName(tr.RefName)
    , Cluster(tr.Cluster)
    , Keys(tr.Keys)
    , Options(tr.Options)
{
}

TString TTableRef::ShortName() const {
    Y_DEBUG_ABORT_UNLESS(Keys);
    if (Keys->GetTableKeys()->GetTableName()) {
        return *Keys->GetTableKeys()->GetTableName();
    }
    return TString();
}

TString TTableRef::ServiceName(const TContext& ctx) const {
    auto service = ctx.GetClusterProvider(Cluster);
    YQL_ENSURE(service);
    return *service;
}

bool TTableRef::Check(TContext& ctx) const {
    if (Cluster.empty()) {
        ctx.Error() << "No cluster name given and no default cluster is selected";
        return false;
    }

    auto service = ctx.GetClusterProvider(Cluster);
    if (!service) {
        ctx.Error() << "Unknown cluster name: " << Cluster;
        return false;
    }

    if (!Keys) {
        ctx.Error() << "No table name given";
        return false;
    }

    return true;
}

TColumnSchema::TColumnSchema(TPosition pos, const TString& name, const TString& type, bool nullable, bool isTypeString)
    : Pos(pos)
    , Name(name)
    , Type(type)
    , Nullable(nullable)
    , IsTypeString(isTypeString)
{
}

INode::INode(TPosition pos)
    : Pos(pos)
{
}

INode::~INode()
{
}

TPosition INode::GetPos() const {
    return Pos;
}

const TString& INode::GetLabel() const {
    return Label;
}

void INode::SetLabel(const TString& label) {
    Label = label;
}

void INode::SetCountHint(bool isCount) {
    State.Set(ENodeState::CountHint, isCount);
}

bool INode::GetCountHint() const {
    return State.Test(ENodeState::CountHint);
}

bool INode::IsConstant() const {
    return HasState(ENodeState::Const);
}

bool INode::IsAggregated() const {
    return HasState(ENodeState::Aggregated);
}

bool INode::IsAggregationKey() const {
    return HasState(ENodeState::AggregationKey);
}

bool INode::IsOverWindow() const {
    return HasState(ENodeState::OverWindow);
}

bool INode::IsNull() const {
    return false;
}

bool INode::IsIntegerLiteral() const {
    return false;
}

bool INode::IsAsterisk() const {
    return false;
}

const TString* INode::SubqueryAlias() const {
    return nullptr;
}

TString INode::GetOpName() const {
    return TString();
}

const TString* INode::GetLiteral(const TString& type) const {
    Y_UNUSED(type);
    return nullptr;
}

const TString* INode::GetColumnName() const {
    return nullptr;
}

void INode::AssumeColumn() {
}

const TString* INode::GetSourceName() const {
    return nullptr;
}

const TString* INode::GetAtomContent() const {
    return nullptr;
}

size_t INode::GetTupleSize() const {
    return 0;
}

INode::TPtr INode::GetTupleElement(size_t index) const {
    Y_UNUSED(index);
    return nullptr;
}

ITableKeys* INode::GetTableKeys() {
    return nullptr;
}

ISource* INode::GetSource() {
    return nullptr;
}

TVector<TNodePtr>* INode::ContentListPtr() {
    return nullptr;
}

bool INode::Init(TContext& ctx, ISource* src) {
    if (State.Test(ENodeState::Failed)) {
        return false;
    }

    if (!State.Test(ENodeState::Initialized)) {
        if (!DoInit(ctx, src)) {
            State.Set(ENodeState::Failed);
            return false;
        }
        State.Set(ENodeState::Initialized);
    }
    return true;
}

bool INode::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(ctx);
    Y_UNUSED(src);
    return true;
}

TNodePtr INode::AstNode() const {
    return new TAstListNodeImpl(Pos);
}

TNodePtr INode::AstNode(TNodePtr node) const {
    return node;
}

TNodePtr INode::AstNode(const TString& str) const {
    return new TAstAtomNodeImpl(Pos, str, TNodeFlags::Default);
}

TNodePtr INode::AstNode(TAstNode* node) const {
    return new TAstDirectNode(node);
}

TNodePtr INode::Clone() const {
    TNodePtr clone = DoClone();
    if (!clone) {
        clone = const_cast<INode*>(this);
    } else {
        YQL_ENSURE(!State.Test(ENodeState::Initialized), "Clone shold be for uninitialized or persistent node");
        clone->SetLabel(Label);
    }
    return clone;
}

TAggregationPtr INode::GetAggregation() const {
    return {};
}

INode::TPtr INode::WindowSpecFunc(const TPtr& type) const {
    Y_UNUSED(type);
    return {};
}

void INode::UseAsInner() {
    AsInner = true;
}

bool INode::UsedSubquery() const {
    return false;
}

bool INode::IsSelect() const {
    return false;
}

TNodePtr INode::ShallowCopy() const {
    Y_DEBUG_ABORT_UNLESS(false, "Node is not copyable");
    return nullptr;
}

void INode::DoUpdateState() const {
}

void INode::PrecacheState() const {
    if (State.Test(ENodeState::Failed)) {
        return;
    }

    /// Not work right now! It's better use Init at first, because some kind of update depend on it
    /// \todo turn on and remove all issues
    //Y_DEBUG_ABORT_UNLESS(State.Test(ENodeState::Initialized));
    if (State.Test(ENodeState::Precached)) {
        return;
    }
    DoUpdateState();
    State.Set(ENodeState::Precached);
}

void INode::DoAdd(TNodePtr node) {
    Y_UNUSED(node);
    Y_DEBUG_ABORT_UNLESS(false, "Node is not expandable");
}

TAstAtomNode::TAstAtomNode(TPosition pos, const TString& content, ui32 flags)
    : INode(pos)
    , Content(content)
    , Flags(flags)
{
}

TAstAtomNode::~TAstAtomNode()
{
}

void TAstAtomNode::DoUpdateState() const {
    State.Set(ENodeState::Const);
}

TAstNode* TAstAtomNode::Translate(TContext& ctx) const {
    return TAstNode::NewAtom(Pos, Content, *ctx.Pool, Flags);
}

const TString* TAstAtomNode::GetAtomContent() const {
    return &Content;
}

TAstDirectNode::TAstDirectNode(TAstNode* node)
    : INode(node->GetPosition())
    , Node(node)
{
}

TAstNode* TAstDirectNode::Translate(TContext& ctx) const {
    Y_UNUSED(ctx);
    return Node;
}

TNodePtr BuildAtom(TPosition pos, const TString& content, ui32 flags) {
    return new TAstAtomNodeImpl(pos, content, flags);
}

TAstListNode::TAstListNode(TPosition pos)
    : INode(pos)
{
}

TAstListNode::~TAstListNode()
{
}

bool TAstListNode::DoInit(TContext& ctx, ISource* src) {
    for (auto& node: Nodes) {
        if (!node->Init(ctx, src)) {
            return false;
        }
    }
    return true;
}

TAstNode* TAstListNode::Translate(TContext& ctx) const {
    TSmallVec<TAstNode*> children;
    children.reserve(Nodes.size());
    auto listPos = Pos;
    for (auto& node: Nodes) {
        if (node) {
            auto astNode = node->Translate(ctx);
            if (!astNode) {
                return nullptr;
            }
            children.push_back(astNode);
        } else {
            ctx.Error(Pos) << "Translation error: encountered empty TNodePtr";
            return nullptr;
        }
    }

    return TAstNode::NewList(listPos, children.data(), children.size(), *ctx.Pool);
}

void TAstListNode::UpdateStateByListNodes(const TVector<TNodePtr>& nodes) const {
    bool isConst = true;
    struct TAttributesFlags {
        bool has = false;
        bool all = true;
    };
    std::array<ENodeState, 3> checkStates = {{ENodeState::Aggregated, ENodeState::AggregationKey, ENodeState::OverWindow}};
    std::map<ENodeState, TAttributesFlags> flags;
    for (auto& node: nodes) {
        const bool isNodeConst = node->IsConstant();
        for (auto state: checkStates) {
            if (node->HasState(state)) {
                flags[state].has = true;
            } else if (!isNodeConst) {
                isConst = false;
                flags[state].all = false;
            }
        }
    }
    State.Set(ENodeState::Const, isConst);
    for (auto& flag: flags) {
        State.Set(flag.first, flag.second.has && flag.second.all);
    }
}

void TAstListNode::DoUpdateState() const {
    UpdateStateByListNodes(Nodes);
}

TAstListNode::TAstListNode(const TAstListNode& node)
    : INode(node.Pos)
    , Nodes(node.Nodes)
{
    Label = node.Label;
    State = node.State;
}

TAstListNode::TAstListNode(TPosition pos, TVector<TNodePtr>&& nodes)
    : INode(pos)
    , Nodes(std::move(nodes))
{
}

TNodePtr TAstListNode::ShallowCopy() const {
    return new TAstListNodeImpl(Pos, Nodes);
}

void TAstListNode::DoAdd(TNodePtr node) {
    Y_DEBUG_ABORT_UNLESS(node);
    Y_DEBUG_ABORT_UNLESS(node.Get() != this);
    Nodes.push_back(node);
}

TAstListNodeImpl::TAstListNodeImpl(TPosition pos)
    : TAstListNode(pos)
{}

TAstListNodeImpl::TAstListNodeImpl(TPosition pos, TVector<TNodePtr> nodes)
    : TAstListNode(pos)
{
    Nodes.swap(nodes);
}

TNodePtr TAstListNodeImpl::DoClone() const {
    return new TAstListNodeImpl(Pos, CloneContainer(Nodes));
}

bool ValidateAllNodesForAggregation(TContext& ctx, const TVector<TNodePtr>& nodes) {
    for (auto& node: nodes) {
        if (node->IsConstant()) {
            continue;
        }
        if (!node->IsAggregated() && !node->IsOverWindow()) {
            ctx.Error(node->GetPos()) << "Expression has to be an aggregation function or key column, because aggregation is used elsewhere in this subquery";
            return false;
        }
    }
    return true;
}

TCallNode::TCallNode(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TAstListNode(pos)
    , OpName(opName)
    , MinArgs(minArgs)
    , MaxArgs(maxArgs)
    , Args(args)
{
}

TString TCallNode::GetOpName() const {
    return OpName;
}

const TString* TCallNode::GetSourceName() const {
    const TString* name = nullptr;
    for (auto& arg: Args) {
        auto n = arg->GetSourceName();
        if (!n) {
            continue;
        }
        if (name && *n != *name) {
            return nullptr;
        }
        name = n;
    }
    return name;
}

const TVector<TNodePtr>& TCallNode::GetArgs() const {
    return Args;
}

void TCallNode::DoUpdateState() const {
    UpdateStateByListNodes(Args);
}

TString TCallNode::GetCallExplain() const {
    auto derivedName = GetOpName();
    TStringBuilder sb;
    sb << derivedName << "()";
    if (derivedName != OpName) {
        sb << ", converted to " << OpName << "()";
    }
    return std::move(sb);
}

bool TCallNode::ValidateArguments(TContext& ctx) const {
    const auto argsCount = static_cast<i32>(Args.size());
    if (MinArgs >= 0 && MaxArgs == MinArgs && argsCount != MinArgs) {
        ctx.Error(Pos) << GetCallExplain() << " requires exactly " << MinArgs << " arguments, given: " << Args.size();
        return false;
    }

    if (MinArgs >= 0 && argsCount < MinArgs) {
        ctx.Error(Pos) << GetCallExplain() << " requires at least " << MinArgs << " arguments, given: " << Args.size();
        return false;
    }

    if (MaxArgs >= 0 && argsCount > MaxArgs) {
        ctx.Error(Pos) << GetCallExplain() << " requires at most " << MaxArgs << " arguments, given: " << Args.size();
        return false;
    }

    return true;
}

bool TCallNode::DoInit(TContext& ctx, ISource* src) {
    if (!ValidateArguments(ctx)) {
        return false;
    }

    bool hasError = false;
    for (auto& arg: Args) {
        if (!arg->Init(ctx, src)) {
            hasError = true;
            continue;
        }
    }

    if (hasError) {
        return false;
    }

    Nodes.push_back(BuildAtom(Pos, OpName));
    Nodes.insert(Nodes.end(), Args.begin(), Args.end());
    return true;
}

TCallNodeImpl::TCallNodeImpl(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, minArgs, maxArgs, args)
{}

TCallNodeImpl::TCallNodeImpl(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, args.size(), args.size(), args)
{}

TCallNode::TPtr TCallNodeImpl::DoClone() const {
    return new TCallNodeImpl(GetPos(), OpName, MinArgs, MaxArgs, CloneContainer(Args));
}

TCallNodeDepArgs::TCallNodeDepArgs(ui32 reqArgsCount, TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, minArgs, maxArgs, args)
    , ReqArgsCount(reqArgsCount)
{}

TCallNodeDepArgs::TCallNodeDepArgs(ui32 reqArgsCount, TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, args.size(), args.size(), args)
    , ReqArgsCount(reqArgsCount)
{}

TCallNode::TPtr TCallNodeDepArgs::DoClone() const {
    return new TCallNodeDepArgs(ReqArgsCount, GetPos(), OpName, MinArgs, MaxArgs, CloneContainer(Args));
}

bool TCallNodeDepArgs::DoInit(TContext& ctx, ISource* src) {
    if (!TCallNode::DoInit(ctx, src)) {
        return false;
    }

    for (ui32 i = 1 + ReqArgsCount; i < Nodes.size(); ++i) {
        Nodes[i] = Y("DependsOn", Nodes[i]);
    }
    return true;
}

TCallDirectRow::TPtr TCallDirectRow::DoClone() const {
    return new TCallDirectRow(Pos, OpName, CloneContainer(Args));
}

TCallDirectRow::TCallDirectRow(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, 0, 0, args)
{}

bool TCallDirectRow::DoInit(TContext& ctx, ISource* src) {
    if (!src) {
        ctx.Error(Pos) << "Unable to use function: " << OpName << " without source";
        return false;
    }
    if (src->IsCompositeSource() || src->GetJoin() || src->HasAggregations() || src->IsFlattenByColumns() || src->IsOverWindowSource()) {
        ctx.Error(Pos) << "Failed to use function: " << OpName << " with aggregation, join, flatten by or window functions";
        return false;
    }
    if (!TCallNode::DoInit(ctx, src)) {
        return false;
    }
    Nodes.push_back(Y("DependsOn", "row"));
    return true;
}

void TCallDirectRow::DoUpdateState() const {
    State.Set(ENodeState::Const, false);
}

void TWinAggrEmulation::DoUpdateState() const {
    State.Set(ENodeState::OverWindow, true);
}

bool TWinAggrEmulation::DoInit(TContext& ctx, ISource* src) {
    if (!src) {
        ctx.Error(Pos) << "Unable to use window function: " << OpName << " without source";
        return false;
    }

    if (!src->IsOverWindowSource()) {
        ctx.Error(Pos) << "Failed to use window function: " << OpName << " without window specification";
        return false;
    }
    if (!src->AddFuncOverWindow(ctx, this)) {
        ctx.Error(Pos) << "Failed to use window function: " << OpName << " without specification or in wrong place";
        return false;
    }

    FuncAlias = "_yql_" + src->MakeLocalName(OpName);
    src->AddTmpWindowColumn(FuncAlias);
    ctx.PushBlockShortcuts();
    if (!TCallNode::DoInit(ctx, src)) {
        return false;
    }
    WinAggrGround = ctx.GroundBlockShortcuts(Pos);
    Nodes.clear();
    Add("Member", "row", Q(FuncAlias));
    return true;
}

INode::TPtr TWinAggrEmulation::WindowSpecFunc(const TPtr& type) const {
    auto result = Y(OpName, type);
    for (const auto& arg: Args) {
        result = L(result, arg);
    }
    return Q(Y(Q(FuncAlias), result));
}

TWinAggrEmulation::TWinAggrEmulation(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, minArgs, maxArgs, args)
    , FuncAlias(opName)
{}

TWinRowNumber::TWinRowNumber(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{}

TWinLeadLag::TWinLeadLag(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{}

bool TWinLeadLag::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() >= 2) {
        if (!Args[1]->IsIntegerLiteral()) {
            ctx.Error(Args[1]->GetPos()) << "Expected integer literal as second parameter of " << OpName << "( ) function";
            return false;
        }
    }
    if (!TWinAggrEmulation::DoInit(ctx, src)) {
        return false;
    }
    if (Args.size() >= 1) {
        Args[0] = BuildLambda(Pos, Y("row"), GroundWithExpr(WinAggrGround, Args[0]));
    }
    return true;
}

class TQuotedAtomNode: public TAstListNode {
public:
    TQuotedAtomNode(TPosition pos, const TString& content, ui32 flags)
        : TAstListNode(pos)
    {
        Add("quote", BuildAtom(pos, content, flags));
    }

protected:
    TQuotedAtomNode(const TQuotedAtomNode& other)
        : TAstListNode(other.Pos)
    {
        Nodes = CloneContainer(other.Nodes);
    }
    TPtr DoClone() const final {
        return new TQuotedAtomNode(*this);
    }
};

TNodePtr BuildQuotedAtom(TPosition pos, const TString& content, ui32 flags) {
    return new TQuotedAtomNode(pos, content, flags);
}

bool TColumns::Add(const TString* column, bool countHint, bool isArtificial, bool isReliable, bool hasName) {
    if (!column || *column == "*") {
        if (!countHint) {
            SetAll();
        }
    } else if (!All) {
        if (column->EndsWith('*')) {
            QualifiedAll = true;
        }

        bool inserted = false;
        if (isArtificial) {
            inserted = Artificial.insert(*column).second;
        } else {
            inserted = Real.insert(*column).second;
        }
        if (!isReliable) {
            HasUnreliable = true;
        }
        if (std::find(List.begin(), List.end(), *column) == List.end()) {
            List.push_back(*column);
            NamedColumns.push_back(hasName);
        }
        return inserted;
    }
    return All;
}

void TColumns::Merge(const TColumns& columns) {
    if (columns.All) {
        SetAll();
    } else {
        for (auto& c: columns.List) {
            if (columns.Real.contains(c)) {
                Add(&c, false, false);
            }
            if (columns.Artificial.contains(c)) {
                Add(&c, false, true);
            }
        }
        HasUnreliable |= columns.HasUnreliable;
    }
}

void TColumns::SetPrefix(const TString& prefix) {
    Y_DEBUG_ABORT_UNLESS(!prefix.empty());
    auto addPrefixFunc = [&prefix](const TString& str) {
        return prefix + "." + str;
    };
    TSet<TString> newReal;
    TSet<TString> newArtificial;
    TVector<TString> newList;
    std::transform(Real.begin(), Real.end(), std::inserter(newReal, newReal.begin()), addPrefixFunc);
    std::transform(Artificial.begin(), Artificial.end(), std::inserter(newArtificial, newArtificial.begin()), addPrefixFunc);
    std::transform(List.begin(), List.end(), std::back_inserter(newList), addPrefixFunc);
    newReal.swap(Real);
    newArtificial.swap(Artificial);
    newList.swap(List);
}

void TColumns::SetAll() {
    All = true;
    Real.clear();
    List.clear();
    Artificial.clear();
}

bool TColumns::IsColumnPossible(TContext& ctx, const TString& name) {
    if (All || Real.contains(name) || Artificial.contains(name)) {
        return true;
    }
    if (QualifiedAll) {
        if (ctx.SimpleColumns) {
            return true;
        }
        for (const auto& real: Real) {
            const auto pos = real.find_first_of("*");
            if (pos == TString::npos) {
                continue;
            }
            if (name.StartsWith(real.substr(0, pos))) {
                return true;
            }
        }
    }
    return false;
}

TSortSpecificationPtr TSortSpecification::Clone() const {
    auto res = MakeIntrusive<TSortSpecification>();
    res->OrderExpr = OrderExpr->Clone();
    res->Ascending = Ascending;
    return res;
}

TWindowSpecificationPtr TWindowSpecification::Clone() const {
    auto res = MakeIntrusive<TWindowSpecification>();
    res->ExistingWindowName = ExistingWindowName;
    res->Partitions = CloneContainer(Partitions);
    res->OrderBy = CloneContainer(OrderBy);
    res->Frame = Frame;
    return res;
}

THoppingWindowSpecPtr THoppingWindowSpec::Clone() const {
    auto res = MakeIntrusive<THoppingWindowSpec>();
    res->TimeExtractor = TimeExtractor->Clone();
    res->Hop = Hop->Clone();
    res->Interval = Interval->Clone();
    res->Delay = Delay->Clone();
    return res;
}

TColumnNode::TColumnNode(TPosition pos, const TString& column, const TString& source)
    : INode(pos)
    , ColumnName(column)
    , Source(source)
{
}

TColumnNode::TColumnNode(TPosition pos, const TNodePtr& column, const TString& source)
    : INode(pos)
    , ColumnExpr(column)
    , Source(source)
{
}

TColumnNode::~TColumnNode()
{
}

bool TColumnNode::IsAsterisk() const {
    return ColumnName == "*";
}

bool TColumnNode::IsArtificial() const {
    return Artificial;
}

const TString* TColumnNode::GetColumnName() const {
    return UseSourceAsColumn ? &Source : (ColumnExpr ? nullptr : &ColumnName);
}

const TString* TColumnNode::GetSourceName() const {
    return UseSourceAsColumn ? &Empty : &Source;
}

bool TColumnNode::DoInit(TContext& ctx, ISource* src) {
    if (src) {
        YQL_ENSURE(!State.Test(ENodeState::Initialized)); /// should be not initialized or Aggregated already invalid
        if (src->ShouldUseSourceAsColumn(*GetSourceName())) {
            if (!IsAsterisk() && IsReliable()) {
                SetUseSourceAsColumn();
            }
        }

        if (GetColumnName()) {
            auto fullName = Source ? DotJoin(Source, *GetColumnName()) : *GetColumnName();
            auto alias = src->GetGroupByColumnAlias(fullName);
            if (alias) {
                ResetColumn(alias, {});
            }
            Artificial = !Source && src->IsExprAlias(*GetColumnName());
        }

        if (!src->AddColumn(ctx, *this)) {
            return false;
        }
        if (GetColumnName()) {
            if (src->GetJoin() && Source) {
                GroupKey = src->IsGroupByColumn(DotJoin(Source, *GetColumnName()));
            } else {
                GroupKey = src->IsGroupByColumn(*GetColumnName()) || src->IsAlias(EExprSeat::GroupBy, *GetColumnName());
            }
        }
    }
    if (IsAsterisk()) {
        Node = AstNode("row");
    } else {
        Node = Y(Reliable && !UseSource ? "Member" : "SqlColumn", "row", ColumnExpr ?
            Y("EvaluateAtom", ColumnExpr) : BuildQuotedAtom(Pos, *GetColumnName()));
        if (UseSource) {
            YQL_ENSURE(Source);
            Node = L(Node, BuildQuotedAtom(Pos, Source));
        }
    }
    return Node->Init(ctx, src);
}

void TColumnNode::SetUseSourceAsColumn() {
    YQL_ENSURE(!State.Test(ENodeState::Initialized)); /// should be not initialized or Aggregated already invalid
    YQL_ENSURE(!IsAsterisk());
    UseSourceAsColumn = true;
}

void TColumnNode::ResetAsReliable() {
    Reliable = true;
}

void TColumnNode::SetAsNotReliable() {
    Reliable = false;
}

void TColumnNode::SetUseSource() {
    UseSource = true;
}

bool TColumnNode::IsUseSourceAsColumn() const {
    return UseSourceAsColumn;
}

bool TColumnNode::IsReliable() const {
    return Reliable;
}

TNodePtr TColumnNode::DoClone() const {
    YQL_ENSURE(!Node, "TColumnNode::Clone: Node should not be initialized");
    auto copy = ColumnExpr ? new TColumnNode(Pos, ColumnExpr, Source) : new TColumnNode(Pos, ColumnName, Source);
    copy->GroupKey = GroupKey;
    copy->Artificial = Artificial;
    copy->Reliable = Reliable;
    copy->UseSource = UseSource;
    copy->UseSourceAsColumn = UseSourceAsColumn;
    return copy;
}

void TColumnNode::DoUpdateState() const {
    State.Set(ENodeState::Const, false);
    State.Set(ENodeState::Aggregated, GroupKey);
    State.Set(ENodeState::AggregationKey, GroupKey);
}

TAstNode* TColumnNode::Translate(TContext& ctx) const {
    return Node->Translate(ctx);
}

void TColumnNode::ResetColumn(const TString& column, const TString& source) {
    YQL_ENSURE(!State.Test(ENodeState::Initialized)); /// should be not initialized
    Reliable = true;
    UseSource = false;
    UseSourceAsColumn = false;
    ColumnName = column;
    ColumnExpr = nullptr;
    Source = source;
}

void TColumnNode::ResetColumn(const TNodePtr& column, const TString& source) {
    YQL_ENSURE(!State.Test(ENodeState::Initialized)); /// should be not initialized
    Reliable = true;
    UseSource = false;
    UseSourceAsColumn = false;
    ColumnName = "";
    ColumnExpr = column;
    Source = source;
}

const TString TColumnNode::Empty;

TNodePtr BuildColumn(TPosition pos, const TString& column, const TString& source) {
    return new TColumnNode(pos, column, source);
}

TNodePtr BuildColumn(TPosition pos, const TNodePtr& column, const TString& source) {
    return new TColumnNode(pos, column, source);
}

TNodePtr BuildColumn(TPosition pos, const TDeferredAtom& column, const TString& source) {
    return column.GetLiteral() ? BuildColumn(pos, *column.GetLiteral(), source) : BuildColumn(pos, column.Build(), source);
}


ITableKeys::ITableKeys(TPosition pos)
    : INode(pos)
{
}

const TString* ITableKeys::GetTableName() const {
    return nullptr;
}

ITableKeys* ITableKeys::GetTableKeys() {
    return this;
}

TAstNode* ITableKeys::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(false);
    Y_UNUSED(ctx);
    return nullptr;
}

bool IAggregation::IsDistinct() const {
    return !DistinctKey.empty();
}

void IAggregation::DoUpdateState() const {
    State.Set(ENodeState::Aggregated, AggMode == EAggregateMode::Normal);
    State.Set(ENodeState::OverWindow, AggMode == EAggregateMode::OverWindow);
}

const TString* IAggregation::GetGenericKey() const {
    return nullptr;
}

void IAggregation::Join(IAggregation*) {
    Y_ABORT_UNLESS(false);
}

const TString& IAggregation::GetName() const {
    return Name;
}

IAggregation::IAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode)
    : INode(pos), Name(name), Func(func), AggMode(aggMode)
{}

TAstNode* IAggregation::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(false);
    Y_UNUSED(ctx);
    return nullptr;
}

TNodePtr IAggregation::AggregationTraits(const TNodePtr& type) const {
    const bool distinct = AggMode == EAggregateMode::Distinct;
    const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos, DistinctKey))) : type;
    return distinct ? Q(Y(Q(Name), GetApply(listType), BuildQuotedAtom(Pos, DistinctKey))): Q(Y(Q(Name), GetApply(listType)));
}

void IAggregation::AddFactoryArguments(TNodePtr& apply) const {
    Y_UNUSED(apply);
}

std::vector<ui32> IAggregation::GetFactoryColumnIndices() const {
    return {0u};
}

TNodePtr IAggregation::WindowTraits(const TNodePtr& type) const {
    YQL_ENSURE(AggMode == EAggregateMode::OverWindow, "Windows traits is unavailable");
    return Q(Y(Q(Name), GetApply(type)));
}

ISource::ISource(TPosition pos)
    : INode(pos)
{
}

ISource::~ISource()
{
}

TSourcePtr ISource::CloneSource() const {
    Y_DEBUG_ABORT_UNLESS(dynamic_cast<ISource*>(Clone().Get()), "Cloned node is no source");
    TSourcePtr result = static_cast<ISource*>(Clone().Get());
    for (auto curFilter: Filters) {
        result->Filters.emplace_back(curFilter->Clone());
    }
    for (int i = 0; i < static_cast<int>(EExprSeat::Max); ++i) {
        result->NamedExprs[i] = CloneContainer(NamedExprs[i]);
    }
    result->FlattenColumns = FlattenColumns;
    return result;
}

bool ISource::IsFake() const {
    return false;
}

void ISource::AllColumns() {
    return;
}

const TColumns* ISource::GetColumns() const {
    return nullptr;
}

void ISource::GetInputTables(TTableList& tableList) const {
    for (auto srcPtr: UsedSources) {
        srcPtr->GetInputTables(tableList);
    }
    return;
}

TMaybe<bool> ISource::AddColumn(TContext& ctx, TColumnNode& column) {
    if (column.IsReliable()) {
        ctx.Error(Pos) << "Source does not allow column references";
    }
    return {};
}

void ISource::FinishColumns() {
}


bool ISource::AddFilter(TContext& ctx, TNodePtr filter) {
    Y_UNUSED(ctx);
    Filters.push_back(filter);
    return true;
}

bool ISource::AddGroupKey(TContext& ctx, const TString& column) {
    if (!GroupKeys.insert(column).second) {
        ctx.Error() << "Duplicate grouping column: " << column;
        return false;
    }
    OrderedGroupKeys.push_back(column);
    return true;
}

bool ISource::AddExpressions(TContext& ctx, const TVector<TNodePtr>& expressions, EExprSeat exprSeat) {
    YQL_ENSURE(exprSeat < EExprSeat::Max);
    TSet<TString> names;
    for (const auto& expr: expressions) {
        const auto& alias = expr->GetLabel();
        const auto& columnNamePtr = expr->GetColumnName();
        if (alias) {
            if (!ExprAliases.emplace(alias).second) {
                ctx.Error(expr->GetPos()) << "Duplicate alias found: " << alias << " in " << exprSeat << " section";
                return false;
            }
            if (names.contains(alias)) {
                ctx.Error(expr->GetPos()) << "Collision between alias and column name: " << alias << " in " << exprSeat << " section";
                return false;
            }
        }
        if (columnNamePtr) {
            const auto& sourceName = *expr->GetSourceName();
            auto columnName = *columnNamePtr;
            if (sourceName) {
                columnName = DotJoin(sourceName, columnName);
            }
            if (!names.emplace(columnName).second) {
                ctx.Error(expr->GetPos()) << "Duplicate column name found: " << columnName << " in " << exprSeat << " section";
                return false;
            }
            if (!alias && ExprAliases.contains(columnName)) {
                ctx.Error(expr->GetPos()) << "Collision between alias and column name: " << columnName << " in " << exprSeat << " section";
                return false;
            }
            if (alias && exprSeat == EExprSeat::GroupBy) {
                auto columnAlias = GroupByColumnAliases.emplace(columnName, alias);
                auto oldAlias = columnAlias.first->second;
                if (columnAlias.second && oldAlias != alias) {
                    ctx.Error(expr->GetPos()) << "Alias for column not same, column: " << columnName <<
                        ", exist alias: " << oldAlias << ", another alias: " << alias;
                    return false;
                }
            }
        }
        Expressions(exprSeat).emplace_back(expr);
    }
    return true;
}

void ISource::SetFlattenByMode(const TString& mode) {
    FlattenMode = mode;
}

void ISource::MarkFlattenColumns() {
    FlattenColumns = true;
}

bool ISource::IsFlattenColumns() const {
    return FlattenColumns;
}

TString ISource::MakeLocalName(const TString& name) {
    auto iter = GenIndexes.find(name);
    if (iter == GenIndexes.end()) {
        iter = GenIndexes.emplace(name, 0).first;
    }
    TStringBuilder str;
    str << name << iter->second;
    ++iter->second;
    return std::move(str);
}

bool ISource::AddAggregation(TContext& ctx, TAggregationPtr aggr) {
    Y_UNUSED(ctx);
    Aggregations.push_back(aggr);
    return true;
}

bool ISource::HasAggregations() const {
    return !Aggregations.empty() || !GroupKeys.empty();
}

void ISource::AddWindowSpecs(TWinSpecs winSpecs) {
    WinSpecs = winSpecs;
}

bool ISource::AddFuncOverWindow(TContext& ctx, TNodePtr expr) {
    Y_UNUSED(ctx);
    Y_UNUSED(expr);
    return false;
}

void ISource::AddTmpWindowColumn(const TString& column) {
    TmpWindowColumns.push_back(column);
}

const TVector<TString>& ISource::GetTmpWindowColumns() const {
    return TmpWindowColumns;
}

void ISource::SetHoppingWindowSpec(THoppingWindowSpecPtr spec) {
    HoppingWindowSpec = spec;
}

THoppingWindowSpecPtr ISource::GetHoppingWindowSpec() const {
    return HoppingWindowSpec;
}

TWindowSpecificationPtr ISource::FindWindowSpecification(TContext& ctx, const TString& windowName) const {
    auto winIter = WinSpecs.find(windowName);
    if (winIter == WinSpecs.end()) {
        ctx.Error(Pos) << "Can't refer to the window specification with name: " << windowName;
        return {};
    }
    auto winSpec = winIter->second;
    if (winSpec->Frame) {
        ctx.Error(Pos) << "Frame that not default is not supported yet for window: " << windowName;
        return {};
    }
    return winSpec;
}

inline TVector<TNodePtr>& ISource::Expressions(EExprSeat exprSeat) {
    return NamedExprs[static_cast<size_t>(exprSeat)];
}

inline const TVector<TNodePtr>& ISource::Expressions(EExprSeat exprSeat) const {
    return NamedExprs[static_cast<size_t>(exprSeat)];
}

inline TNodePtr ISource::AliasOrColumn(const TNodePtr& node, bool withSource) {
    auto result = node->GetLabel();
    if (!result) {
        const auto columnNamePtr = node->GetColumnName();
        YQL_ENSURE(columnNamePtr);
        result = *columnNamePtr;
        if (withSource) {
            const auto sourceNamePtr = node->GetSourceName();
            if (sourceNamePtr) {
                result = DotJoin(*sourceNamePtr, result);
            }
        }
    }
    return node->Q(result);
}

bool ISource::AddAggregationOverWindow(TContext& ctx, const TString& windowName, TAggregationPtr func) {
    if (func->IsDistinct()) {
        ctx.Error(func->GetPos()) << "Aggregation with distinct is not allowed over window: " << windowName;
        return false;
    }
    if (!FindWindowSpecification(ctx, windowName)) {
        return false;
    }
    AggregationOverWindow.emplace(windowName, func);
    return true;
}

bool ISource::AddFuncOverWindow(TContext& ctx, const TString& windowName, TNodePtr func) {
    if (!FindWindowSpecification(ctx, windowName)) {
        return false;
    }
    FuncOverWindow.emplace(windowName, func);
    return true;
}

bool ISource::IsCompositeSource() const {
    return false;
}

bool ISource::IsGroupByColumn(const TString& column) const {
    return GroupKeys.contains(column);
}

bool ISource::IsFlattenByColumns() const {
    return !Expressions(EExprSeat::FlattenBy).empty();
}

bool ISource::IsAlias(EExprSeat exprSeat, const TString& column) const {
    for (const auto& exprNode: Expressions(exprSeat)) {
        const auto& labelName = exprNode->GetLabel();
        if (labelName && labelName == column) {
            return true;
        }
    }
    return false;
}

bool ISource::IsExprAlias(const TString& column) const {
    std::array<EExprSeat, 3> exprSeats = {{EExprSeat::FlattenBy, EExprSeat::GroupBy, EExprSeat::WindowPartitionBy}};
    for (auto seat: exprSeats) {
        if (IsAlias(seat, column)) {
            return true;
        }
    }
    return false;
}

bool ISource::IsExprSeat(EExprSeat exprSeat, EExprType type) const {
    auto expressions = Expressions(exprSeat);
    if (!expressions) {
        return false;
    }
    for (const auto& exprNode: expressions) {
        if (exprNode->GetLabel()) {
            return type == EExprType::WithExpression;
        }
    }
    return type == EExprType::ColumnOnly;
}

TString ISource::GetGroupByColumnAlias(const TString& column) const {
    auto iter = GroupByColumnAliases.find(column);
    if (iter == GroupByColumnAliases.end()) {
        return {};
    }
    return iter->second;
}

const TString* ISource::GetWindowName() const {
    return {};
}

bool ISource::IsCalcOverWindow() const {
    return !AggregationOverWindow.empty() || !FuncOverWindow.empty();
}

bool ISource::IsOverWindowSource() const {
    return !WinSpecs.empty();
}

bool ISource::IsStream() const {
    return false;
}

bool ISource::IsOrdered() const {
    return false;
}

TWriteSettings ISource::GetWriteSettings() const {
    return {};
}

bool ISource::SetSamplingOptions(TContext& ctx,
                                 TPosition pos,
                                 ESampleMode mode,
                                 TNodePtr samplingRate,
                                 TNodePtr samplingSeed) {
    Y_UNUSED(pos);
    Y_UNUSED(mode);
    Y_UNUSED(samplingRate);
    Y_UNUSED(samplingSeed);
    ctx.Error() << "Sampling is only supported for table sources";
    return false;
}

bool ISource::CalculateGroupingHint(TContext& ctx, const TVector<TString>& columns, ui64& hint) const {
    Y_UNUSED(columns);
    Y_UNUSED(hint);
    ctx.Error() << "Source not support grouping hint";
    return false;
}

TNodePtr ISource::BuildFilter(TContext& ctx, const TString& label, const TNodePtr& groundNode) {
    return Filters.empty() ? nullptr : Y(ctx.UseUnordered(*this) ? "OrderedFilter" : "Filter", label, BuildFilterLambda(groundNode));
}

TNodePtr ISource::BuildFilterLambda(const TNodePtr& groundNode) {
    if (Filters.empty()) {
        return BuildLambda(Pos, Y("row"), Y("Bool", Q("true")));
    }
    YQL_ENSURE(Filters[0]->HasState(ENodeState::Initialized));
    TNodePtr filter(Filters[0]);
    for (ui32 i = 1; i < Filters.size(); ++i) {
        YQL_ENSURE(Filters[i]->HasState(ENodeState::Initialized));
        filter = Y("And", filter, Filters[i]);
    }
    filter = Y("Coalesce", filter, Y("Bool", Q("false")));
    if (groundNode) {
        filter = Y("block", Q(L(groundNode, Y("return", filter))));
    }
    return BuildLambda(Pos, Y("row"), filter);
}

TNodePtr ISource::BuildFlattenByColumns(const TString& label) {
    auto columnsList = Y("FlattenByColumns", Q(FlattenMode), label);
    for (const auto& column: Expressions(EExprSeat::FlattenBy)) {
        const auto columnNamePtr = column->GetColumnName();
        YQL_ENSURE(columnNamePtr);
        if (column->GetLabel().empty()) {
            columnsList = L(columnsList, Q(*columnNamePtr));
        } else {
            columnsList = L(columnsList, Q(Y(Q(*columnNamePtr), Q(column->GetLabel()))));
        }
    }
    return Y(Y("let", "res", columnsList));
}

TNodePtr ISource::BuildFlattenColumns(const TString& label) {
    return Y(Y("let", "res", Y("Just", Y("FlattenStructs", label))));
}

TNodePtr ISource::BuildPreaggregatedMap(TContext& ctx) {
    const TColumns* columnsPtr = GetColumns();
    if (!columnsPtr) {
        ctx.Error(GetPos()) << "Missed columns for preaggregated map";
        return nullptr;
    }
    auto structObj = BuildAtom(Pos, "row", TNodeFlags::Default);
    for (const auto& exprNode: Expressions(EExprSeat::GroupBy)) {
        const auto name = exprNode->GetLabel();
        if (name) {
            structObj = Y("ForceRemoveMember", structObj, Q(name));
            structObj = Y("AddMember", structObj, Q(name), exprNode);
        }
    }
    auto block = Y("AsList", structObj);
    return block;
}

TNodePtr ISource::BuildPrewindowMap(TContext& ctx, const TNodePtr& groundNode) {
    Y_UNUSED(ctx);
    auto feed = BuildAtom(Pos, "row", TNodeFlags::Default);
    for (const auto& exprNode: Expressions(EExprSeat::WindowPartitionBy)) {
        const auto name = exprNode->GetLabel();
        if (name) {
            feed = Y("AddMember", feed, Q(name), GroundWithExpr(groundNode, exprNode));
        }
    }
    return Y(ctx.UseUnordered(*this) ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos, Y("row"), Y("AsList", feed)));
}

TNodePtr ISource::BuildAggregation(const TString& label) {
    if (GroupKeys.empty() && Aggregations.empty() && !IsCompositeSource()) {
        return nullptr;
    }

    auto keysTuple = Y();
    for (const auto& key: GroupKeys) {
        keysTuple = L(keysTuple, BuildQuotedAtom(Pos, key));
    }

    std::map<std::pair<bool, TString>, std::vector<IAggregation*>> genericAggrs;
    for (const auto& aggr: Aggregations) {
        if (const auto key = aggr->GetGenericKey()) {
            genericAggrs[{aggr->IsDistinct(), *key}].emplace_back(aggr.Get());
        }
    }

    for (const auto& aggr : genericAggrs) {
        for (size_t i = 1U; i < aggr.second.size(); ++i) {
            aggr.second.front()->Join(aggr.second[i]);
        }
    }

    const auto listType = Y("TypeOf", label);
    auto aggrArgs = Y();
    for (const auto& aggr: Aggregations) {
        if (const auto traits = aggr->AggregationTraits(listType))
            aggrArgs = L(aggrArgs, traits);
    }

    if (HoppingWindowSpec) {
        auto hoppingTraits = Y(
            "HoppingTraits",
            Y("ListItemType", listType),
            BuildLambda(Pos, Y("row"), HoppingWindowSpec->TimeExtractor),
            HoppingWindowSpec->Hop,
            HoppingWindowSpec->Interval,
            HoppingWindowSpec->Delay,
            Q("False"),
            Q("v1"));

        return Y("Aggregate", label, Q(keysTuple), Q(aggrArgs),
            Q(Y(Q(Y(BuildQuotedAtom(Pos, "hopping"), hoppingTraits)))));
    }

    return Y("Aggregate", label, Q(keysTuple), Q(aggrArgs));
}

TMaybe<TString> ISource::FindColumnMistype(const TString& name) const {
    auto result = FindMistypeIn(GroupKeys, name);
    return result ? result : FindMistypeIn(ExprAliases, name);
}

void ISource::AddDependentSource(ISource* usedSource) {
    UsedSources.push_back(usedSource);
}

/// \todo fill it
struct TWinFrame {
};

struct TWinPartition {
    TString ParentLabel;
    size_t Id = 0;
    TVector<size_t> FrameIds;
    TVector<TSortSpecificationPtr> OrderBy;
    TVector<TNodePtr> Partitions;
};

/// \todo use few levels of grouping (got from group by cube, etc)
class WindowFuncSupp {
public:
    struct EvalOverWindow {
        TVector<TAggregationPtr> Aggregations;
        TVector<TNodePtr> Functions;
    };

    size_t GetWindowByName(const TString& windowName) {
        auto iter = WindowMap.find(windowName);
        return iter != WindowMap.end() ? iter->second : 0;
    }
    size_t CreateWindowBySpec(const TString& windowName, const TWindowSpecificationPtr& winSpec) {
        Y_UNUSED(windowName);
        auto curPartitions = winSpec->Partitions;
        auto curOrderBy = winSpec->OrderBy;
        auto partition = std::find_if(Partitions.begin(), Partitions.end(), [&curPartitions, &curOrderBy](const TWinPartition& other) {
            /// \todo this compare is too strong;
            if (curPartitions != other.Partitions) {
                return false;
            }
            if (curOrderBy.size() != other.OrderBy.size()) {
                return false;
            }
            for (unsigned i = 0; i < curOrderBy.size(); ++i) {
                // failed in common case
                if (curOrderBy[i]->OrderExpr != other.OrderBy[i]->OrderExpr) {
                    return false;
                }
                if (curOrderBy[i]->Ascending != other.OrderBy[i]->Ascending) {
                    return false;
                }
            }
            return true;
        });
        if (partition == Partitions.end()) {
            TWinPartition newPartition;
            newPartition.Partitions = curPartitions;
            newPartition.OrderBy = curOrderBy;
            Partitions.emplace_back(newPartition);
            partition = Partitions.end() - 1;
        }
        /// \todo add smart frame search and creation
        auto frame = partition->FrameIds.begin();
        if (frame == partition->FrameIds.end()) {
            YQL_ENSURE(!winSpec->Frame, "Supported only default frame yet!");
            Evals.push_back({});
            const size_t curEval = Evals.size();
            partition->FrameIds.push_back(curEval);
            frame = partition->FrameIds.end() - 1;
        }
        return *frame;
    }
    void AddAggregationFunc(size_t windowId, TAggregationPtr func) {
        Evals[windowId-1].Aggregations.push_back(func);
    }

    void AddSimpleFunc(size_t windowId, TNodePtr func) {
        Evals[windowId-1].Functions.push_back(func);
    }

    const TVector<TWinPartition>& GetPartitions() {
        return Partitions;
    }
    const EvalOverWindow& GetEvals(size_t frameId) {
        YQL_ENSURE(frameId && frameId <= Evals.size());
        return Evals[frameId-1];
    }
    TNodePtr BuildFrame(TPosition pos, size_t frameId) {
        Y_UNUSED(frameId);
        /// \todo support not default frame
        return BuildLiteralVoid(pos);
    }

private:
    TVector<TWinPartition> Partitions;
    TMap<TString, size_t> WindowMap;
    TVector<EvalOverWindow> Evals;
};

TNodePtr ISource::BuildCalcOverWindow(TContext& ctx, const TString& label, const TNodePtr& ground) {
    if (AggregationOverWindow.empty() && FuncOverWindow.empty()) {
        return {};
    }

    WindowFuncSupp winSupp;
    for (auto iter: AggregationOverWindow) {
        auto windowId = winSupp.GetWindowByName(iter.first);
        if (!windowId) {
            windowId = winSupp.CreateWindowBySpec(iter.first, WinSpecs.at(iter.first));
        }
        winSupp.AddAggregationFunc(windowId, iter.second);
    }
    for (auto iter: FuncOverWindow) {
        auto windowId = winSupp.GetWindowByName(iter.first);
        if (!windowId) {
            windowId = winSupp.CreateWindowBySpec(iter.first, WinSpecs.at(iter.first));
        }
        winSupp.AddSimpleFunc(windowId, iter.second);
    }

    auto partitions = winSupp.GetPartitions();
    const bool onePartition = partitions.size() == 1;
    const auto useLabel = onePartition ? label : "partitioning";
    const auto listType = Y("TypeOf", useLabel);
    auto framesProcess = Y();
    auto resultNode = onePartition ? Y() : Y(Y("let", "partitioning", label));
    for (auto partition: partitions) {
        if (!partition.ParentLabel.empty()) {
            ctx.Error(GetPos()) << "Dependent partition for Window function unsupported yet!";
            return nullptr;
        }
        auto keysTuple = Y();
        for (const auto& key: partition.Partitions) {
            keysTuple = L(keysTuple, AliasOrColumn(key, GetJoin()));
        }
        auto frames = Y();
        for (auto frameId: partition.FrameIds) {
            auto callOnFrame = Y("WinOnRows", winSupp.BuildFrame(ctx.Pos(), frameId));
            const auto& evals = winSupp.GetEvals(frameId);
            for (auto eval: evals.Aggregations) {
                if (!eval->IsOverWindow()) {
                    ctx.Error(eval->GetPos()) << "Aggregation over window is not supported for function: " << eval->GetName();
                    return nullptr;
                }
                auto winTraits = eval->WindowTraits(listType);
                callOnFrame = L(callOnFrame, winTraits);
            }
            for (auto eval: evals.Functions) {
                auto winSpec = eval->WindowSpecFunc(listType);
                callOnFrame = L(callOnFrame, winSpec);
            }
            /// \todo some smart frame building  not "WinOnRows" hardcode
            frames = L(frames, callOnFrame);
        }
        auto sortSpec = partition.OrderBy.empty() ? BuildLiteralVoid(ctx.Pos()) : BuildSortSpec(partition.OrderBy, useLabel, ground, true);
        framesProcess = Y("CalcOverWindow", useLabel, Q(keysTuple), sortSpec, Q(frames));
        if (!onePartition) {
            resultNode = L(resultNode, Y("let", "partitioning", framesProcess));
        }
    }
    if (onePartition) {
        return framesProcess;
    } else {
        return Y("block", Q(L(resultNode, Y("return", "partitioning"))));
    }
}

TNodePtr ISource::BuildSort(TContext& ctx, const TString& label) {
    Y_UNUSED(ctx);
    Y_UNUSED(label);
    return nullptr;
}

IJoin* ISource::GetJoin() {
    return nullptr;
}

ISource* ISource::GetCompositeSource() {
    return nullptr;
}

bool ISource::IsSelect() const {
    return true;
}

bool ISource::IsTableSource() const {
    return false;
}

bool ISource::ShouldUseSourceAsColumn(const TString& source) {
    Y_UNUSED(source);
    return false;
}

bool ISource::IsJoinKeysInitializing() const {
    return false;
}

bool ISource::DoInit(TContext& ctx, ISource* src) {
    for (auto& column: Expressions(EExprSeat::FlattenBy)) {
        if (!column->Init(ctx, this)) {
            return false;
        }
    }

    if (IsFlattenColumns() && src) {
        src->AllColumns();
    }

    return true;
}

bool ISource::InitFilters(TContext& ctx) {
    for (auto& filter: Filters) {
        if (!filter->Init(ctx, this)) {
            return false;
        }
        if (filter->IsAggregated() && !filter->IsConstant() && !filter->HasState(ENodeState::AggregationKey)) {
            ctx.Error(filter->GetPos()) << "Can not use aggregated values in filtering";
            return false;
        }
    }
    return true;
}

TAstNode* ISource::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(false);
    Y_UNUSED(ctx);
    return nullptr;
}

void ISource::FillSortParts(const TVector<TSortSpecificationPtr>& orderBy, TNodePtr& sortDirection, TNodePtr& sortKeySelector) {
    TNodePtr expr;
    if (orderBy.empty()) {
        YQL_ENSURE(!sortKeySelector);
        sortDirection = sortKeySelector = Y("Void");
        return;
    } else if (orderBy.size() == 1) {
        auto& sortSpec = orderBy.front();
        expr = Y("EnsurePersistable", sortSpec->OrderExpr);
        sortDirection = Y("Bool", Q(sortSpec->Ascending ? "true" : "false"));
    } else {
        auto exprList = Y();
        sortDirection = Y();
        for (const auto& sortSpec: orderBy) {
            const auto asc = sortSpec->Ascending;
            sortDirection = L(sortDirection, Y("Bool", Q(asc ? "true" : "false")));
            exprList = L(exprList, Y("EnsurePersistable", sortSpec->OrderExpr));
        }
        sortDirection = Q(sortDirection);
        expr = Q(exprList);
    }
    expr = sortKeySelector ? expr->Y("block", expr->Q(expr->L(sortKeySelector, expr->Y("return", expr)))) : expr;
    sortKeySelector = BuildLambda(Pos, Y("row"), expr);
}

TNodePtr ISource::BuildSortSpec(const TVector<TSortSpecificationPtr>& orderBy, const TString& label, const TNodePtr& ground, bool traits) {
    YQL_ENSURE(!orderBy.empty());
    TNodePtr dirsNode;
    auto keySelectorNode = ground;
    FillSortParts(orderBy, dirsNode, keySelectorNode);
    if (traits) {
        return Y("SortTraits", Y("TypeOf", label), dirsNode, keySelectorNode);
    } else {
        return Y("Sort", label, dirsNode, keySelectorNode);
    }
}

IJoin::IJoin(TPosition pos)
    : ISource(pos)
{
}

IJoin::~IJoin()
{
}

IJoin* IJoin::GetJoin() {
    return this;
}

bool TryStringContent(const TString& str, TString& result, ui32& flags, TString& error, TPosition& pos) {
    error.clear();
    result.clear();

    bool doubleQuoted = (str.StartsWith('"') && str.EndsWith('"'));
    bool singleQuoted = !doubleQuoted && (str.StartsWith('\'') && str.EndsWith('\''));

    if (str.size() >= 2 && (doubleQuoted || singleQuoted)) {
        flags = TNodeFlags::ArbitraryContent;
        char quoteChar = doubleQuoted ? '"' : '\'';
        size_t readBytes = 0;
        TStringBuf atom(str);
        TStringOutput sout(result);
        atom.Skip(1);
        result.reserve(str.size());

        auto unescapeResult = UnescapeArbitraryAtom(atom, quoteChar, &sout, &readBytes);
        if (unescapeResult != EUnescapeResult::OK) {
            TTextWalker walker(pos);
            walker.Advance(atom.Trunc(readBytes));
            error = UnescapeResultToString(unescapeResult);
            return false;
        }
    } else if (str.size() >= 4 && str.StartsWith("@@") && str.EndsWith("@@")) {
        flags = TNodeFlags::MultilineContent;
        TString s = str.substr(2, str.length() - 4);
        SubstGlobal(s, "@@@@", "@@");
        result.swap(s);
    } else {
        flags = TNodeFlags::Default;
        result = str;
    }
    return true;
}

TString StringContent(TContext& ctx, const TString& str) {
    ui32 flags = 0;
    TString result;
    TString error;
    TPosition pos;

    if (!TryStringContent(str, result, flags, error, pos)) {
        ctx.Error(pos) << "Failed to parse string literal: " << error;
        return {};
    }
    return result;
}

TString IdContent(TContext& ctx, const TString& s) {
    YQL_ENSURE(!s.empty(), "Empty identifier not expected");
    if (!s.StartsWith('[') && !s.StartsWith('`')) {
        return s;
    }
    auto endSym = s.StartsWith('[') ? ']' : '`';
    if (s.size() < 2 || !s.EndsWith(endSym)) {
        ctx.Error() << "The identifier that starts with: '" << s[0] << "' should ends with: '" << endSym << "'";
        return {};
    }
    size_t skipSymbols = 1;

    /// @TODO: temporary back compatibility case
    if (s.StartsWith('[') && s[1] == '"') {
        ctx.Warning(ctx.Pos(), TIssuesIds::YQL_DEPRECATED_DOUBLE_QUOTE_IN_BRACKETS) <<
            "The use of double quotes in the identifier in square brackets is deprecated."
            " Either simply remove the double quotes or use backticks."
            " If you need quotes they can be escaped by '\\'.";
        if (s.size() < 4 || s[s.size() - 2] != '"') {
            ctx.Error() << "Missed closed quote for identifier, either remove double quote after '[', "
                " or put double quote before ']'";
            return {};
        }
        endSym = '"';
        skipSymbols += 1;
    }

    TStringBuf atom(s.data() + skipSymbols, s.size() - 2 * skipSymbols + 1);
    TString unescapedStr;
    TStringOutput sout(unescapedStr);
    unescapedStr.reserve(s.size());

    size_t readBytes = 0;
    TPosition pos = ctx.Pos();
    pos.Column += skipSymbols - 1;

    auto unescapeResult = UnescapeArbitraryAtom(atom, endSym, &sout, &readBytes);
    if (unescapeResult != EUnescapeResult::OK) {
        TTextWalker walker(pos);
        walker.Advance(atom.Trunc(readBytes));
        ctx.Error(pos) << "Cannot parse broken identifier: " << UnescapeResultToString(unescapeResult);
        return {};
    }

    if (readBytes != atom.size()) {
        ctx.Error() << "The identifier not parsed completely";
        return {};
    }

    return unescapedStr;
}

namespace {
class TInvalidLiteralNode final: public INode {
public:
    TInvalidLiteralNode(TPosition pos)
        : INode(pos)
    {
    }

    bool DoInit(TContext& ctx, ISource* source) override {
        Y_UNUSED(ctx);
        Y_UNUSED(source);
        return false;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_UNUSED(ctx);
        return nullptr;
    }

    TPtr DoClone() const override {
        return {};
    }
};

}

TLiteralNode::TLiteralNode(TPosition pos, bool isNull)
    : TAstListNode(pos)
    , Null(isNull)
    , Void(!isNull)
{
    Add(isNull ? "Null" : "Void");
}

TLiteralNode::TLiteralNode(TPosition pos, const TString& type, const TString& value)
    : TAstListNode(pos)
    , Null(false)
    , Void(false)
    , Type(type)
    , Value(value)
{
    Add(Type, BuildQuotedAtom(Pos, Value));
}

TLiteralNode::TLiteralNode(TPosition pos, const TString& value, ui32 nodeFlags)
    : TAstListNode(pos)
    , Null(false)
    , Void(false)
    , Type("String")
    , Value(value)
{
    Add(Type, BuildQuotedAtom(pos, Value, nodeFlags));
}

bool TLiteralNode::IsNull() const {
    return Null;
}

const TString* TLiteralNode::GetLiteral(const TString& type) const {
    return type == Type ? &Value : nullptr;
}

void TLiteralNode::DoUpdateState() const {
    State.Set(ENodeState::Const);
}

TNodePtr TLiteralNode::DoClone() const {
    auto res = (Null || Void) ? MakeIntrusive<TLiteralNode>(Pos, Null) : MakeIntrusive<TLiteralNode>(Pos, Type, Value);
    res->Nodes = Nodes;
    return res;
}

template<typename T>
TLiteralNumberNode<T>::TLiteralNumberNode(TPosition pos, const TString& type, const TString& value)
    : TLiteralNode(pos, type, value)
{}

template<typename T>
TNodePtr TLiteralNumberNode<T>::DoClone() const {
    return new TLiteralNumberNode<T>(Pos, Type, Value);
}

template<typename T>
bool TLiteralNumberNode<T>::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(src);
    T val;
    if (!TryFromString(Value, val)) {
        ctx.Error(Pos) << "Failed to convert string: " << Value << " to " << Type << " value";
        return false;
    }
    return true;
}

template<typename T>
bool TLiteralNumberNode<T>::IsIntegerLiteral() const {
    return std::numeric_limits<T>::is_integer;
}

template class TLiteralNumberNode<i32>;
template class TLiteralNumberNode<i64>;
template class TLiteralNumberNode<ui32>;
template class TLiteralNumberNode<ui64>;
template class TLiteralNumberNode<float>;
template class TLiteralNumberNode<double>;
template class TLiteralNumberNode<ui8>;
template class TLiteralNumberNode<i8>;
template class TLiteralNumberNode<ui16>;
template class TLiteralNumberNode<i16>;

TNodePtr BuildLiteralNull(TPosition pos) {
    return new TLiteralNode(pos, true);
}

TNodePtr BuildLiteralVoid(TPosition pos) {
    return new TLiteralNode(pos, false);
}

TNodePtr BuildLiteralSmartString(TContext& ctx, const TString& value) {

    TString unescaped;
    TString error;
    TPosition pos = ctx.Pos();
    ui32 flags = 0;

    if (TryStringContent(value, unescaped, flags, error, pos)) {
        return new TLiteralNode(ctx.Pos(), unescaped, flags);
    } else {
        ctx.Error(pos) << "Failed to parse string literal: " << error;
        return new TInvalidLiteralNode(ctx.Pos());
    }
}

TNodePtr BuildLiteralRawString(TPosition pos, const TString& value) {
    return new TLiteralNode(pos, "String", value);
}

TNodePtr BuildLiteralBool(TPosition pos, const TString& value) {
    return new TLiteralNode(pos, "Bool", value);
}

TNodePtr BuildEmptyAction(TPosition pos) {
    TNodePtr params = new TAstListNodeImpl(pos);
    TNodePtr arg = new TAstAtomNodeImpl(pos, "x", TNodeFlags::Default);
    params->Add(arg);
    return BuildLambda(pos, params, arg);
}

TDeferredAtom::TDeferredAtom()
{}

TDeferredAtom::TDeferredAtom(TPosition pos, const TString& str)
{
    Node = BuildQuotedAtom(pos, str);
    Explicit = str;
    Repr = str;
}

TDeferredAtom::TDeferredAtom(TNodePtr node, TContext& ctx)
{
    Node = node;
    Repr = ctx.MakeName("DeferredAtom");
}

const TString* TDeferredAtom::GetLiteral() const {
    return Explicit.Get();
}

TNodePtr TDeferredAtom::Build() const {
    return Node;
}

TString TDeferredAtom::GetRepr() const {
    return Repr;
}

bool TDeferredAtom::Empty() const {
    return !Node || Repr.empty();
}

TTupleNode::TTupleNode(TPosition pos, const TVector<TNodePtr>& exprs)
    : TAstListNode(pos)
    , Exprs(exprs)
{}

bool TTupleNode::IsEmpty() const {
    return Exprs.empty();
}

const TVector<TNodePtr>& TTupleNode::Elements() const {
    return Exprs;
}

bool TTupleNode::DoInit(TContext& ctx, ISource* src) {
    auto node(Y());
    for (auto& expr: Exprs) {
        if (expr->GetLabel()) {
            ctx.Error(expr->GetPos()) << "Tuple does not allow named members";
            return false;
        }
        node = L(node, expr);
    }
    Add("quote", node);
    return TAstListNode::DoInit(ctx, src);
}

size_t TTupleNode::GetTupleSize() const {
    return Exprs.size();
}

TNodePtr TTupleNode::GetTupleElement(size_t index) const {
    return Exprs[index];
}

TNodePtr TTupleNode::DoClone() const {
    return new TTupleNode(Pos, CloneContainer(Exprs));
}

TNodePtr BuildTuple(TPosition pos, const TVector<TNodePtr>& exprs) {
    return new TTupleNode(pos, exprs);
}

TStructNode::TStructNode(TPosition pos, const TVector<TNodePtr>& exprs)
    : TAstListNode(pos)
    , Exprs(exprs)
{}

bool TStructNode::DoInit(TContext& ctx, ISource* src) {
    Nodes.push_back(BuildAtom(Pos, "AsStruct", TNodeFlags::Default));
    for (const auto& expr : Exprs) {
        const auto& label = expr->GetLabel();
        if (!label) {
            ctx.Error(expr->GetPos()) << "Structure does not allow anonymous members";
            return false;
        }
        Nodes.push_back(Q(Y(BuildQuotedAtom(expr->GetPos(), label), expr)));
    }
    return TAstListNode::DoInit(ctx, src);
}

TNodePtr TStructNode::DoClone() const {
    return new TStructNode(Pos, CloneContainer(Exprs));
}

TNodePtr BuildStructure(TPosition pos, const TVector<TNodePtr>& exprs) {
    return new TStructNode(pos, exprs);
}

TListOfNamedNodes::TListOfNamedNodes(TPosition pos, TVector<TNodePtr>&& exprs)
    : INode(pos)
    , Exprs(std::move(exprs))
{}

TVector<TNodePtr>* TListOfNamedNodes::ContentListPtr() {
    return &Exprs;
}

TAstNode* TListOfNamedNodes::Translate(TContext& ctx) const {
    YQL_ENSURE(!"Unexpected usage");
    Y_UNUSED(ctx);
    return nullptr;
}

TNodePtr TListOfNamedNodes::DoClone() const {
    return {};
}

TNodePtr BuildListOfNamedNodes(TPosition pos, TVector<TNodePtr>&& exprs) {
    return new TListOfNamedNodes(pos, std::move(exprs));
}

const char* const TArgPlaceholderNode::ProcessRows = "$ROWS";
const char* const TArgPlaceholderNode::ProcessRow = "$ROW";

TArgPlaceholderNode::TArgPlaceholderNode(TPosition pos, const TString &name) :
    INode(pos),
    Name(name)
{
}

bool TArgPlaceholderNode::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(src);
    ctx.Error(Pos) << Name << " can't be used as a part of expression.";
    return false;
}

TAstNode* TArgPlaceholderNode::Translate(TContext& ctx) const {
    Y_UNUSED(ctx);
    return nullptr;
}

TString TArgPlaceholderNode::GetName() const {
    return Name;
}

TNodePtr TArgPlaceholderNode::DoClone() const {
    return {};
}

TNodePtr BuildArgPlaceholder(TPosition pos, const TString& name) {
    return new TArgPlaceholderNode(pos, name);
}

class TAccessNode: public INode {
public:
    TAccessNode(TPosition pos, const TVector<TIdPart>& ids, bool isLookup)
        : INode(pos)
        , Ids(ids)
        , IsLookup(isLookup)
        , ColumnOnly(false)
        , IsColumnRequired(false)
    {
        Y_DEBUG_ABORT_UNLESS(Ids.size() > 1);
        Y_DEBUG_ABORT_UNLESS(Ids[0].Expr);
        auto column = dynamic_cast<TColumnNode*>(Ids[0].Expr.Get());
        if (column) {
            ui32 idx = 1;
            TString source;
            if (Ids.size() > 2) {
                source = Ids[idx].Name;
                ++idx;
            }

            ColumnOnly = !IsLookup && Ids.size() < 4;
            if (ColumnOnly && Ids[idx].Expr) {
                column->ResetColumn(Ids[idx].Expr, source);
            } else {
                column->ResetColumn(Ids[idx].Name, source);
            }
        }
    }

    void AssumeColumn() override {
        IsColumnRequired = true;
    }

    TMaybe<std::pair<TString, TString>> TryMakeClusterAndTable(TContext& ctx, bool& hasErrors) {
        hasErrors = false;
        if (!ColumnOnly) {
            return Nothing();
        }

        ui32 idx = 1;
        TString cluster;
        if (Ids.size() > 2) {
            cluster = Ids[idx].Name;
            ++idx;
        }

        if (cluster.StartsWith('$')) {
            return Nothing();
        }

        TString normalizedClusterName;
        if (!cluster.empty() && !ctx.GetClusterProvider(cluster, normalizedClusterName)) {
            hasErrors = true;
            ctx.Error() << "Unknown cluster: " << cluster;
            return Nothing();
        }

        auto tableName = Ids[idx].Name;
        if (tableName.empty()) {
            return Nothing();
        }

        return std::make_pair(normalizedClusterName, tableName);
    }

    TSourcePtr TryMakeSource(TContext& ctx, const TString& view, bool& hasErrors) {
        auto clusterAndTable = TryMakeClusterAndTable(ctx, hasErrors);
        if (!clusterAndTable) {
            return nullptr;
        }

        auto cluster = clusterAndTable->first.empty() ? ctx.CurrCluster : clusterAndTable->first;
        TNodePtr tableKey = BuildTableKey(GetPos(), cluster, TDeferredAtom(GetPos(), clusterAndTable->second), view);
        TTableRef table(ctx.MakeName("table"), cluster, tableKey);
        table.Options = BuildInputOptions(GetPos(), GetContextHints(ctx));
        return BuildTableSource(GetPos(), table, false);
    }

    TMaybe<TString> TryMakeTable() {
        if (!ColumnOnly) {
            return Nothing();
        }

        ui32 idx = 1;
        if (Ids.size() > 2) {
            return Nothing();
        }

        return Ids[idx].Name;
    }

    const TString* GetColumnName() const override {
        return ColumnOnly ? Ids[0].Expr->GetColumnName() : nullptr;
    }

    const TString* GetSourceName() const override {
        return Ids[0].Expr->GetSourceName();
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto expr = Ids[0].Expr;
        const TPosition pos(expr->GetPos());
        if (expr->IsAsterisk()) {
            ctx.Error(pos) << "Asterisk column does not allow any access";
            return false;
        }
        if (!expr->Init(ctx, src)) {
            return false;
        }
        for (auto& id: Ids) {
            if (id.Expr && !id.Expr->Init(ctx, src)) {
                return false;
            }
        }
        ui32 idx = 1;
        auto column = dynamic_cast<TColumnNode*>(expr.Get());
        if (column) {
            const bool useSourceAsColumn = column->IsUseSourceAsColumn();
            ColumnOnly &= !useSourceAsColumn;
            if (IsColumnRequired && !ColumnOnly) {
                ctx.Error(pos) << "Please use a full form (corellation.struct.field) or an alias (struct.field as alias) to access struct's field in the GROUP BY";
                return false;
            }

            if (Ids.size() > 2) {
                if (!CheckColumnId(pos, ctx, Ids[idx], ColumnOnly ? "Correlation" : "Column", true)) {
                    return false;
                }
                ++idx;
            }
            if (!useSourceAsColumn) {
                if (!IsLookup && !CheckColumnId(pos, ctx, Ids[idx], ColumnOnly ? "Column" : "Member", false)) {
                    return false;
                }
                ++idx;
            }
        }
        for (; idx < Ids.size(); ++idx) {
            const auto& id = Ids[idx];
            if (!id.Name.empty()) {
                expr = Y("SqlAccess", Q("struct"), expr, id.Expr ? Y("EvaluateAtom", id.Expr) : BuildQuotedAtom(Pos, id.Name));
                AccessOpName = "AccessStructMember";
            } else if (id.Expr) {
                expr = Y("SqlAccess", Q("dict"), expr, id.Expr);
                AccessOpName = "AccessDictMember";
            } else if (id.Pos >= 0) {
                expr = Y("SqlAccess", Q("tuple"), expr, Q(ToString(id.Pos)));
                AccessOpName = "AccessTupleElement";
            } else {
                continue;
            }

            if (ctx.PragmaYsonAutoConvert || ctx.PragmaYsonStrict) {
                auto ysonOptions = Y();
                if (ctx.PragmaYsonAutoConvert) {
                    ysonOptions->Add(BuildQuotedAtom(Pos, "yson_auto_convert"));
                }
                if (ctx.PragmaYsonStrict) {
                    ysonOptions->Add(BuildQuotedAtom(Pos, "yson_strict"));
                }
                expr->Add(Q(ysonOptions));
            }
        }
        Node = expr;
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node);
        return Node->Translate(ctx);
    }

    TPtr DoClone() const override {
        YQL_ENSURE(!Node, "TAccessNode::Clone: Node should not be initialized");
        TVector<TIdPart> cloneIds;
        cloneIds.reserve(Ids.size());
        for (const auto& id: Ids) {
            cloneIds.emplace_back(id.Clone());
        }
        auto copy = new TAccessNode(Pos, cloneIds, IsLookup);
        copy->ColumnOnly = ColumnOnly;
        return copy;
    }

protected:
    void DoUpdateState() const override {
        State.Set(ENodeState::Const, Ids[0].Expr->IsConstant());
        State.Set(ENodeState::Aggregated, Ids[0].Expr->IsAggregated());
        State.Set(ENodeState::AggregationKey, Ids[0].Expr->HasState(ENodeState::AggregationKey));
        State.Set(ENodeState::OverWindow, Ids[0].Expr->IsOverWindow());
    }

    bool CheckColumnId(TPosition pos, TContext& ctx, const TIdPart& id, const TString& where, bool checkLookup) {
        if (id.Name.empty()) {
            ctx.Error(pos) << where << " name can not be empty";
            return false;
        }
        if (id.Pos >= 0) {
            ctx.Error(pos) << where << " name does not allow element selection";
            return false;
        }
        if (checkLookup && id.Expr) {
            ctx.Error(pos) << where << " name does not allow dict lookup";
            return false;
        }
        return true;
    }

    TString GetOpName() const override {
        return AccessOpName;
    }

private:
    TNodePtr Node;
    TVector<TIdPart> Ids;
    bool IsLookup;
    bool ColumnOnly;
    bool IsColumnRequired;
    TString AccessOpName;
};

TNodePtr BuildAccess(TPosition pos, const TVector<INode::TIdPart>& ids, bool isLookup) {
    return new TAccessNode(pos, ids, isLookup);
}

class TBindNode: public TAstListNode {
public:
    TBindNode(TPosition pos, const TString& module, const TString& alias)
        : TAstListNode(pos)
    {
        Add("bind", AstNode(module), BuildQuotedAtom(pos, alias));
    }

    TPtr DoClone() const final {
        return {};
    }
};

TNodePtr BuildBind(TPosition pos, const TString& module, const TString& alias) {
    return new TBindNode(pos, module, alias);
}

class TLambdaNode: public TAstListNode {
public:
    TLambdaNode(TPosition pos, TNodePtr params, TNodePtr body, const TString& resName)
        : TAstListNode(pos)
    {
        if (!resName.empty()) {
            body = Y("block", Q(L(body, Y("return", resName))));
        }
        Add("lambda", Q(params), body);
    }

    TPtr DoClone() const final {
        return {};
    }
};

TNodePtr BuildLambda(TPosition pos, TNodePtr params, TNodePtr body, const TString& resName) {
    return new TLambdaNode(pos, params, body, resName);
}

template <bool Bit>
class TCastNode: public TAstListNode {
public:
    TCastNode(TPosition pos, TNodePtr expr, const TString& typeName, const TString& paramOne, const TString& paramTwo)
        : TAstListNode(pos)
        , Expr(expr)
        , NormalizedTypeName(TypeByAlias(typeName))
        , ParamOne(paramOne)
        , ParamTwo(paramTwo)
    {}

    const TString* GetSourceName() const override {
        return Expr->GetSourceName();
    }

    TString GetOpName() const override {
        return Bit ? "BitCast" : "Cast";
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, Expr->IsConstant());
        State.Set(ENodeState::Aggregated, Expr->IsAggregated());
        State.Set(ENodeState::OverWindow, Expr->IsOverWindow());
    }

    TPtr DoClone() const final {
        return new TCastNode(Pos, Expr->Clone(), NormalizedTypeName, ParamOne, ParamTwo);
    }

    bool DoInit(TContext& ctx, ISource* src) override;
private:
    TNodePtr Expr;
    const TString NormalizedTypeName;
    const TString ParamOne, ParamTwo;
};

template <>
bool TCastNode<false>::DoInit(TContext& ctx, ISource* src) {
    if (Expr->IsNull()) {
        if (ParamOne.empty() && ParamTwo.empty()) {
            Add("Nothing", Y("OptionalType", Y("DataType", Q(NormalizedTypeName))));
        } else if (ParamTwo.empty()) {
            Add("Nothing", Y("OptionalType", Y("DataType", Q(NormalizedTypeName), Q(ParamOne))));
        } else {
            Add("Nothing", Y("OptionalType", Y("DataType", Q(NormalizedTypeName), Q(ParamOne), Q(ParamTwo))));
        }
    } else {
        if (ParamOne.empty() && ParamTwo.empty()) {
            Add("Cast", Expr, Q(NormalizedTypeName));
        } else if (ParamTwo.empty()) {
            Add("Cast", Expr, Q(NormalizedTypeName), Q(ParamOne));
        } else {
            Add("Cast", Expr, Q(NormalizedTypeName), Q(ParamOne), Q(ParamTwo));
        }
    }
    return TAstListNode::DoInit(ctx, src);
}

template <>
bool TCastNode<true>::DoInit(TContext& ctx, ISource* src) {
    if (Expr->IsNull()) {
        if (ParamOne.empty() && ParamTwo.empty()) {
            Add("Nothing", Y("OptionalType", Y("DataType", Q(NormalizedTypeName))));
        } else if (ParamTwo.empty()) {
            Add("Nothing", Y("OptionalType", Y("DataType", Q(NormalizedTypeName), Q(ParamOne))));
        } else {
            Add("Nothing", Y("OptionalType", Y("DataType", Q(NormalizedTypeName), Q(ParamOne), Q(ParamTwo))));
        }
    } else {
        if (ParamOne.empty() && ParamTwo.empty()) {
            Add("BitCast", Expr, Q(NormalizedTypeName));
        } else if (ParamTwo.empty()) {
            Add("BitCast", Expr, Q(NormalizedTypeName), Q(ParamOne));
        } else {
            Add("BitCast", Expr, Q(NormalizedTypeName), Q(ParamOne), Q(ParamTwo));
        }
    }
    return TAstListNode::DoInit(ctx, src);
}

TNodePtr BuildCast(TContext& ctx, TPosition pos, TNodePtr expr, const TString& typeName, const TString& paramOne, const TString& paramTwo) {
    Y_UNUSED(ctx);
    if (!expr) {
        return nullptr;
    }
    return new TCastNode<false>(pos, expr, typeName, paramOne, paramTwo);
}

TNodePtr BuildBitCast(TContext& ctx, TPosition pos, TNodePtr expr, const TString& typeName, const TString& paramOne, const TString& paramTwo) {
    Y_UNUSED(ctx);
    if (!expr) {
        return nullptr;
    }
    return new TCastNode<true>(pos, expr, typeName, paramOne, paramTwo);
}

TString TypeByAlias(const TString& alias, bool normalize) {
    TString type(alias);
    TCiString typeAlias(alias);
    if (typeAlias.StartsWith("varchar")) {
        type = "String";
    } else if (typeAlias == "tinyint") {
        type = "Int8";
    } else if (typeAlias == "byte") {
        type = "Uint8";
    } else if (typeAlias == "smallint") {
        type = "Int16";
    } else if (typeAlias == "int" || typeAlias == "integer") {
        type = "Int32";
    } else if (typeAlias == "bigint") {
        type = "Int64";
    }
    return normalize ? NormalizeTypeString(type) : type;
}

TNodePtr BuildIsNullOp(TPosition pos, TNodePtr a) {
    if (!a) {
        return nullptr;
    }
    if (a->IsNull()) {
        return BuildLiteralBool(pos, "true");
    }
    return new TCallNodeImpl(pos, "Not", {new TCallNodeImpl(pos, "Exists", {a})});
}

TNodePtr BuildUnaryOp(TPosition pos, const TString& opName, TNodePtr a) {
    if (!a) {
        return nullptr;
    }
    if (a->IsNull()) {
        return BuildLiteralNull(pos);
    }
    return new TCallNodeImpl(pos, opName, {a});
}

class TBinaryOpNode final: public TCallNode {
public:
    TBinaryOpNode(TPosition pos, const TString& opName, TNodePtr a, TNodePtr b);

    TNodePtr DoClone() const final {
        YQL_ENSURE(Args.size() == 2);
        return new TBinaryOpNode(Pos, OpName, Args[0]->Clone(), Args[1]->Clone());
    }
};

TBinaryOpNode::TBinaryOpNode(TPosition pos, const TString& opName, TNodePtr a, TNodePtr b)
    : TCallNode(pos, opName, 2, 2, { a, b })
{
}

TNodePtr BuildBinaryOp(TPosition pos, const TString& opName, TNodePtr a, TNodePtr b) {
    if (!a || !b) {
        return nullptr;
    }
    if (a->IsNull() && b->IsNull()) {
        return BuildLiteralNull(pos);
    }
    return new TBinaryOpNode(pos, opName, a, b);
}

class TCalcOverWindow final: public INode {
public:
    TCalcOverWindow(TPosition pos, const TString& windowName, TNodePtr node)
        : INode(pos)
        , WindowName(windowName)
        , FuncNode(node)
    {}

    TAstNode* Translate(TContext& ctx) const override {
        return FuncNode->Translate(ctx);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        YQL_ENSURE(src);
        TSourcePtr overWindowSource = BuildOverWindowSource(ctx.Pos(), WindowName, src);
        if (!FuncNode->Init(ctx, overWindowSource.Get())) {
            return false;
        }
        return true;
    }

    TPtr DoClone() const final {
        return new TCalcOverWindow(Pos, WindowName, SafeClone(FuncNode));
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, FuncNode->IsConstant());
        State.Set(ENodeState::Aggregated, FuncNode->IsAggregated());
        State.Set(ENodeState::OverWindow, true);
    }
protected:
    const TString WindowName;
    TNodePtr FuncNode;
};

TNodePtr BuildCalcOverWindow(TPosition pos, const TString& windowName, TNodePtr call) {
    return new TCalcOverWindow(pos, windowName, call);
}

class TYsonOptionsNode final: public INode {
public:
    TYsonOptionsNode(TPosition pos, bool autoConvert, bool strict)
        : INode(pos)
        , AutoConvert(autoConvert)
        , Strict(strict)
    {
        auto udf = Y("Udf", Q("Yson.Options"));
        auto autoConvertNode = BuildLiteralBool(pos, autoConvert ? "true" : "false");
        autoConvertNode->SetLabel("AutoConvert");
        auto strictNode = BuildLiteralBool(pos, strict ? "true" : "false");
        strictNode->SetLabel("Strict");
        Node = Y("NamedApply", udf, Q(Y()), BuildStructure(pos, { autoConvertNode, strictNode }));
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node->Translate(ctx);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        YQL_ENSURE(src);
        if (!Node->Init(ctx, src)) {
            return false;
        }
        return true;
    }

    TPtr DoClone() const final {
        return new TYsonOptionsNode(Pos, AutoConvert, Strict);
    }

protected:
    TNodePtr Node;
    const bool AutoConvert;
    const bool Strict;
};

TNodePtr BuildYsonOptionsNode(TPosition pos, bool autoConvert, bool strict) {
    return new TYsonOptionsNode(pos, autoConvert, strict);
}

class TShortcutNode: public TAstAtomNode {
    TNodePtr ShortcutNode;
    TNodePtr SameNode;
    const TString BaseName;
public:
    TShortcutNode(const TNodePtr& node, const TString& baseName)
        : TAstAtomNode(node->GetPos(), TStringBuilder() << "Shortcut" << baseName, TNodeFlags::Default)
        , ShortcutNode(node)
        , BaseName(baseName)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        auto shortcut = ctx.HasBlockShortcut(ShortcutNode);
        if (!shortcut) {
            SameNode = ShortcutNode->Clone();
            if (!SameNode->Init(ctx, src)) {
                return false;
            }
            shortcut = ctx.RegisterBlockShortcut(ShortcutNode, SameNode, BaseName);
            YQL_ENSURE(shortcut);
        } else {
            SameNode = ctx.GetBlockShortcut(shortcut);
        }
        Content = shortcut;
        return true;
    }

    const TString* GetSourceName() const override {
        return ShortcutNode->GetSourceName();
    }

    void DoUpdateState() const override {
        auto& workedNode = SameNode ? SameNode : ShortcutNode;
        State.Set(ENodeState::Const, workedNode->IsConstant());
        State.Set(ENodeState::Aggregated, workedNode->IsAggregated());
        State.Set(ENodeState::OverWindow, workedNode->IsOverWindow());
    }

    TNodePtr DoClone() const final {
        return new TShortcutNode(ShortcutNode, BaseName);
    }
};

TNodePtr BuildShortcutNode(const TNodePtr& node, const TString& baseName) {
    return new TShortcutNode(node, baseName);
}

class TDoCall final : public INode {
public:
    TDoCall(TPosition pos, const TNodePtr& node)
        : INode(pos)
        , Node(node)
    {
        FakeSource = BuildFakeSource(pos);
    }

    ISource* GetSource() final {
        return FakeSource.Get();
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        Y_UNUSED(src);
        ctx.PushBlockShortcuts();
        if (!Node->Init(ctx, FakeSource.Get())) {
            return false;
        }

        Node = ctx.GroundBlockShortcutsForExpr(Node);
        return true;
    }

    TAstNode* Translate(TContext& ctx) const final {
        return Node->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TDoCall(Pos, Node->Clone());
    }

private:
    TNodePtr Node;
    TSourcePtr FakeSource;
};

TNodePtr BuildDoCall(TPosition pos, const TNodePtr& node) {
    return new TDoCall(pos, node);
}

bool Parseui32(TNodePtr from, ui32& to) {
    const TString* val;

    if (!(val = from->GetLiteral("Int32"))) {
        if (!(val = from->GetLiteral("Uint32"))) {
            return false;
        }
    }

    return TryFromString(*val, to);
}

TNodePtr GroundWithExpr(const TNodePtr& ground, const TNodePtr& expr) {
    return ground ? expr->Y("block", expr->Q(expr->L(ground, expr->Y("return", expr)))) : expr;
}

TSourcePtr TryMakeSourceFromExpression(TContext& ctx, TNodePtr node, const TString& view) {
    if (auto literal = node->GetLiteral("String")) {
        if (ctx.CurrCluster.empty()) {
            return nullptr;
        }

        TNodePtr tableKey = BuildTableKey(node->GetPos(), ctx.CurrCluster, TDeferredAtom(node->GetPos(), *literal), view);
        TTableRef table(ctx.MakeName("table"), ctx.CurrCluster, tableKey);
        table.Options = BuildInputOptions(node->GetPos(), GetContextHints(ctx));
        return BuildTableSource(node->GetPos(), table, false);
    }

    if (auto access = dynamic_cast<TAccessNode*>(node.Get())) {
        bool hasErrors;
        auto src = access->TryMakeSource(ctx, view, hasErrors);
        if (src || hasErrors) {
            return src;
        }
    }

    if (dynamic_cast<TLambdaNode*>(node.Get())) {
        ctx.Error() << "Lambda is not allowed to be used as source. Did you forget to call a subquery template?";
        return nullptr;
    }

    if (ctx.CurrCluster.empty()) {
        return nullptr;
    }

    auto wrappedNode = node->Y("EvaluateAtom", node);
    TNodePtr tableKey = BuildTableKey(node->GetPos(), ctx.CurrCluster, TDeferredAtom(wrappedNode, ctx), view);
    TTableRef table(ctx.MakeName("table"), ctx.CurrCluster, tableKey);
    table.Options = BuildInputOptions(node->GetPos(), GetContextHints(ctx));
    return BuildTableSource(node->GetPos(), table, false);
}

void MakeTableFromExpression(TContext& ctx, TNodePtr node, TDeferredAtom& table) {
    if (auto literal = node->GetLiteral("String")) {
        table = TDeferredAtom(node->GetPos(), *literal);
        return;
    }

    if (auto access = dynamic_cast<TAccessNode*>(node.Get())) {
        auto ret = access->TryMakeTable();
        if (ret) {
            table = TDeferredAtom(node->GetPos(), *ret);
            return;
        }
    }

    auto wrappedNode = node->Y("EvaluateAtom", node);
    table = TDeferredAtom(wrappedNode, ctx);
}

TDeferredAtom MakeAtomFromExpression(TContext& ctx, TNodePtr node) {
    if (auto literal = node->GetLiteral("String")) {
        return TDeferredAtom(node->GetPos(), *literal);
    }

    auto wrappedNode = node->Y("EvaluateAtom", node);
    return TDeferredAtom(wrappedNode, ctx);
}

bool TryMakeClusterAndTableFromExpression(TNodePtr node, TString& cluster, TDeferredAtom& table, TContext& ctx) {
    if (auto literal = node->GetLiteral("String")) {
        cluster.clear();
        table = TDeferredAtom(node->GetPos(), *literal);
        return true;
    }

    if (auto access = dynamic_cast<TAccessNode*>(node.Get())) {
        bool hasErrors;
        auto ret = access->TryMakeClusterAndTable(ctx, hasErrors);
        if (ret) {
            cluster = ret->first;
            table = TDeferredAtom(node->GetPos(), ret->second);
            return true;
        }

        if (hasErrors) {
            return false;
        }
    }

    auto wrappedNode = node->Y("EvaluateAtom", node);
    table = TDeferredAtom(wrappedNode, ctx);
    return true;
}

class TTupleResultNode: public INode {
public:
    TTupleResultNode(TNodePtr&& tuple, int ensureTupleSize)
        : INode(tuple->GetPos())
        , Node(std::move(tuple))
        , EnsureTupleSize(ensureTupleSize)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        ctx.PushBlockShortcuts();
        if (!Node->Init(ctx, src)) {
            return false;
        }

        Node = ctx.GroundBlockShortcutsForExpr(Node);
        Node = Y("EnsureTupleSize", Node, Q(ToString(EnsureTupleSize)));

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node->Translate(ctx);
    }

    TPtr DoClone() const final {
        return {};
    }

protected:
    TNodePtr Node;
    const int EnsureTupleSize;
};

TNodePtr BuildTupleResult(TNodePtr tuple, int ensureTupleSize) {
    return new TTupleResultNode(std::move(tuple), ensureTupleSize);
}


} // namespace NSQLTranslationV0
