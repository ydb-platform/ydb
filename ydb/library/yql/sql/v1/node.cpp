#include "node.h"
#include "source.h"
#include "context.h"

#include <ydb/library/yql/ast/yql_ast_escaping.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/sql_types/simple_types.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/charset/ci_string.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/escape.h>
#include <util/string/subst.h>

using namespace NYql;

namespace NSQLTranslationV1 {

TString ErrorDistinctWithoutCorrelation(const TString& column) {
    return TStringBuilder() << "DISTINCT columns for JOIN in SELECT should have table aliases (correlation name),"
        " add it if necessary to FROM section over 'AS <alias>' keyword and put it like '<alias>." << column << "'";
}

TString ErrorDistinctByGroupKey(const TString& column) {
    return TStringBuilder() << "Unable to use DISTINCT by grouping column: " << column << ". You should leave one of them.";
}

TTopicRef::TTopicRef(const TString& refName, const TDeferredAtom& cluster, TNodePtr keys)
    : RefName(refName)
    , Cluster(cluster)
    , Keys(keys)
{
}

TColumnConstraints::TColumnConstraints(TNodePtr defaultExpr, bool nullable)
    : DefaultExpr(defaultExpr)
    , Nullable(nullable)
{
}


TColumnSchema::TColumnSchema(TPosition pos, const TString& name, const TNodePtr& type, bool nullable,
        TVector<TIdentifier> families, bool serial, TNodePtr defaultExpr, ETypeOfChange typeOfChange)
    : Pos(pos)
    , Name(name)
    , Type(type)
    , Nullable(nullable)
    , Families(families)
    , Serial(serial)
    , DefaultExpr(defaultExpr)
    , TypeOfChange(typeOfChange)
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

TMaybe<TPosition> INode::GetLabelPos() const {
    return LabelPos;
}

void INode::SetLabel(const TString& label, TMaybe<TPosition> pos) {
    Label = label;
    LabelPos = pos;
}

bool INode::IsImplicitLabel() const {
    return ImplicitLabel;
}

void INode::MarkImplicitLabel(bool isImplicitLabel) {
    ImplicitLabel = isImplicitLabel;
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

bool INode::MaybeConstant() const {
    return HasState(ENodeState::MaybeConst);
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

bool INode::IsLiteral() const {
    return false;
}

TString INode::GetLiteralType() const {
    return "";
}

TString INode::GetLiteralValue() const {
    return "";
}

bool INode::IsIntegerLiteral() const {
    return false;
}

INode::TPtr INode::ApplyUnaryOp(TContext& ctx, TPosition pos, const TString& opName) const {
    Y_UNUSED(ctx);
    if (IsNull()) {
        return BuildLiteralNull(pos);
    }
    return new TCallNodeImpl(pos, opName, { Clone() });
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

bool INode::IsOptionalArg() const {
    return false;
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

bool INode::InitReference(TContext& ctx) {
    Y_UNUSED(ctx);
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
        YQL_ENSURE(!State.Test(ENodeState::Initialized), "Clone should be for uninitialized or persistent node");
        clone->SetLabel(Label, LabelPos);
        clone->MarkImplicitLabel(ImplicitLabel);
    }
    return clone;
}

TAggregationPtr INode::GetAggregation() const {
    return {};
}

void INode::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    Y_UNUSED(ctx);
    Y_UNUSED(src);
    Y_UNUSED(exprs);
}

INode::TPtr INode::WindowSpecFunc(const TPtr& type) const {
    Y_UNUSED(type);
    return {};
}

bool INode::SetViewName(TContext& ctx, TPosition pos, const TString& view) {
    Y_UNUSED(pos);
    Y_UNUSED(view);
    ctx.Error() << "Node not support views";
    return false;
}

bool INode::SetPrimaryView(TContext& ctx, TPosition pos) {
    Y_UNUSED(pos);
    ctx.Error() << "Node not support primary views";
    return false;
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

bool INode::HasSelectResult() const {
    return false;
}

const TString* INode::FuncName() const {
    return nullptr;
}

const TString* INode::ModuleName() const {
    return nullptr;
}

bool INode::HasSkip() const {
    return false;
}

TColumnNode* INode::GetColumnNode() {
    return nullptr;
}

const TColumnNode* INode::GetColumnNode() const {
    return nullptr;
}

TTupleNode* INode::GetTupleNode() {
    return nullptr;
}

const TTupleNode* INode::GetTupleNode() const {
    return nullptr;
}

TCallNode* INode::GetCallNode() {
    return nullptr;
}

const TCallNode* INode::GetCallNode() const {
    return nullptr;
}

TStructNode* INode::GetStructNode() {
    return nullptr;
}

const TStructNode* INode::GetStructNode() const {
    return nullptr;
}

TAccessNode* INode::GetAccessNode() {
    return nullptr;
}

const TAccessNode* INode::GetAccessNode() const {
    return nullptr;
}

TLambdaNode* INode::GetLambdaNode() {
    return nullptr;
}

const TLambdaNode* INode::GetLambdaNode() const {
    return nullptr;
}

TUdfNode* INode::GetUdfNode() {
    return nullptr;
}

const TUdfNode* INode::GetUdfNode() const {
    return nullptr;
}

void INode::VisitTree(const TVisitFunc& func) const {
    TVisitNodeSet visited;
    VisitTree(func, visited);
}

void INode::VisitTree(const TVisitFunc& func, TVisitNodeSet& visited) const {
    if (visited.emplace(this).second && HasState(ENodeState::Initialized) && func(*this)) {
        DoVisitChildren(func, visited);
    }
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

void INode::DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const {
    Y_UNUSED(func);
    Y_UNUSED(visited);
}

void INode::DoAdd(TNodePtr node) {
    Y_UNUSED(node);
    Y_DEBUG_ABORT_UNLESS(false, "Node is not expandable");
}

bool IProxyNode::IsNull() const {
    return Inner->IsNull();
}

bool IProxyNode::IsLiteral() const {
    return Inner->IsNull();
}

TString IProxyNode::GetLiteralType() const {
    return Inner->GetLiteralType();
}

TString IProxyNode::GetLiteralValue() const {
    return Inner->GetLiteralValue();
}

bool IProxyNode::IsIntegerLiteral() const {
    return Inner->IsIntegerLiteral();
}

INode::TPtr IProxyNode::ApplyUnaryOp(TContext& ctx, TPosition pos, const TString& opName) const {
    return Inner->ApplyUnaryOp(ctx, pos, opName);
}

bool IProxyNode::IsAsterisk() const {
    return Inner->IsAsterisk();
}

const TString* IProxyNode::SubqueryAlias() const {
    return Inner->SubqueryAlias();
}

TString IProxyNode::GetOpName() const {
    return Inner->GetOpName();
}

const TString* IProxyNode::GetLiteral(const TString& type) const {
    return Inner->GetLiteral(type);
}

const TString* IProxyNode::GetColumnName() const {
    return Inner->GetColumnName();
}

void IProxyNode::AssumeColumn() {
    Inner->AssumeColumn();
}

const TString* IProxyNode::GetSourceName() const {
    return Inner->GetSourceName();
}

const TString* IProxyNode::GetAtomContent() const {
    return Inner->GetAtomContent();
}

bool IProxyNode::IsOptionalArg() const {
    return Inner->IsOptionalArg();
}

size_t IProxyNode::GetTupleSize() const {
    return Inner->GetTupleSize();
}

INode::TPtr IProxyNode::GetTupleElement(size_t index) const {
    return Inner->GetTupleElement(index);
}

ITableKeys* IProxyNode::GetTableKeys() {
    return Inner->GetTableKeys();
}

ISource* IProxyNode::GetSource() {
    return Inner->GetSource();
}

TVector<INode::TPtr>* IProxyNode::ContentListPtr() {
    return Inner->ContentListPtr();
}

TAggregationPtr IProxyNode::GetAggregation() const {
    return Inner->GetAggregation();
}

void IProxyNode::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    Inner->CollectPreaggregateExprs(ctx, src, exprs);
}

INode::TPtr IProxyNode::WindowSpecFunc(const TPtr& type) const {
    return Inner->WindowSpecFunc(type);
}

bool IProxyNode::SetViewName(TContext& ctx, TPosition pos, const TString& view) {
    return Inner->SetViewName(ctx, pos, view);
}

bool IProxyNode::SetPrimaryView(TContext& ctx, TPosition pos) {
    return Inner->SetPrimaryView(ctx, pos);
}

bool IProxyNode::UsedSubquery() const {
    return Inner->UsedSubquery();
}

bool IProxyNode::IsSelect() const {
    return Inner->IsSelect();
}

bool IProxyNode::HasSelectResult() const {
    return Inner->HasSelectResult();
}

const TString* IProxyNode::FuncName() const {
    return Inner->FuncName();
}

const TString* IProxyNode::ModuleName() const {
    return Inner->ModuleName();
}

bool IProxyNode::HasSkip() const {
    return Inner->HasSkip();
}

TColumnNode* IProxyNode::GetColumnNode() {
    return Inner->GetColumnNode();
}

const TColumnNode* IProxyNode::GetColumnNode() const {
    return static_cast<const INode*>(Inner.Get())->GetColumnNode();
}

TTupleNode* IProxyNode::GetTupleNode() {
    return Inner->GetTupleNode();
}

const TTupleNode* IProxyNode::GetTupleNode() const {
    return static_cast<const INode*>(Inner.Get())->GetTupleNode();
}

TCallNode* IProxyNode::GetCallNode() {
    return Inner->GetCallNode();
}

const TCallNode* IProxyNode::GetCallNode() const {
    return static_cast<const INode*>(Inner.Get())->GetCallNode();
}

TStructNode* IProxyNode::GetStructNode() {
    return Inner->GetStructNode();
}

const TStructNode* IProxyNode::GetStructNode() const {
    return static_cast<const INode*>(Inner.Get())->GetStructNode();
}

TAccessNode* IProxyNode::GetAccessNode() {
    return Inner->GetAccessNode();
}

const TAccessNode* IProxyNode::GetAccessNode() const {
    return static_cast<const INode*>(Inner.Get())->GetAccessNode();
}

TLambdaNode* IProxyNode::GetLambdaNode() {
    return Inner->GetLambdaNode();
}

const TLambdaNode* IProxyNode::GetLambdaNode() const {
    return static_cast<const INode*>(Inner.Get())->GetLambdaNode();
}

TUdfNode* IProxyNode::GetUdfNode() {
    return Inner->GetUdfNode();
}

const TUdfNode* IProxyNode::GetUdfNode() const {
    return static_cast<const INode*>(Inner.Get())->GetUdfNode();
}

void IProxyNode::DoUpdateState() const {
    static_assert(static_cast<int>(ENodeState::End) == 9, "Need to support new states here");
    State.Set(ENodeState::CountHint, Inner->GetCountHint());
    State.Set(ENodeState::Const, Inner->IsConstant());
    State.Set(ENodeState::MaybeConst, Inner->MaybeConstant());
    State.Set(ENodeState::Aggregated, Inner->IsAggregated());
    State.Set(ENodeState::AggregationKey, Inner->IsAggregationKey());
    State.Set(ENodeState::OverWindow, Inner->IsOverWindow());
}

void IProxyNode::DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const {
    Inner->VisitTree(func, visited);
}

bool IProxyNode::InitReference(TContext& ctx) {
    return Inner->InitReference(ctx);
}

bool IProxyNode::DoInit(TContext& ctx, ISource* src) {
    return Inner->Init(ctx, src);
}

void IProxyNode::DoAdd(TPtr node) {
    Inner->Add(node);
}

void MergeHints(TTableHints& base, const TTableHints& overrides) {
    for (auto& i : overrides) {
        base[i.first] = i.second;
    }
}

TTableHints CloneContainer(const TTableHints& hints) {
    TTableHints result;
    for (auto& [name, nodes] : hints) {
        result.emplace(std::make_pair(name, CloneContainer(nodes)));
    }
    return result;
}

TAstAtomNode::TAstAtomNode(TPosition pos, const TString& content, ui32 flags, bool isOptionalArg)
    : INode(pos)
    , Content(content)
    , Flags(flags)
    , IsOptionalArg_(isOptionalArg)
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

bool TAstAtomNode::IsOptionalArg() const {
    return IsOptionalArg_;
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

TNodePtr BuildAtom(TPosition pos, const TString& content, ui32 flags, bool isOptionalArg) {
    return new TAstAtomNodeImpl(pos, content, flags, isOptionalArg);
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
        const bool isNodeMaybeConst = node->MaybeConstant();
        for (auto state: checkStates) {
            if (node->HasState(state)) {
                flags[state].has = true;
            } else if (!isNodeConst && !isNodeMaybeConst) {
                flags[state].all = false;
            }

            if (!isNodeConst) {
                isConst = false;
            }
        }
    }
    State.Set(ENodeState::Const, isConst);
    for (auto& flag: flags) {
        State.Set(flag.first, flag.second.has && flag.second.all);
    }
    State.Set(ENodeState::MaybeConst, !isConst && AllOf(nodes, [](const auto& node) { return node->IsConstant() || node->MaybeConstant(); }));
}

void TAstListNode::DoUpdateState() const {
    UpdateStateByListNodes(Nodes);
}

void TAstListNode::DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const {
    for (auto& node : Nodes) {
        node->VisitTree(func, visited);
    }
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
    for (const auto& node: Nodes) {
        YQL_ENSURE(node, "Null ptr passed as list element");
    }
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
    for (const auto& node: nodes) {
        YQL_ENSURE(node, "Null ptr passed as list element");
    }
    Nodes.swap(nodes);
}

void TAstListNodeImpl::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    for (auto& node : Nodes) {
        node->CollectPreaggregateExprs(ctx, src, exprs);
    }
}

TNodePtr TAstListNodeImpl::DoClone() const {
    return new TAstListNodeImpl(Pos, CloneContainer(Nodes));
}

TCallNode::TCallNode(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TAstListNode(pos)
    , OpName(opName)
    , MinArgs(minArgs)
    , MaxArgs(maxArgs)
    , Args(args)
{
    for (const auto& arg: Args) {
        YQL_ENSURE(arg, "Null ptr passed as call argument");
    }
}

TString TCallNode::GetOpName() const {
    return OpName;
}

const TString* DeriveCommonSourceName(const TVector<TNodePtr> &nodes) {
    const TString* name = nullptr;
    for (auto& node: nodes) {
        auto n = node->GetSourceName();
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


const TString* TCallNode::GetSourceName() const {
    return DeriveCommonSourceName(Args);
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

void TCallNode::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    for (auto& arg : Args) {
        arg->CollectPreaggregateExprs(ctx, src, exprs);
    }
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

    Nodes.push_back(BuildAtom(Pos, OpName,
         OpName.cend() == std::find_if_not(OpName.cbegin(), OpName.cend(), [](char c) { return bool(std::isalnum(c)); }) ? TNodeFlags::Default : TNodeFlags::ArbitraryContent));
    Nodes.insert(Nodes.end(), Args.begin(), Args.end());
    return true;
}

TCallNode* TCallNode::GetCallNode() {
    return this;
}

const TCallNode* TCallNode::GetCallNode() const {
    return this;
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

TFuncNodeImpl::TFuncNodeImpl(TPosition pos, const TString& opName)
    : TCallNode(pos, opName, 0, 0, {})
{}

TCallNode::TPtr TFuncNodeImpl::DoClone() const {
    return new TFuncNodeImpl(GetPos(), OpName);
}

const TString* TFuncNodeImpl::FuncName() const {
    return &OpName;
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

TCallDirectRow::TCallDirectRow(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, minArgs, maxArgs, args)
{}

TCallDirectRow::TCallDirectRow(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, 0, 0, args)
{}

bool TCallDirectRow::DoInit(TContext& ctx, ISource* src) {
    if (!src || (ctx.CompactNamedExprs && src->IsFake())) {
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
        ctx.Error(Pos) << "Unable to use window function " << OpName << " without source";
        return false;
    }

    if (!src->IsOverWindowSource()) {
        ctx.Error(Pos) << "Failed to use window function " << OpName << " without window specification";
        return false;
    }
    if (!src->AddFuncOverWindow(ctx, this)) {
        ctx.Error(Pos) << "Failed to use window function " << OpName << " without window specification or in wrong place";
        return false;
    }

    FuncAlias = "_yql_" + src->MakeLocalName(OpName);
    src->AddTmpWindowColumn(FuncAlias);
    if (!TCallNode::DoInit(ctx, src)) {
        return false;
    }
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

TWinCumeDist::TWinCumeDist(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{}

bool TWinCumeDist::DoInit(TContext& ctx, ISource* src) {
    if (!ValidateArguments(ctx)) {
        return false;
    }

    YQL_ENSURE(Args.size() == 0);
    TVector<TNodePtr> optionsElements;
    if (ctx.AnsiCurrentRow) {
        optionsElements.push_back(BuildTuple(Pos, { BuildQuotedAtom(Pos, "ansi", NYql::TNodeFlags::Default) }));
    }
    Args.push_back(BuildTuple(Pos, optionsElements));

    MinArgs = MaxArgs = 1;
    if (!TWinAggrEmulation::DoInit(ctx, src)) {
        return false;
    }

    YQL_ENSURE(Args.size() == 1);
    return true;
}

TWinNTile::TWinNTile(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{
    FakeSource = BuildFakeSource(pos);
}

bool TWinNTile::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() >= 1 && !Args[0]->Init(ctx, FakeSource.Get())) {
        return false;
    }

    if (!TWinAggrEmulation::DoInit(ctx, src)) {
        return false;
    }
    return true;
}

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
        Args[0] = BuildLambda(Pos, Y("row"), Args[0]);
    }
    return true;
}

TWinRank::TWinRank(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{

}

bool TExternalFunctionConfig::DoInit(TContext& ctx, ISource* src) {
    for (auto& param: Config) {
        auto paramName = Y(BuildQuotedAtom(Pos, param.first));
        if (!param.second->Init(ctx, src)) {
            return false;
        }
        Nodes.push_back(Q(L(paramName, param.second)));
    }
    return true;
}

INode::TPtr TExternalFunctionConfig::DoClone() const {
    TFunctionConfig cloned;
    for (auto& [name, node] : Config) {
        cloned[name] = SafeClone(node);
    }

    return new TExternalFunctionConfig(GetPos(), cloned);
}

bool TWinRank::DoInit(TContext& ctx, ISource* src) {
    if (!ValidateArguments(ctx)) {
        return false;
    }

    if (!src) {
        ctx.Error(Pos) << "Unable to use window function: " << OpName << " without source";
        return false;
    }

    auto winNamePtr = src->GetWindowName();
    if (!winNamePtr) {
        ctx.Error(Pos) << "Failed to use window function: " << OpName << " without window";
        return false;
    }

    auto winSpecPtr = src->FindWindowSpecification(ctx, *winNamePtr);
    if (!winSpecPtr) {
        return false;
    }

    const auto& orderSpec = winSpecPtr->OrderBy;
    if (orderSpec.empty()) {
        if (Args.empty()) {
            ctx.Warning(GetPos(), TIssuesIds::YQL_RANK_WITHOUT_ORDER_BY) <<
                OpName << "() is used with unordered window - all rows will be considered equal to each other";
        } else {
            ctx.Warning(GetPos(), TIssuesIds::YQL_RANK_WITHOUT_ORDER_BY) <<
                OpName << "(<expression>) is used with unordered window - the result is likely to be undefined";
        }
    }

    if (Args.empty()) {
        for (const auto& spec: orderSpec) {
            Args.push_back(spec->Clone()->OrderExpr);
        }

        if (Args.size() != 1) {
            Args = {BuildTuple(GetPos(), Args)};
        }
    }

    YQL_ENSURE(Args.size() == 1);

    TVector<TNodePtr> optionsElements;
    if (!ctx.AnsiRankForNullableKeys.Defined()) {
        optionsElements.push_back(BuildTuple(Pos, { BuildQuotedAtom(Pos, "warnNoAnsi", NYql::TNodeFlags::Default) }));
    } else if (*ctx.AnsiRankForNullableKeys) {
        optionsElements.push_back(BuildTuple(Pos, { BuildQuotedAtom(Pos, "ansi", NYql::TNodeFlags::Default) }));
    }
    Args.push_back(BuildTuple(Pos, optionsElements));

    MinArgs = MaxArgs = 2;
    if (!TWinAggrEmulation::DoInit(ctx, src)) {
        return false;
    }

    YQL_ENSURE(Args.size() == 2);
    Args[0] = BuildLambda(Pos, Y("row"), Args[0]);
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


TNodePtr ITableKeys::AddView(TNodePtr key, const TViewDescription& view) {
    if (view.PrimaryFlag) {
        return L(key, Q(Y(Q("primary_view"))));
    } else if (!view.empty()) {
        return L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos, view.ViewName)))));
    } else {
        return key;
    }
}

TString TColumns::AddUnnamed() {
    TString desiredResult = TStringBuilder() << "column" << List.size();
    if (!All) {
        HasUnnamed = true;
        List.emplace_back();
        NamedColumns.push_back(false);
    }
    return desiredResult;
}

bool TColumns::Add(const TString* column, bool countHint, bool isArtificial, bool isReliable) {
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
            NamedColumns.push_back(true);
        }
        return inserted;
    }
    return All;
}

void TColumns::Merge(const TColumns& columns) {
    if (columns.All) {
        SetAll();
    } else {
        YQL_ENSURE(columns.List.size() == columns.NamedColumns.size());
        size_t myUnnamed = NamedColumns.size() - std::accumulate(NamedColumns.begin(), NamedColumns.end(), 0);
        size_t otherUnnamed = 0;
        for (size_t i = 0; i < columns.List.size(); ++i) {
            auto& c = columns.List[i];
            if (!columns.NamedColumns[i]) {
                if (++otherUnnamed > myUnnamed) {
                    AddUnnamed();
                    ++myUnnamed;
                }
                continue;
            }
            if (columns.Real.contains(c)) {
                Add(&c, false, false);
            }
            if (columns.Artificial.contains(c)) {
                Add(&c, false, true);
            }
        }
        HasUnreliable |= columns.HasUnreliable;
        HasUnnamed |= columns.HasUnnamed;
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
    QualifiedAll = false;
    Real.clear();
    List.clear();
    Artificial.clear();
    NamedColumns.clear();
    HasUnnamed = HasUnreliable = false;
}

namespace {

bool MaybeAutogenerated(const TString& name) {
    TStringBuf prefix = "column";
    if (!name.StartsWith(prefix)) {
        return false;
    }

    TString suffix = name.substr(prefix.size());
    return !suffix.empty() && AllOf(suffix, [](const auto c) { return std::isdigit(c); });
}

}

bool TColumns::IsColumnPossible(TContext& ctx, const TString& name) const {
    if (All || Real.contains(name) || Artificial.contains(name)) {
        return true;
    }
    if (QualifiedAll) {
        if (ctx.SimpleColumns) {
            return true;
        }
        if (HasUnnamed) {
            const auto dotPos = name.find_first_of(".");
            TString suffix = (dotPos == TString::npos) ? name : name.substr(dotPos + 1);
            if (MaybeAutogenerated(suffix)) {
                return true;
            }
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
    } else if (HasUnnamed && MaybeAutogenerated(name)) {
        return true;
    }
    return false;
}

TSortSpecification::TSortSpecification(const TNodePtr& orderExpr, bool ascending)
    : OrderExpr(orderExpr->Clone())
    , Ascending(ascending)
    , CleanOrderExpr(orderExpr->Clone())
{
}

TSortSpecificationPtr TSortSpecification::Clone() const {
    return MakeIntrusive<TSortSpecification>(CleanOrderExpr, Ascending);
}

TFrameBoundPtr TFrameBound::Clone() const {
    auto res = MakeIntrusive<TFrameBound>();
    res->Pos = Pos;
    res->Bound = SafeClone(Bound);
    res->Settings = Settings;
    return res;
}

TFrameSpecificationPtr TFrameSpecification::Clone() const {
    YQL_ENSURE(FrameBegin);
    YQL_ENSURE(FrameEnd);
    auto res = MakeIntrusive<TFrameSpecification>();
    res->FrameType = FrameType;
    res->FrameBegin = FrameBegin->Clone();
    res->FrameEnd = FrameEnd->Clone();
    res->FrameExclusion = FrameExclusion;
    return res;
}

TWindowSpecificationPtr TWindowSpecification::Clone() const {
    YQL_ENSURE(Frame);
    auto res = MakeIntrusive<TWindowSpecification>();
    res->ExistingWindowName = ExistingWindowName;
    res->Partitions = CloneContainer(Partitions);
    res->IsCompact = IsCompact;
    res->OrderBy = CloneContainer(OrderBy);
    res->Session = SafeClone(Session);
    res->Frame = Frame->Clone();
    return res;
}

TWinSpecs CloneContainer(const TWinSpecs& specs) {
    TWinSpecs newSpecs;
    for (auto cur: specs) {
        newSpecs.emplace(cur.first, cur.second->Clone());
    }
    return newSpecs;
}

TLegacyHoppingWindowSpecPtr TLegacyHoppingWindowSpec::Clone() const {
    auto res = MakeIntrusive<TLegacyHoppingWindowSpec>();
    res->TimeExtractor = TimeExtractor->Clone();
    res->Hop = Hop->Clone();
    res->Interval = Interval->Clone();
    res->Delay = Delay->Clone();
    res->DataWatermarks = DataWatermarks;
    return res;
}

TColumnNode::TColumnNode(TPosition pos, const TString& column, const TString& source, bool maybeType)
    : INode(pos)
    , ColumnName(column)
    , Source(source)
    , MaybeType(maybeType)
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

TColumnNode* TColumnNode::GetColumnNode() {
    return this;
}

const TColumnNode* TColumnNode::GetColumnNode () const {
    return this;
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
        TString callable;
        if (MaybeType) {
            callable = Reliable && !UseSource ? "SqlPlainColumnOrType" : "SqlColumnOrType";
        } else {
            // TODO: consider replacing Member -> SqlPlainColumn
            callable = Reliable && !UseSource ? "Member" : "SqlColumn";
        }
        Node = Y(callable, "row", ColumnExpr ? Y("EvaluateAtom", ColumnExpr) : BuildQuotedAtom(Pos, *GetColumnName()));
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

bool TColumnNode::IsUseSource() const {
    return UseSource;
}

bool TColumnNode::IsReliable() const {
    return Reliable;
}

bool TColumnNode::CanBeType() const {
    return MaybeType;
}

TNodePtr TColumnNode::DoClone() const {
    YQL_ENSURE(!Node, "TColumnNode::Clone: Node should not be initialized");
    auto copy = ColumnExpr ? new TColumnNode(Pos, ColumnExpr, Source) : new TColumnNode(Pos, ColumnName, Source, MaybeType);
    copy->GroupKey = GroupKey;
    copy->Artificial = Artificial;
    copy->Reliable = Reliable;
    copy->UseSource = UseSource;
    copy->UseSourceAsColumn = UseSourceAsColumn;
    return copy;
}

void TColumnNode::DoUpdateState() const {
    State.Set(ENodeState::Const, false);
    State.Set(ENodeState::MaybeConst, MaybeType);
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
    bool maybeType = false;
    return new TColumnNode(pos, column, source, maybeType);
}

TNodePtr BuildColumn(TPosition pos, const TNodePtr& column, const TString& source) {
    return new TColumnNode(pos, column, source);
}

TNodePtr BuildColumn(TPosition pos, const TDeferredAtom& column, const TString& source) {
    return column.GetLiteral() ? BuildColumn(pos, *column.GetLiteral(), source) : BuildColumn(pos, column.Build(), source);
}

TNodePtr BuildColumnOrType(TPosition pos, const TString& column) {
    TString source = "";
    bool maybeType = true;
    return new TColumnNode(pos, column, source, maybeType);
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
    YQL_ENSURE(false, "Should not be called");
}

const TString& IAggregation::GetName() const {
    return Name;
}

EAggregateMode IAggregation::GetAggregationMode() const {
    return AggMode;
}

void IAggregation::MarkKeyColumnAsGenerated() {
    IsGeneratedKeyColumn = true;
}

IAggregation::IAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode)
    : INode(pos), Name(name), Func(func), AggMode(aggMode)
{}

TAstNode* IAggregation::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(false);
    Y_UNUSED(ctx);
    return nullptr;
}

std::pair<TNodePtr, bool> IAggregation::AggregationTraits(const TNodePtr& type, bool overState, bool many, bool allowAggApply, TContext& ctx) const {
    const bool distinct = AggMode == EAggregateMode::Distinct;
    const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos, DistinctKey))) : type;
    auto apply = GetApply(listType, many, allowAggApply, ctx);
    if (!apply) {
        return { nullptr, false };
    }

    auto wrapped = WrapIfOverState(apply, overState, many, ctx);
    if (!wrapped) {
        return { nullptr, false };
    }

    return { distinct ?
        Q(Y(Q(Name), wrapped, BuildQuotedAtom(Pos, DistinctKey))) :
        Q(Y(Q(Name), wrapped)), true };
}

TNodePtr IAggregation::WrapIfOverState(const TNodePtr& input, bool overState, bool many, TContext& ctx) const {
    if (!overState) {
        return input;
    }

    auto extractor = GetExtractor(many, ctx);
    if (!extractor) {
        return nullptr;
    }

    return Y(ToString("AggOverState"), extractor, BuildLambda(Pos, Y(), input));
}

void IAggregation::AddFactoryArguments(TNodePtr& apply) const {
    Y_UNUSED(apply);
}

std::vector<ui32> IAggregation::GetFactoryColumnIndices() const {
    return {0u};
}

TNodePtr IAggregation::WindowTraits(const TNodePtr& type, TContext& ctx) const {
    YQL_ENSURE(AggMode == EAggregateMode::OverWindow, "Windows traits is unavailable");
    return Q(Y(Q(Name), GetApply(type, false, false, ctx)));
}

namespace {
bool UnescapeQuoted(const TString& str, TPosition& pos, char quoteChar, TString& result, TString& error) {
    result = error = {};

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
    return true;
}

TString UnescapeAnsiQuoted(const TString& str) {
    YQL_ENSURE(str.length() >= 2);
    YQL_ENSURE(str[0] == str[str.length() - 1]);
    YQL_ENSURE(str[0] == '\'' || str[0] == '"');

    TString quote(1, str[0]);
    TString replace(2, str[0]);

    TString result = str.substr(1, str.length() - 2);
    SubstGlobal(result, replace, quote);
    return result;
}

enum class EStringContentMode : int {
    Default = 0,
    AnsiIdent,
    TypedStringLiteral,
};

TMaybe<TStringContent>
StringContentInternal(TContext& ctx, TPosition pos, const TString& input, EStringContentMode mode) {
    TStringContent result;
    if (mode == EStringContentMode::AnsiIdent) {
        if (!(input.size() >= 2 && input.StartsWith('"') && input.EndsWith('"'))) {
            ctx.Error(pos) << "Expected double quoted identifier, got string literal";
            return {};
        }

        result.Flags = NYql::TNodeFlags::ArbitraryContent;
        result.Content = UnescapeAnsiQuoted(input);
        return result;
    }

    TString str = input;
    if (mode == EStringContentMode::TypedStringLiteral) {
        auto lower = to_lower(str);
        if (lower.EndsWith("y")) {
            str = str.substr(0, str.Size() - 1);
            result.Type = NKikimr::NUdf::EDataSlot::Yson;
        } else if (lower.EndsWith("j")) {
            str = str.substr(0, str.Size() - 1);
            result.Type = NKikimr::NUdf::EDataSlot::Json;
        } else if (lower.EndsWith("p")) {
            str = str.substr(0, str.Size() - 1);
            result.PgType = "PgText";
        } else if (lower.EndsWith("pt")) {
            str = str.substr(0, str.Size() - 2);
            result.PgType = "PgText";
        } else if (lower.EndsWith("pb")) {
            str = str.substr(0, str.Size() - 2);
            result.PgType = "PgBytea";
        } else if (lower.EndsWith("pv")) {
            str = str.substr(0, str.Size() - 2);
            result.PgType = "PgVarchar";
        } else if (lower.EndsWith("s")) {
            str = str.substr(0, str.Size() - 1);
            result.Type = NKikimr::NUdf::EDataSlot::String;
        } else if (lower.EndsWith("u")) {
            str = str.substr(0, str.Size() - 1);
            result.Type = NKikimr::NUdf::EDataSlot::Utf8;
        } else {
            if (ctx.Scoped->WarnUntypedStringLiterals) {
                ctx.Warning(pos, TIssuesIds::YQL_UNTYPED_STRING_LITERALS)
                << "Please add suffix u for Utf8 strings or s for arbitrary binary strings";
            }

            if (ctx.Scoped->UnicodeLiterals) {
                result.Type = NKikimr::NUdf::EDataSlot::Utf8;
            }
        }
    }

    if (mode == EStringContentMode::Default && (result.Type != NKikimr::NUdf::EDataSlot::String || result.PgType)) {
        ctx.Error(pos) << "Type suffix is not allowed here";
        return {};
    }

    bool doubleQuoted = (str.StartsWith('"') && str.EndsWith('"'));
    bool singleQuoted = !doubleQuoted && (str.StartsWith('\'') && str.EndsWith('\''));

    if (str.size() >= 2 && (doubleQuoted || singleQuoted)) {
        result.Flags = NYql::TNodeFlags::ArbitraryContent;
        if (ctx.Settings.AnsiLexer) {
            YQL_ENSURE(singleQuoted);
            result.Content = UnescapeAnsiQuoted(str);
        } else {
            TString error;
            if (!UnescapeQuoted(str, pos, str[0], result.Content, error)) {
                ctx.Error(pos) << "Failed to parse string literal: " << error;
                return {};
            }
        }
    } else if (str.size() >= 4 && str.StartsWith("@@") && str.EndsWith("@@")) {
        result.Flags = TNodeFlags::MultilineContent;
        TString s = str.substr(2, str.length() - 4);
        SubstGlobal(s, "@@@@", "@@");
        result.Content.swap(s);
    } else {
        ctx.Error(pos) << "Invalid string literal: " << EscapeC(str);
        return {};
    }

    if (!result.PgType.Defined() && !NKikimr::NMiniKQL::IsValidStringValue(result.Type, result.Content)) {
        ctx.Error() << "Invalid value " << result.Content.Quote() << " for type " << result.Type;
        return {};
    }

    return result;
}
} // namespace

TMaybe<TStringContent> StringContent(TContext& ctx, TPosition pos, const TString& input) {
    if (ctx.AnsiQuotedIdentifiers && input.StartsWith('"')) {
        ctx.Error() << "Expected string literal, got quoted identifier";
        return {};
    }

    return StringContentInternal(ctx, pos, input, EStringContentMode::Default);
}

TMaybe<TStringContent> StringContentOrIdContent(TContext& ctx, TPosition pos, const TString& input) {
    return StringContentInternal(ctx, pos, input,
        (ctx.AnsiQuotedIdentifiers && input.StartsWith('"'))? EStringContentMode::AnsiIdent : EStringContentMode::Default);
}

TTtlSettings::TTtlSettings(const TIdentifier& columnName, const TNodePtr& expr, const TMaybe<EUnit>& columnUnit)
    : ColumnName(columnName)
    , Expr(expr)
    , ColumnUnit(columnUnit)
{
}

TString IdContent(TContext& ctx, const TString& s) {
    YQL_ENSURE(!s.empty(), "Empty identifier not expected");
    if (!s.StartsWith('`')) {
        return s;
    }
    auto endSym = '`';
    if (s.size() < 2 || !s.EndsWith(endSym)) {
        ctx.Error() << "The identifier that starts with: '" << s[0] << "' should ends with: '" << endSym << "'";
        return {};
    }
    size_t skipSymbols = 1;

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

TString IdContentFromString(TContext& ctx, const TString& str) {
    if (!ctx.AnsiQuotedIdentifiers) {
        ctx.Error() << "String literal can not be used here";
        return {};
    }
    auto parsed = StringContentInternal(ctx, ctx.Pos(), str, EStringContentMode::AnsiIdent);
    if (!parsed) {
        return {};
    }

    return parsed->Content;
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
        return new TInvalidLiteralNode(GetPos());
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
    if (Type.StartsWith("Pg")) {
        Add("PgConst", BuildQuotedAtom(Pos, Value), Y("PgType", Q(to_lower(Type.substr(2)))));
    } else {
        Add(Type, BuildQuotedAtom(Pos, Value));
    }
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

TLiteralNode::TLiteralNode(TPosition pos, const TString& value, ui32 nodeFlags, const TString& type)
    : TAstListNode(pos)
    , Null(false)
    , Void(false)
    , Type(type)
    , Value(value)
{
    if (Type.StartsWith("Pg")) {
        Add("PgConst", BuildQuotedAtom(Pos, Value, nodeFlags), Y("PgType", Q(to_lower(Type.substr(2)))));
    } else {
        Add(Type, BuildQuotedAtom(pos, Value, nodeFlags));
    }
}

bool TLiteralNode::IsNull() const {
    return Null;
}

const TString* TLiteralNode::GetLiteral(const TString& type) const {
    return type == Type ? &Value : nullptr;
}

bool TLiteralNode::IsLiteral() const {
    return true;
}

TString TLiteralNode::GetLiteralType() const {
    return Type;
}

TString TLiteralNode::GetLiteralValue() const {
    return Value;
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
TLiteralNumberNode<T>::TLiteralNumberNode(TPosition pos, const TString& type, const TString& value, bool implicitType)
    : TLiteralNode(pos, type, value)
    , ImplicitType(implicitType)
{}

template<typename T>
TNodePtr TLiteralNumberNode<T>::DoClone() const {
    return new TLiteralNumberNode<T>(Pos, Type, Value, ImplicitType);
}

template<typename T>
bool TLiteralNumberNode<T>::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(src);
    T val;
    if (!TryFromString(Value, val)) {
        ctx.Error(Pos) << "Failed to parse " << Value << " as integer literal of " << Type << " type: value out of range for " << Type;
        return false;
    }
    return true;
}

template<typename T>
bool TLiteralNumberNode<T>::IsIntegerLiteral() const {
    return std::numeric_limits<T>::is_integer;
}

template<typename T>
TNodePtr TLiteralNumberNode<T>::ApplyUnaryOp(TContext& ctx, TPosition pos, const TString& opName) const {
    YQL_ENSURE(!Value.empty());
    if (opName == "Minus" && IsIntegerLiteral() && Value[0] != '-') {
        if (ImplicitType) {
            ui64 val = FromString<ui64>(Value);
            TString negated = "-" + Value;
            if (val <= ui64(std::numeric_limits<i32>::max()) + 1) {
                // negated value fits in Int32
                i32 v;
                YQL_ENSURE(TryFromString(negated, v));
                return new TLiteralNumberNode<i32>(pos, Type.StartsWith("Pg") ? "PgInt4" : "Int32", negated);
            }
            if (val <= ui64(std::numeric_limits<i64>::max()) + 1) {
                // negated value fits in Int64
                i64 v;
                YQL_ENSURE(TryFromString(negated, v));
                return new TLiteralNumberNode<i64>(pos, Type.StartsWith("Pg") ? "PgInt8" : "Int64", negated);
            }

            ctx.Error(pos) << "Failed to parse negative integer: " << negated << ", number limit overflow";
            return {};
        }

        if (std::numeric_limits<T>::is_signed) {
            return new TLiteralNumberNode<T>(pos, Type, "-" + Value);
        }
    }
    return INode::ApplyUnaryOp(ctx, pos, opName);
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
    auto unescaped = StringContent(ctx, ctx.Pos(), value);
    if (!unescaped) {
        return new TInvalidLiteralNode(ctx.Pos());
    }

    YQL_ENSURE(unescaped->Type == NKikimr::NUdf::EDataSlot::String);
    return new TLiteralNode(ctx.Pos(), unescaped->Content, unescaped->Flags, "String");
}

TMaybe<TExprOrIdent> BuildLiteralTypedSmartStringOrId(TContext& ctx, const TString& value) {
    TExprOrIdent result;
    if (ctx.AnsiQuotedIdentifiers && value.StartsWith('"')) {
        auto unescaped = StringContentInternal(ctx, ctx.Pos(), value, EStringContentMode::AnsiIdent);
        if (!unescaped) {
            return {};
        }
        result.Ident = unescaped->Content;
        return result;
    }
    auto unescaped = StringContentInternal(ctx, ctx.Pos(), value, EStringContentMode::TypedStringLiteral);
    if (!unescaped) {
        return {};
    }

    TString type = unescaped->PgType ? *unescaped->PgType : ToString(unescaped->Type);
    result.Expr = new TLiteralNode(ctx.Pos(), unescaped->Content, unescaped->Flags, type);
    return result;
}


TNodePtr BuildLiteralRawString(TPosition pos, const TString& value, bool isUtf8) {
    return new TLiteralNode(pos, isUtf8 ? "Utf8" : "String", value);
}

TNodePtr BuildLiteralBool(TPosition pos, bool value) {
    return new TLiteralNode(pos, "Bool", value ? "true" : "false");
}

TAsteriskNode::TAsteriskNode(TPosition pos)
   : INode(pos)
{}

bool TAsteriskNode::IsAsterisk() const {
    return true;
};

TNodePtr TAsteriskNode::DoClone() const {
    return new TAsteriskNode(Pos);
}

TAstNode* TAsteriskNode::Translate(TContext& ctx) const {
    ctx.Error(Pos) << "* is not allowed here";
    return nullptr;
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

bool TDeferredAtom::GetLiteral(TString& value, TContext& ctx) const {
    if (Explicit) {
        value = *Explicit;
        return true;
    }

    ctx.Error(Node ? Node->GetPos() : ctx.Pos()) << "Expected literal value";
    return false;
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

bool TDeferredAtom::HasNode() const {
    return !!Node;
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

TTupleNode* TTupleNode::GetTupleNode() {
    return this;
}

const TTupleNode* TTupleNode::GetTupleNode() const {
    return this;
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

void TTupleNode::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    for (auto& expr : Exprs) {
        expr->CollectPreaggregateExprs(ctx, src, exprs);
    }
}

const TString* TTupleNode::GetSourceName() const {
    return DeriveCommonSourceName(Exprs);
}

TNodePtr BuildTuple(TPosition pos, const TVector<TNodePtr>& exprs) {
    return new TTupleNode(pos, exprs);
}

TStructNode::TStructNode(TPosition pos, const TVector<TNodePtr>& exprs, const TVector<TNodePtr>& labels, bool ordered)
    : TAstListNode(pos)
    , Exprs(exprs)
    , Labels(labels)
    , Ordered(ordered)
{
    YQL_ENSURE(Labels.empty() || Labels.size() == Exprs.size());
}

bool TStructNode::DoInit(TContext& ctx, ISource* src) {
    Nodes.push_back(BuildAtom(Pos, (Ordered || Exprs.size() < 2) ? "AsStruct" : "AsStructUnordered", TNodeFlags::Default));
    size_t i = 0;
    for (const auto& expr : Exprs) {
        TNodePtr label;
        if (Labels.empty()) {
            if (!expr->GetLabel()) {
                ctx.Error(expr->GetPos()) << "Structure does not allow anonymous members";
                return false;
            }
            label = BuildQuotedAtom(expr->GetPos(), expr->GetLabel());
        } else {
            label = Labels[i++];
        }
        Nodes.push_back(Q(Y(label, expr)));
    }
    return TAstListNode::DoInit(ctx, src);
}

TNodePtr TStructNode::DoClone() const {
    return new TStructNode(Pos, CloneContainer(Exprs), CloneContainer(Labels), Ordered);
}

TStructNode* TStructNode::GetStructNode() {
    return this;
}

const TStructNode* TStructNode::GetStructNode() const {
    return this;
}

void TStructNode::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    for (auto& expr : Exprs) {
        expr->CollectPreaggregateExprs(ctx, src, exprs);
    }
}

const TString* TStructNode::GetSourceName() const {
    return DeriveCommonSourceName(Exprs);
}

TNodePtr BuildStructure(TPosition pos, const TVector<TNodePtr>& exprs) {
    bool ordered = false;
    return new TStructNode(pos, exprs, {}, ordered);
}

TNodePtr BuildStructure(TPosition pos, const TVector<TNodePtr>& exprsUnlabeled, const TVector<TNodePtr>& labels) {
    bool ordered = false;
    return new TStructNode(pos, exprsUnlabeled, labels, ordered);
}

TNodePtr BuildOrderedStructure(TPosition pos, const TVector<TNodePtr>& exprsUnlabeled, const TVector<TNodePtr>& labels) {
    bool ordered = true;
    return new TStructNode(pos, exprsUnlabeled, labels, ordered);
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
    return new TListOfNamedNodes(GetPos(), CloneContainer(Exprs));
}

void TListOfNamedNodes::DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const {
    for (auto& expr : Exprs) {
        expr->VisitTree(func, visited);
    }
}

TNodePtr BuildListOfNamedNodes(TPosition pos, TVector<TNodePtr>&& exprs) {
    return new TListOfNamedNodes(pos, std::move(exprs));
}

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
    return new TArgPlaceholderNode(GetPos(), Name);
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
        , AccessOpName("AccessNode")
    {
        Y_DEBUG_ABORT_UNLESS(Ids.size() > 1);
        Y_DEBUG_ABORT_UNLESS(Ids[0].Expr);
        auto column = Ids[0].Expr->GetColumnNode();
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

    TAccessNode* GetAccessNode() override {
        return this;
    }

    const TAccessNode* GetAccessNode() const override {
        return this;
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
        auto column = expr->GetColumnNode();
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
            } else {
                continue;
            }

            if (ctx.PragmaYsonAutoConvert || ctx.PragmaYsonStrict || ctx.PragmaYsonFast) {
                auto ysonOptions = Y();
                if (ctx.PragmaYsonAutoConvert) {
                    ysonOptions->Add(BuildQuotedAtom(Pos, "yson_auto_convert"));
                }
                if (ctx.PragmaYsonStrict) {
                    ysonOptions->Add(BuildQuotedAtom(Pos, "yson_strict"));
                }
                if (ctx.PragmaYsonFast) {
                    ysonOptions->Add(BuildQuotedAtom(Pos, "yson_fast"));
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

    const TVector<TIdPart>& GetParts() const {
        return Ids;
    }

protected:
    void DoUpdateState() const override {
        YQL_ENSURE(Node);
        State.Set(ENodeState::Const, Node->IsConstant());
        State.Set(ENodeState::MaybeConst, Node->MaybeConstant());
        State.Set(ENodeState::Aggregated, Node->IsAggregated());
        State.Set(ENodeState::AggregationKey, Node->HasState(ENodeState::AggregationKey));
        State.Set(ENodeState::OverWindow, Node->IsOverWindow());
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node);
        Node->VisitTree(func, visited);
    }

    bool CheckColumnId(TPosition pos, TContext& ctx, const TIdPart& id, const TString& where, bool checkLookup) {
        if (id.Name.empty()) {
            ctx.Error(pos) << where << " name can not be empty";
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

    void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override {
        for (auto& id : Ids) {
            if (id.Expr) {
                id.Expr->CollectPreaggregateExprs(ctx, src, exprs);
            }
        }
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

TNodePtr BuildMatchRecognizeVarAccess(TPosition pos, const TString& var, const TString& column, bool theSameVar) {
    return new TMatchRecognizeVarAccessNode(pos, var, column, theSameVar);
}

void WarnIfAliasFromSelectIsUsedInGroupBy(TContext& ctx, const TVector<TNodePtr>& selectTerms, const TVector<TNodePtr>& groupByTerms,
                                          const TVector<TNodePtr>& groupByExprTerms)
{
    THashMap<TString, TNodePtr> termsByLabel;
    for (auto& term : selectTerms) {
        auto label = term->GetLabel();
        if (!label || term->IsOverWindow()) {
            continue;
        }

        auto column = term->GetColumnName();

        // do not warn for trivial renaming such as '[X.]foo AS foo'
        if (column && *column == label) {
            continue;
        }

        // skip terms with aggregation functions inside
        bool hasAggregationFunction = false;
        auto visitor = [&](const INode& current) {
            hasAggregationFunction = hasAggregationFunction || current.GetAggregation();
            return !hasAggregationFunction;
        };

        term->VisitTree(visitor);
        if (!hasAggregationFunction) {
            termsByLabel[label] = term;
        }
    }

    if (termsByLabel.empty()) {
        return;
    }

    bool found = false;
    auto visitor = [&](const INode& current) {
        if (found) {
            return false;
        }

        if (auto columnName = current.GetColumnName()) {
            // do not warn if source name is set
            auto src = current.GetSourceName();
            if (src && *src) {
                return true;
            }
            auto it = termsByLabel.find(*columnName);
            if (it != termsByLabel.end()) {
                found = true;
                ctx.Warning(current.GetPos(), TIssuesIds::YQL_PROJECTION_ALIAS_IS_REFERENCED_IN_GROUP_BY)
                    << "GROUP BY will aggregate by column `" << *columnName << "` instead of aggregating by SELECT expression with same alias";
                ctx.Warning(it->second->GetPos(), TIssuesIds::YQL_PROJECTION_ALIAS_IS_REFERENCED_IN_GROUP_BY)
                    << "You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details";
                return false;
            }
        }

        return true;
    };

    TVector<TNodePtr> originalGroupBy;
    {
        THashSet<TString> groupByExprLabels;
        for (auto& expr : groupByExprTerms) {
            auto label = expr->GetLabel();
            YQL_ENSURE(label);
            groupByExprLabels.insert(label);
        }

        originalGroupBy = groupByTerms;
        EraseIf(originalGroupBy, [&](const TNodePtr& node) {
            auto column = node->GetColumnName();
            auto src = node->GetSourceName();

            return (!src || src->empty()) && column && groupByExprLabels.contains(*column);
        });

        originalGroupBy.insert(originalGroupBy.end(), groupByExprTerms.begin(), groupByExprTerms.end());
    }

    for (auto& groupByTerm : originalGroupBy) {
        groupByTerm->VisitTree(visitor);
        if (found) {
            return;
        }
    }
}

bool ValidateAllNodesForAggregation(TContext& ctx, const TVector<TNodePtr>& nodes) {
    for (auto& node: nodes) {
        if (!node->HasState(ENodeState::Initialized) || node->IsConstant() || node->MaybeConstant()) {
            continue;
        }
        // TODO: "!node->IsOverWindow()" doesn't look right here
        if (!node->IsAggregated() && !node->IsOverWindow()) {
            // locate column which is not a key column and not aggregated
            const INode* found = nullptr;
            auto visitor = [&found](const INode& current) {
                if (found || current.IsAggregated() || current.IsOverWindow()) {
                    return false;
                }

                if (current.GetColumnNode() || current.GetAccessNode()) {
                    found = &current;
                    return false;
                }
                return true;
            };

            node->VisitTree(visitor);
            if (found) {
                TString columnName;
                if (auto col = found->GetColumnName(); col && *col) {
                    columnName = "`";
                    if (auto src = found->GetSourceName(); src && *src) {
                        columnName += DotJoin(*src, *col);
                    } else {
                        columnName += *col;
                    }
                    columnName += "` ";
                }
                ctx.Error(found->GetPos()) << "Column " << columnName << "must either be a key column in GROUP BY or it should be used in aggregation function";
            } else {
                ctx.Error(node->GetPos()) << "Expression has to be an aggregation function or key column, because aggregation is used elsewhere in this subquery";
            }

            return false;
        }
    }
    return true;
}

class TBindNode: public TAstListNode {
public:
    TBindNode(TPosition pos, const TString& module, const TString& alias)
        : TAstListNode(pos)
    {
        Add("bind", AstNode(module), BuildQuotedAtom(pos, alias));
    }
private:
    TBindNode(const TBindNode& other)
        : TAstListNode(other.GetPos())
    {
        Nodes = CloneContainer(other.Nodes);
    }

    TPtr DoClone() const final {
        return new TBindNode(*this);
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

    TLambdaNode(TPosition pos, TNodePtr params, TVector<TNodePtr> bodies)
        : TAstListNode(pos)
    {
        Add("lambda", Q(params));
        for (const auto& b : bodies) {
            Add(b);
        }
    }

    TLambdaNode* GetLambdaNode() override {
        return this;
    }

    const TLambdaNode* GetLambdaNode() const override {
        return this;
    }

private:
    TLambdaNode(const TLambdaNode& other)
        : TAstListNode(other.GetPos())
    {
        Nodes = CloneContainer(other.Nodes);
    }

    TPtr DoClone() const final {
        return new TLambdaNode(*this);
    }

    void DoUpdateState() const final {
        State.Set(ENodeState::Const);
    }
};

TNodePtr BuildLambda(TPosition pos, TNodePtr params, TNodePtr body, const TString& resName) {
    return new TLambdaNode(pos, params, body, resName);
}

TNodePtr BuildLambda(TPosition pos, TNodePtr params, const TVector<TNodePtr>& bodies) {
    return new TLambdaNode(pos, params, bodies);
}

TNodePtr BuildDataType(TPosition pos, const TString& typeName) {
    return new TCallNodeImpl(pos, "DataType", {BuildQuotedAtom(pos, typeName, TNodeFlags::Default)});
}

TMaybe<TString> LookupSimpleType(const TStringBuf& alias, bool flexibleTypes, bool isPgType) {
    TString normalized = to_lower(TString(alias));
    if (isPgType) {
        // expecting original pg type (like _int4 or varchar) with optional pg suffix (i.e. _pgint4, pgvarchar)
        if (normalized.StartsWith("pg")) {
            normalized = normalized.substr(2);
        } else if (normalized.StartsWith("_pg")) {
            normalized = "_" + normalized.substr(3);
        }

        if (!NPg::HasType(normalized)) {
            return {};
        }

        if (normalized.StartsWith("_")) {
            return "_pg" + normalized.substr(1);
        }
        return "pg" + normalized;
    }

    if (auto sqlAlias = LookupSimpleTypeBySqlAlias(alias, flexibleTypes)) {
        return TString(*sqlAlias);
    }

    TString pgType;
    if (normalized.StartsWith("_pg")) {
        pgType = normalized.substr(3);
    } else if (normalized.StartsWith("pg")) {
        pgType = normalized.substr(2);
    } else {
        return {};
    }

    if (NPg::HasType(pgType)) {
        return normalized;
    }

    return {};
}

TNodePtr BuildSimpleType(TContext& ctx, TPosition pos, const TString& typeName, bool dataOnly) {
    bool explicitPgType = ctx.GetColumnReferenceState() == EColumnRefState::AsPgType;
    auto found = LookupSimpleType(typeName, ctx.FlexibleTypes, explicitPgType);
    if (!found) {
        ctx.Error(pos) << "Unknown " << (explicitPgType ? "pg" : "simple") <<  " type '" << typeName << "'";
        return {};
    }

    auto type = *found;
    if (type == "Void" || type == "Unit" || type == "Generic" || type == "EmptyList" || type == "EmptyDict") {
        if (dataOnly) {
            ctx.Error(pos) << "Only data types are allowed here, but got: '" << typeName << "'";
            return {};
        }
        type += "Type";
        return new TCallNodeImpl(pos, type, {});
    }

    if (type.StartsWith("_pg") || type.StartsWith("pg")) {
        TString pgType;
        if (type.StartsWith("_pg")) {
            pgType = "_" + type.substr(3);
        } else {
            pgType = type.substr(2);
        }
        return new TCallNodeImpl(pos, "PgType", { BuildQuotedAtom(pos, pgType, TNodeFlags::Default) });
    }

    return new TCallNodeImpl(pos, "DataType", { BuildQuotedAtom(pos, type, TNodeFlags::Default) });
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
        return BuildLiteralBool(pos, true);
    }
    return new TCallNodeImpl(pos, "Not", {new TCallNodeImpl(pos, "Exists", {a})});
}



TUdfNode::TUdfNode(TPosition pos, const TVector<TNodePtr>& args)
    : INode(pos)
    , Args(args)
{
    if (Args.size()) {
        // If there aren't any named args, args are passed as vector of positional args,
        // else Args has length 2: tuple for positional args and struct for named args,
        // so let's construct tuple of args there. Other type checks will within DoInit call.
        if (!Args[0]->GetTupleNode()) {
            Args = {BuildTuple(pos, args)};
        }
    }
}

bool TUdfNode::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(src);
    if (Args.size() < 1) {
        ctx.Error(Pos) << "Udf: expected at least one argument";
        return false;
    }

    TTupleNode* as_tuple = Args[0]->GetTupleNode();

    if (!as_tuple || as_tuple->GetTupleSize() < 1) {
        ctx.Error(Pos) << "Udf: first argument must be a callable, like Foo::Bar";
        return false;
    }

    TNodePtr function = as_tuple->GetTupleElement(0);

    if (!function || !function->FuncName()) {
        ctx.Error(Pos) << "Udf: first argument must be a callable, like Foo::Bar";
        return false;
    }

    FunctionName = function->FuncName();
    ModuleName = function->ModuleName();
    TVector<TNodePtr> external;
    external.reserve(as_tuple->GetTupleSize() - 1);

    for (size_t i = 1; i < as_tuple->GetTupleSize(); ++i) {
        // TODO(): support named args in GetFunctionArgColumnStatus
        TNodePtr current = as_tuple->GetTupleElement(i);
        if (TAccessNode* as_access = current->GetAccessNode(); as_access) {
            external.push_back(Y("DataType", Q(as_access->GetParts()[1].Name)));
            continue;
        }
        external.push_back(current);
    }

    ExternalTypesTuple = new TCallNodeImpl(Pos, "TupleType", external);

    if (Args.size() == 1) {
        return true;
    }

    if (TStructNode* named_args = Args[1]->GetStructNode(); named_args) {
        for (const auto &arg: named_args->GetExprs()) {
            if (arg->GetLabel() == "TypeConfig") {
                TypeConfig = MakeAtomFromExpression(Pos, ctx, arg);
            } else if (arg->GetLabel() == "RunConfig") {
                RunConfig = arg;
            }
        }
    }

    return true;
}

const TNodePtr TUdfNode::GetExternalTypes() const {
    return ExternalTypesTuple;
}

const TString& TUdfNode::GetFunction() const {
    return *FunctionName;
}

const TString& TUdfNode::GetModule() const {
    return *ModuleName;
}

TNodePtr TUdfNode::GetRunConfig() const {
    return RunConfig;
}

const TDeferredAtom& TUdfNode::GetTypeConfig() const {
    return TypeConfig;
}

TUdfNode* TUdfNode::GetUdfNode() {
    return this;
}

const TUdfNode* TUdfNode::GetUdfNode() const {
    return this;
}

TAstNode* TUdfNode::Translate(TContext& ctx) const {
    ctx.Error(Pos) << "Abstract Udf Node can't be used as a part of expression.";
    return nullptr;
}

TNodePtr TUdfNode::DoClone() const {
    return new TUdfNode(Pos, CloneContainer(Args));
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

TNodePtr BuildBinaryOp(TContext& ctx, TPosition pos, const TString& opName, TNodePtr a, TNodePtr b) {
    if (!a || !b) {
        return nullptr;
    }

    static const THashSet<TStringBuf> nullSafeOps = {"IsDistinctFrom", "IsNotDistinctFrom"};
    if (!nullSafeOps.contains(opName)) {
        const bool bothArgNull = a->IsNull() && b->IsNull();
        const bool oneArgNull  = a->IsNull() || b->IsNull();

        if (bothArgNull || (oneArgNull && opName != "Or" && opName != "And")) {
            ctx.Warning(pos, TIssuesIds::YQL_OPERATION_WILL_RETURN_NULL) << "Binary operation "
            << opName.substr(0, opName.Size() - 7 * opName.EndsWith("MayWarn"))
            << " will return NULL here";
        }
    }

    return new TBinaryOpNode(pos, opName, a, b);
}

TNodePtr BuildBinaryOpRaw(TPosition pos, const TString& opName, TNodePtr a, TNodePtr b) {
    if (!a || !b) {
        return nullptr;
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
        State.Set(ENodeState::MaybeConst, FuncNode->MaybeConstant());
        State.Set(ENodeState::Aggregated, FuncNode->IsAggregated());
        State.Set(ENodeState::OverWindow, true);
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(FuncNode);
        FuncNode->VisitTree(func, visited);
    }
protected:
    const TString WindowName;
    TNodePtr FuncNode;
};

TNodePtr BuildCalcOverWindow(TPosition pos, const TString& windowName, TNodePtr call) {
    return new TCalcOverWindow(pos, windowName, call);
}

template<bool Fast>
class TYsonOptionsNode final: public INode {
public:
    TYsonOptionsNode(TPosition pos, bool autoConvert, bool strict)
        : INode(pos)
        , AutoConvert(autoConvert)
        , Strict(strict)
    {
        auto udf = Y("Udf", Q(Fast ? "Yson2.Options" : "Yson.Options"));
        auto autoConvertNode = BuildLiteralBool(pos, autoConvert);
        autoConvertNode->SetLabel("AutoConvert");
        auto strictNode = BuildLiteralBool(pos, strict);
        strictNode->SetLabel("Strict");
        Node = Y("NamedApply", udf, Q(Y()), BuildStructure(pos, { autoConvertNode, strictNode }));
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node->Translate(ctx);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Node->Init(ctx, src)) {
            return false;
        }
        return true;
    }

    TPtr DoClone() const final {
        return new TYsonOptionsNode(Pos, AutoConvert, Strict);
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, true);
    }

protected:
    TNodePtr Node;
    const bool AutoConvert;
    const bool Strict;
};

TNodePtr BuildYsonOptionsNode(TPosition pos, bool autoConvert, bool strict, bool fastYson) {
    if (fastYson)
        return new TYsonOptionsNode<true>(pos, autoConvert, strict);
    else
        return new TYsonOptionsNode<false>(pos, autoConvert, strict);
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
        if (!Node->Init(ctx, FakeSource.Get())) {
            return false;
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const final {
        return Node->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TDoCall(Pos, Node->Clone());
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node);
        Node->VisitTree(func, visited);
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

TSourcePtr TryMakeSourceFromExpression(TPosition pos, TContext& ctx, const TString& currService, const TDeferredAtom& currCluster,
    TNodePtr node, const TString& view) {
    if (currCluster.Empty()) {
        ctx.Error() << "No cluster name given and no default cluster is selected";
        return nullptr;
    }

    if (auto literal = node->GetLiteral("String")) {
        TNodePtr tableKey = BuildTableKey(node->GetPos(), currService, currCluster, TDeferredAtom(node->GetPos(), *literal), {view});
        TTableRef table(ctx.MakeName("table"), currService, currCluster, tableKey);
        table.Options = BuildInputOptions(node->GetPos(), GetContextHints(ctx));
        return BuildTableSource(node->GetPos(), table);
    }

    if (node->GetLambdaNode()) {
        ctx.Error() << "Lambda is not allowed to be used as source. Did you forget to call a subquery template?";
        return nullptr;
    }

    auto wrappedNode = new TAstListNodeImpl(pos, { 
        new TAstAtomNodeImpl(pos, "EvaluateAtom", TNodeFlags::Default),
        node
    });

    TNodePtr tableKey = BuildTableKey(node->GetPos(), currService, currCluster, TDeferredAtom(wrappedNode, ctx), {view});
    TTableRef table(ctx.MakeName("table"), currService, currCluster, tableKey);
    table.Options = BuildInputOptions(node->GetPos(), GetContextHints(ctx));
    return BuildTableSource(node->GetPos(), table);
}

void MakeTableFromExpression(TPosition pos, TContext& ctx, TNodePtr node, TDeferredAtom& table, const TString& prefix) {
    if (auto literal = node->GetLiteral("String")) {
        table = TDeferredAtom(node->GetPos(), prefix + *literal);
        return;
    }

    if (auto access = node->GetAccessNode()) {
        auto ret = access->TryMakeTable();
        if (ret) {
            table = TDeferredAtom(node->GetPos(), prefix + *ret);
            return;
        }
    }

    if (!prefix.Empty()) {
        node = node->Y("Concat", node->Y("String", node->Q(prefix)), node);
    }

    auto wrappedNode = new TAstListNodeImpl(pos, { 
        new TAstAtomNodeImpl(pos, "EvaluateAtom", TNodeFlags::Default),
        node
    });

    table = TDeferredAtom(wrappedNode, ctx);
}

TDeferredAtom MakeAtomFromExpression(TPosition pos, TContext& ctx, TNodePtr node, const TString& prefix) {
    if (auto literal = node->GetLiteral("String")) {
        return TDeferredAtom(node->GetPos(), prefix + *literal);
    }

    if (!prefix.Empty()) {
        node = node->Y("Concat", node->Y("String", node->Q(prefix)), node);
    }

    auto wrappedNode = new TAstListNodeImpl(pos, { 
        new TAstAtomNodeImpl(pos, "EvaluateAtom", TNodeFlags::Default),
        node
    });

    return TDeferredAtom(wrappedNode, ctx);
}

class TTupleResultNode: public INode {
public:
    TTupleResultNode(TNodePtr&& tuple, size_t ensureTupleSize)
        : INode(tuple->GetPos())
        , Node(std::move(tuple))
        , EnsureTupleSize(ensureTupleSize)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Node->Init(ctx, src)) {
            return false;
        }

        Node = Y("EnsureTupleSize", Node, Q(ToString(EnsureTupleSize)));

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TTupleResultNode(Node->Clone(), EnsureTupleSize);
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node);
        Node->VisitTree(func, visited);
    }
protected:
    TNodePtr Node;
    const size_t EnsureTupleSize;
};

TNodePtr BuildTupleResult(TNodePtr tuple, size_t ensureTupleSize) {
    return new TTupleResultNode(std::move(tuple), ensureTupleSize);
}

class TNamedExprReferenceNode: public IProxyNode {
public:
    TNamedExprReferenceNode(TNodePtr parent, const TString& name, TMaybe<size_t> tupleIndex)
        : IProxyNode(parent->GetPos(), parent)
        , Name(name)
        , TupleIndex(tupleIndex)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        Y_UNUSED(src);
        if (!IProxyNode::DoInit(ctx, nullptr) || !IProxyNode::InitReference(ctx)) {
            return false;
        }

        Node = BuildAtom(GetPos(), Name, TNodeFlags::Default);
        if (TupleIndex.Defined()) {
            Node = Y("Nth", Node, Q(ToString(*TupleIndex)));
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        YQL_ENSURE(Node, "Init() should be done before Translate()");
        return Node->Translate(ctx);
    }

    TPtr DoClone() const final {
        // do not clone Inner here
        return new TNamedExprReferenceNode(Inner, Name, TupleIndex);
    }

private:
    const TString Name;
    const TMaybe<size_t> TupleIndex;
    TNodePtr Node;
};

TNodePtr BuildNamedExprReference(TNodePtr parent, const TString& name, TMaybe<size_t> tupleIndex) {
    YQL_ENSURE(parent);
    return new TNamedExprReferenceNode(parent, name, tupleIndex);
}

class TNamedExprNode: public IProxyNode {
public:
    TNamedExprNode(TNodePtr parent)
        : IProxyNode(parent->GetPos(), parent)
        , FakeSource(BuildFakeSource(parent->GetPos()))
        , Referenced(false)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        YQL_ENSURE(!Referenced, "Refrence is initialized before named expr itself");
        Y_UNUSED(src);
        if (ctx.ValidateUnusedExprs) {
            return IProxyNode::DoInit(ctx, FakeSource.Get());
        }
        // do actual init in InitReference()
        return true;
    }

    bool InitReference(TContext& ctx) final {
        Referenced = true;
        return IProxyNode::DoInit(ctx, FakeSource.Get());
    }

    TAstNode* Translate(TContext& ctx) const override {
        if (ctx.ValidateUnusedExprs || Referenced) {
            return Inner->Translate(ctx);
        }
        auto unused = BuildQuotedAtom(GetPos(), "unused", TNodeFlags::Default);
        return unused->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TNamedExprNode(Inner->Clone());
    }

private:
    const TSourcePtr FakeSource;
    bool Referenced;
};

TNodePtr BuildNamedExpr(TNodePtr parent) {
    YQL_ENSURE(parent);
    return new TNamedExprNode(parent);
}

bool TVectorIndexSettings::Validate(TContext& ctx) const {
    if (Metric.index() == 0) {
        ctx.Error() << "either distance or similarity should be set";
        return false;
    } 
    if (!VectorType) {
        ctx.Error() << "vector_type should be set";
        return false;
    }
    if (!VectorDimension) {
        ctx.Error() << "vector_dimension should be set";
        return false;
    }
    return true;
}

} // namespace NSQLTranslationV1
