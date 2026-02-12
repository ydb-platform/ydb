#include "node.h"
#include "source.h"
#include "context.h"

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/sql_types/simple_types.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/utils/yql_panic.h>

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
                               " add it if necessary to FROM section over 'AS <alias>' keyword and put it like '<alias>."
                            << column << "'";
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

INode::INode(TPosition pos)
    : Pos_(pos)
{
}

INode::~INode()
{
}

TPosition INode::GetPos() const {
    return Pos_;
}

const TString& INode::GetLabel() const {
    return Label_;
}

TMaybe<TPosition> INode::GetLabelPos() const {
    return LabelPos_;
}

void INode::SetLabel(const TString& label, TMaybe<TPosition> pos) {
    Label_ = label;
    LabelPos_ = pos;
}

bool INode::IsImplicitLabel() const {
    return ImplicitLabel_;
}

void INode::MarkImplicitLabel(bool isImplicitLabel) {
    ImplicitLabel_ = isImplicitLabel;
}

void INode::SetRefPos(TPosition pos) {
    RefPos_ = pos;
}

TMaybe<TPosition> INode::GetRefPos() const {
    return RefPos_;
}

void INode::SetCountHint(bool isCount) {
    State_.Set(ENodeState::CountHint, isCount);
}

bool INode::GetCountHint() const {
    return State_.Test(ENodeState::CountHint);
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

bool INode::IsOverWindowDistinct() const {
    return HasState(ENodeState::OverWindowDistinct);
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
    return new TCallNodeImpl(pos, opName, {Clone()});
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

bool INode::IsPlainColumn() const {
    return GetColumnName() != nullptr;
}

bool INode::IsTableRow() const {
    return false;
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
    if (State_.Test(ENodeState::Failed)) {
        return false;
    }

    if (!State_.Test(ENodeState::Initialized)) {
        if (!DoInit(ctx, src)) {
            State_.Set(ENodeState::Failed);
            return false;
        }
        State_.Set(ENodeState::Initialized);
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
    return new TAstListNodeImpl(Pos_);
}

TNodePtr INode::AstNode(TNodePtr node) const {
    return node;
}

TNodePtr INode::AstNode(const TString& str) const {
    return new TAstAtomNodeImpl(Pos_, str, TNodeFlags::Default);
}

TNodePtr INode::AstNode(TAstNode* node) const {
    return new TAstDirectNode(node);
}

TNodePtr INode::Clone() const {
    TNodePtr clone = DoClone();
    if (!clone) {
        clone = const_cast<INode*>(this);
    } else {
        YQL_ENSURE(!State_.Test(ENodeState::Initialized), "Clone should be for uninitialized or persistent node");
        clone->SetLabel(Label_, LabelPos_);
        clone->MarkImplicitLabel(ImplicitLabel_);
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
    AsInner_ = true;
}

void INode::DisableSort() {
    DisableSort_ = true;
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

bool INode::IsScript() const {
    return false;
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
    if (State_.Test(ENodeState::Failed)) {
        return;
    }

    /// Not work right now! It's better use Init at first, because some kind of update depend on it
    /// \todo turn on and remove all issues
    // Y_DEBUG_ABORT_UNLESS(State.Test(ENodeState::Initialized));
    if (State_.Test(ENodeState::Precached)) {
        return;
    }
    DoUpdateState();
    State_.Set(ENodeState::Precached);
}

void INode::DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const {
    Y_UNUSED(func);
    Y_UNUSED(visited);
}

void INode::DoAdd(TNodePtr node) {
    Y_UNUSED(node);
    Y_DEBUG_ABORT_UNLESS(false, "Node is not expandable");
}

bool Init(TContext& ctx, ISource* src, const TVector<TNodePtr>& nodes) {
    for (const TNodePtr& node : nodes) {
        if (!node->Init(ctx, src)) {
            return false;
        }
    }
    return true;
}

TNodeResult Wrap(TNodePtr node) {
    if (!node) {
        return std::unexpected(ESQLError::Basic);
    }

    return TNonNull(std::move(node));
}

TNodePtr Unwrap(TNodeResult result) {
    EnsureUnwrappable(result);

    return result
               ? TNodePtr(std::move(*result))
               : nullptr;
}

bool IProxyNode::IsNull() const {
    return Inner_->IsNull();
}

bool IProxyNode::IsLiteral() const {
    return Inner_->IsNull();
}

TString IProxyNode::GetLiteralType() const {
    return Inner_->GetLiteralType();
}

TString IProxyNode::GetLiteralValue() const {
    return Inner_->GetLiteralValue();
}

bool IProxyNode::IsIntegerLiteral() const {
    return Inner_->IsIntegerLiteral();
}

INode::TPtr IProxyNode::ApplyUnaryOp(TContext& ctx, TPosition pos, const TString& opName) const {
    return Inner_->ApplyUnaryOp(ctx, pos, opName);
}

bool IProxyNode::IsAsterisk() const {
    return Inner_->IsAsterisk();
}

const TString* IProxyNode::SubqueryAlias() const {
    return Inner_->SubqueryAlias();
}

TString IProxyNode::GetOpName() const {
    return Inner_->GetOpName();
}

const TString* IProxyNode::GetLiteral(const TString& type) const {
    return Inner_->GetLiteral(type);
}

const TString* IProxyNode::GetColumnName() const {
    return Inner_->GetColumnName();
}

bool IProxyNode::IsPlainColumn() const {
    return Inner_->IsPlainColumn();
}

bool IProxyNode::IsTableRow() const {
    return Inner_->IsTableRow();
}

void IProxyNode::AssumeColumn() {
    Inner_->AssumeColumn();
}

const TString* IProxyNode::GetSourceName() const {
    return Inner_->GetSourceName();
}

const TString* IProxyNode::GetAtomContent() const {
    return Inner_->GetAtomContent();
}

bool IProxyNode::IsOptionalArg() const {
    return Inner_->IsOptionalArg();
}

size_t IProxyNode::GetTupleSize() const {
    return Inner_->GetTupleSize();
}

INode::TPtr IProxyNode::GetTupleElement(size_t index) const {
    return Inner_->GetTupleElement(index);
}

ITableKeys* IProxyNode::GetTableKeys() {
    return Inner_->GetTableKeys();
}

ISource* IProxyNode::GetSource() {
    return Inner_->GetSource();
}

TVector<INode::TPtr>* IProxyNode::ContentListPtr() {
    return Inner_->ContentListPtr();
}

TAggregationPtr IProxyNode::GetAggregation() const {
    return Inner_->GetAggregation();
}

void IProxyNode::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    Inner_->CollectPreaggregateExprs(ctx, src, exprs);
}

INode::TPtr IProxyNode::WindowSpecFunc(const TPtr& type) const {
    return Inner_->WindowSpecFunc(type);
}

bool IProxyNode::SetViewName(TContext& ctx, TPosition pos, const TString& view) {
    return Inner_->SetViewName(ctx, pos, view);
}

bool IProxyNode::SetPrimaryView(TContext& ctx, TPosition pos) {
    return Inner_->SetPrimaryView(ctx, pos);
}

bool IProxyNode::UsedSubquery() const {
    return Inner_->UsedSubquery();
}

bool IProxyNode::IsSelect() const {
    return Inner_->IsSelect();
}

bool IProxyNode::HasSelectResult() const {
    return Inner_->HasSelectResult();
}

const TString* IProxyNode::FuncName() const {
    return Inner_->FuncName();
}

const TString* IProxyNode::ModuleName() const {
    return Inner_->ModuleName();
}

bool IProxyNode::IsScript() const {
    return Inner_->IsScript();
}

bool IProxyNode::HasSkip() const {
    return Inner_->HasSkip();
}

TColumnNode* IProxyNode::GetColumnNode() {
    return Inner_->GetColumnNode();
}

const TColumnNode* IProxyNode::GetColumnNode() const {
    return static_cast<const INode*>(Inner_.Get())->GetColumnNode();
}

TTupleNode* IProxyNode::GetTupleNode() {
    return Inner_->GetTupleNode();
}

const TTupleNode* IProxyNode::GetTupleNode() const {
    return static_cast<const INode*>(Inner_.Get())->GetTupleNode();
}

TCallNode* IProxyNode::GetCallNode() {
    return Inner_->GetCallNode();
}

const TCallNode* IProxyNode::GetCallNode() const {
    return static_cast<const INode*>(Inner_.Get())->GetCallNode();
}

TStructNode* IProxyNode::GetStructNode() {
    return Inner_->GetStructNode();
}

const TStructNode* IProxyNode::GetStructNode() const {
    return static_cast<const INode*>(Inner_.Get())->GetStructNode();
}

TAccessNode* IProxyNode::GetAccessNode() {
    return Inner_->GetAccessNode();
}

const TAccessNode* IProxyNode::GetAccessNode() const {
    return static_cast<const INode*>(Inner_.Get())->GetAccessNode();
}

TLambdaNode* IProxyNode::GetLambdaNode() {
    return Inner_->GetLambdaNode();
}

const TLambdaNode* IProxyNode::GetLambdaNode() const {
    return static_cast<const INode*>(Inner_.Get())->GetLambdaNode();
}

TUdfNode* IProxyNode::GetUdfNode() {
    return Inner_->GetUdfNode();
}

const TUdfNode* IProxyNode::GetUdfNode() const {
    return static_cast<const INode*>(Inner_.Get())->GetUdfNode();
}

void IProxyNode::DoUpdateState() const {
    static_assert(static_cast<int>(ENodeState::End) == 10, "Need to support new states here");
    State_.Set(ENodeState::CountHint, Inner_->GetCountHint());
    State_.Set(ENodeState::Const, Inner_->IsConstant());
    State_.Set(ENodeState::MaybeConst, Inner_->MaybeConstant());
    State_.Set(ENodeState::Aggregated, Inner_->IsAggregated());
    State_.Set(ENodeState::AggregationKey, Inner_->IsAggregationKey());
    State_.Set(ENodeState::OverWindow, Inner_->IsOverWindow());
    State_.Set(ENodeState::OverWindowDistinct, Inner_->IsOverWindowDistinct());
}

void IProxyNode::DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const {
    Inner_->VisitTree(func, visited);
}

bool IProxyNode::InitReference(TContext& ctx) {
    return Inner_->InitReference(ctx);
}

bool IProxyNode::DoInit(TContext& ctx, ISource* src) {
    return Inner_->Init(ctx, src);
}

void IProxyNode::DoAdd(TPtr node) {
    Inner_->Add(node);
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
    , Content_(content)
    , Flags_(flags)
    , IsOptionalArg_(isOptionalArg)
{
}

TAstAtomNode::~TAstAtomNode()
{
}

void TAstAtomNode::DoUpdateState() const {
    State_.Set(ENodeState::Const);
}

TAstNode* TAstAtomNode::Translate(TContext& ctx) const {
    return TAstNode::NewAtom(Pos_, Content_, *ctx.Pool, Flags_);
}

const TString* TAstAtomNode::GetAtomContent() const {
    return &Content_;
}

bool TAstAtomNode::IsOptionalArg() const {
    return IsOptionalArg_;
}

TAstDirectNode::TAstDirectNode(TAstNode* node)
    : INode(node->GetPosition())
    , Node_(node)
{
}

TAstNode* TAstDirectNode::Translate(TContext& ctx) const {
    Y_UNUSED(ctx);
    return Node_;
}

TNodePtr BuildList(TPosition pos, TVector<TNodePtr> nodes) {
    return new TAstListNodeImpl(pos, std::move(nodes));
}

TNodePtr BuildQuote(TPosition pos, TNodePtr expr) {
    return BuildList(pos, {BuildAtom(pos, "quote", TNodeFlags::Default), expr});
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
    for (auto& node : Nodes_) {
        if (!node->Init(ctx, src)) {
            return false;
        }
    }
    return true;
}

TAstNode* TAstListNode::Translate(TContext& ctx) const {
    TSmallVec<TAstNode*> children;
    children.reserve(Nodes_.size());
    auto listPos = Pos_;
    for (auto& node : Nodes_) {
        if (node) {
            auto astNode = node->Translate(ctx);
            if (!astNode) {
                return nullptr;
            }
            children.push_back(astNode);
        } else {
            ctx.Error(Pos_) << "Translation error: encountered empty TNodePtr";
            return nullptr;
        }
    }

    return TAstNode::NewList(listPos, children.data(), children.size(), *ctx.Pool);
}

void TAstListNode::UpdateStateByListNodes(const TVector<TNodePtr>& nodes) const {
    bool isConst = true;
    struct TAttributesFlags {
        bool Has = false;
        bool All = true;
    };
    std::array<ENodeState, 3> checkStates = {{ENodeState::Aggregated, ENodeState::AggregationKey, ENodeState::OverWindow}};
    std::map<ENodeState, TAttributesFlags> flags;
    for (auto& node : nodes) {
        const bool isNodeConst = node->IsConstant();
        const bool isNodeMaybeConst = node->MaybeConstant();
        for (auto state : checkStates) {
            if (node->HasState(state)) {
                flags[state].Has = true;
            } else if (!isNodeConst && !isNodeMaybeConst) {
                flags[state].All = false;
            }

            if (!isNodeConst) {
                isConst = false;
            }
        }
    }
    State_.Set(ENodeState::Const, isConst);
    for (auto& flag : flags) {
        State_.Set(flag.first, flag.second.Has && flag.second.All);
    }
    State_.Set(ENodeState::MaybeConst, !isConst && AllOf(nodes, [](const auto& node) { return node->IsConstant() || node->MaybeConstant(); }));
}

void TAstListNode::DoUpdateState() const {
    UpdateStateByListNodes(Nodes_);
}

void TAstListNode::DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const {
    for (auto& node : Nodes_) {
        node->VisitTree(func, visited);
    }
}

TAstListNode::TAstListNode(const TAstListNode& node)
    : INode(node.Pos_)
    , Nodes_(node.Nodes_)
{
    Label_ = node.Label_;
    State_ = node.State_;
}

TAstListNode::TAstListNode(TPosition pos, TVector<TNodePtr>&& nodes)
    : INode(pos)
    , Nodes_(std::move(nodes))
{
    for (const auto& node : Nodes_) {
        YQL_ENSURE(node, "Null ptr passed as list element");
    }
}

TNodePtr TAstListNode::ShallowCopy() const {
    return new TAstListNodeImpl(Pos_, Nodes_);
}

void TAstListNode::DoAdd(TNodePtr node) {
    Y_DEBUG_ABORT_UNLESS(node);
    Y_DEBUG_ABORT_UNLESS(node.Get() != this);
    Nodes_.push_back(node);
}

TAstListNodeImpl::TAstListNodeImpl(TPosition pos)
    : TAstListNode(pos)
{
}

TAstListNodeImpl::TAstListNodeImpl(TPosition pos, TVector<TNodePtr> nodes)
    : TAstListNode(pos)
{
    for (const auto& node : nodes) {
        YQL_ENSURE(node, "Null ptr passed as list element");
    }
    Nodes_.swap(nodes);
}

void TAstListNodeImpl::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    for (auto& node : Nodes_) {
        node->CollectPreaggregateExprs(ctx, src, exprs);
    }
}

const TString* TAstListNodeImpl::GetSourceName() const {
    return DeriveCommonSourceName(Nodes_);
}

TNodePtr TAstListNodeImpl::DoClone() const {
    return new TAstListNodeImpl(Pos_, CloneContainer(Nodes_));
}

TCallNode::TCallNode(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TAstListNode(pos)
    , OpName_(opName)
    , MinArgs_(minArgs)
    , MaxArgs_(maxArgs)
    , Args_(args)
{
    for (const auto& arg : Args_) {
        YQL_ENSURE(arg, "Null ptr passed as call argument");
    }
}

TString TCallNode::GetOpName() const {
    return OpName_;
}

const TString* DeriveCommonSourceName(const TVector<TNodePtr>& nodes) {
    const TString* name = nullptr;
    for (auto& node : nodes) {
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
    return DeriveCommonSourceName(Args_);
}

const TVector<TNodePtr>& TCallNode::GetArgs() const {
    return Args_;
}

void TCallNode::DoUpdateState() const {
    UpdateStateByListNodes(Args_);
}

TString TCallNode::GetCallExplain() const {
    auto derivedName = GetOpName();
    TStringBuilder sb;
    sb << derivedName << "()";
    if (derivedName != OpName_) {
        sb << ", converted to " << OpName_ << "()";
    }
    return std::move(sb);
}

void TCallNode::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    for (auto& arg : Args_) {
        arg->CollectPreaggregateExprs(ctx, src, exprs);
    }
}

bool TCallNode::ValidateArguments(TContext& ctx) const {
    const auto argsCount = static_cast<i32>(Args_.size());
    if (MinArgs_ >= 0 && MaxArgs_ == MinArgs_ && argsCount != MinArgs_) {
        ctx.Error(Pos_) << GetCallExplain() << " requires exactly " << MinArgs_ << " arguments, given: " << Args_.size();
        return false;
    }

    if (MinArgs_ >= 0 && argsCount < MinArgs_) {
        ctx.Error(Pos_) << GetCallExplain() << " requires at least " << MinArgs_ << " arguments, given: " << Args_.size();
        return false;
    }

    if (MaxArgs_ >= 0 && argsCount > MaxArgs_) {
        ctx.Error(Pos_) << GetCallExplain() << " requires at most " << MaxArgs_ << " arguments, given: " << Args_.size();
        return false;
    }

    return true;
}

bool TCallNode::DoInit(TContext& ctx, ISource* src) {
    if (!ValidateArguments(ctx)) {
        return false;
    }

    bool hasError = false;
    for (auto& arg : Args_) {
        if (!arg->Init(ctx, src)) {
            hasError = true;
            continue;
        }
    }

    if (hasError) {
        return false;
    }

    Nodes_.push_back(BuildAtom(Pos_, OpName_,
                               OpName_.cend() == std::find_if_not(OpName_.cbegin(), OpName_.cend(), [](char c) { return bool(std::isalnum(c)); }) ? TNodeFlags::Default : TNodeFlags::ArbitraryContent));
    Nodes_.insert(Nodes_.end(), Args_.begin(), Args_.end());
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
{
}

TCallNodeImpl::TCallNodeImpl(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, args.size(), args.size(), args)
{
}

TCallNode::TPtr TCallNodeImpl::DoClone() const {
    return new TCallNodeImpl(GetPos(), OpName_, MinArgs_, MaxArgs_, CloneContainer(Args_));
}

TFuncNodeImpl::TFuncNodeImpl(TPosition pos, const TString& opName)
    : TCallNode(pos, opName, 0, 0, {})
{
}

TCallNode::TPtr TFuncNodeImpl::DoClone() const {
    return new TFuncNodeImpl(GetPos(), OpName_);
}

const TString* TFuncNodeImpl::FuncName() const {
    return &OpName_;
}

TCallNodeDepArgs::TCallNodeDepArgs(ui32 reqArgsCount, TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, minArgs, maxArgs, args)
    , ReqArgsCount_(reqArgsCount)
{
}

TCallNodeDepArgs::TCallNodeDepArgs(ui32 reqArgsCount, TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, args.size(), args.size(), args)
    , ReqArgsCount_(reqArgsCount)
{
}

TCallNode::TPtr TCallNodeDepArgs::DoClone() const {
    return new TCallNodeDepArgs(ReqArgsCount_, GetPos(), OpName_, MinArgs_, MaxArgs_, CloneContainer(Args_));
}

bool TCallNodeDepArgs::DoInit(TContext& ctx, ISource* src) {
    if (!TCallNode::DoInit(ctx, src)) {
        return false;
    }

    for (ui32 i = 1 + ReqArgsCount_; i < Nodes_.size(); ++i) {
        Nodes_[i] = Y("DependsOn", Nodes_[i]);
    }
    return true;
}

INode::TPtr TCallDirectRow::DoClone() const {
    return new TCallDirectRow(Pos_, OpName_, CloneContainer(Args_));
}

TCallDirectRow::TCallDirectRow(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, minArgs, maxArgs, args)
{
}

TCallDirectRow::TCallDirectRow(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, 0, 0, args)
{
}

bool TCallDirectRow::DoInit(TContext& ctx, ISource* src) {
    if (!src || (ctx.CompactNamedExprs && src->IsFake())) {
        ctx.Error(Pos_) << "Unable to use function: " << OpName_ << " without source";
        return false;
    }
    if (src->IsCompositeSource() || src->GetJoin() || src->HasAggregations() || src->IsFlattenByColumns() || src->IsOverWindowSource()) {
        ctx.Error(Pos_) << "Failed to use function: " << OpName_ << " with aggregation, join, flatten by or window functions";
        return false;
    }
    if (!TCallNode::DoInit(ctx, src)) {
        return false;
    }

    if (ctx.DirectRowDependsOn.GetOrElse(true)) {
        Nodes_.push_back(Y("DependsOn", "row"));
    } else {
        Nodes_.push_back(AstNode("row"));
    }

    return true;
}

void TCallDirectRow::DoUpdateState() const {
    State_.Set(ENodeState::Const, false);
}

void TWinAggrEmulation::DoUpdateState() const {
    State_.Set(ENodeState::OverWindow, true);
}

bool TWinAggrEmulation::DoInit(TContext& ctx, ISource* src) {
    if (!src) {
        ctx.Error(Pos_) << "Unable to use window function " << OpName_ << " without source";
        return false;
    }

    if (!src->IsOverWindowSource()) {
        ctx.Error(Pos_) << "Failed to use window function " << OpName_ << " without window specification";
        return false;
    }
    if (!src->AddFuncOverWindow(ctx, this)) {
        ctx.Error(Pos_) << "Failed to use window function " << OpName_ << " without window specification or in wrong place";
        return false;
    }

    FuncAlias_ = "_yql_" + src->MakeLocalName(OpName_);
    src->AddTmpWindowColumn(FuncAlias_);
    if (!TCallNode::DoInit(ctx, src)) {
        return false;
    }
    Nodes_.clear();
    Add("Member", "row", Q(FuncAlias_));
    return true;
}

INode::TPtr TWinAggrEmulation::WindowSpecFunc(const TPtr& type) const {
    auto result = Y(OpName_, type);
    for (const auto& arg : Args_) {
        result = L(result, arg);
    }
    return Q(Y(Q(FuncAlias_), result));
}

TWinAggrEmulation::TWinAggrEmulation(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TCallNode(pos, opName, minArgs, maxArgs, args)
    , FuncAlias_(opName)
{
}

TWinRowNumber::TWinRowNumber(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{
}

TWinCumeDist::TWinCumeDist(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{
}

bool TWinCumeDist::DoInit(TContext& ctx, ISource* src) {
    if (!ValidateArguments(ctx)) {
        return false;
    }

    YQL_ENSURE(Args_.size() == 0);
    TVector<TNodePtr> optionsElements;
    if (ctx.AnsiCurrentRow) {
        optionsElements.push_back(BuildTuple(Pos_, {BuildQuotedAtom(Pos_, "ansi", NYql::TNodeFlags::Default)}));
    }
    Args_.push_back(BuildTuple(Pos_, optionsElements));

    MinArgs_ = MaxArgs_ = 1;
    if (!TWinAggrEmulation::DoInit(ctx, src)) {
        return false;
    }

    YQL_ENSURE(Args_.size() == 1);
    return true;
}

TWinNTile::TWinNTile(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{
    FakeSource_ = BuildFakeSource(pos);
}

bool TWinNTile::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() >= 1 && !Args_[0]->Init(ctx, FakeSource_.Get())) {
        return false;
    }

    if (!TWinAggrEmulation::DoInit(ctx, src)) {
        return false;
    }
    return true;
}

TWinLeadLag::TWinLeadLag(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{
}

bool TWinLeadLag::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() >= 2) {
        if (!Args_[1]->IsIntegerLiteral()) {
            ctx.Error(Args_[1]->GetPos()) << "Expected integer literal as second parameter of " << OpName_ << "( ) function";
            return false;
        }
    }
    if (!TWinAggrEmulation::DoInit(ctx, src)) {
        return false;
    }
    if (Args_.size() >= 1) {
        Args_[0] = BuildLambda(Pos_, Y("row"), Args_[0]);
    }
    return true;
}

TWinRank::TWinRank(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args)
    : TWinAggrEmulation(pos, opName, minArgs, maxArgs, args)
{
}

bool TExternalFunctionConfig::DoInit(TContext& ctx, ISource* src) {
    for (auto& param : Config_) {
        auto paramName = Y(BuildQuotedAtom(Pos_, param.first));
        if (!param.second->Init(ctx, src)) {
            return false;
        }
        Nodes_.push_back(Q(L(paramName, param.second)));
    }
    return true;
}

INode::TPtr TExternalFunctionConfig::DoClone() const {
    TFunctionConfig cloned;
    for (auto& [name, node] : Config_) {
        cloned[name] = SafeClone(node);
    }

    return new TExternalFunctionConfig(GetPos(), cloned);
}

bool TWinRank::DoInit(TContext& ctx, ISource* src) {
    if (!ValidateArguments(ctx)) {
        return false;
    }

    if (!src) {
        ctx.Error(Pos_) << "Unable to use window function: " << OpName_ << " without source";
        return false;
    }

    auto winNamePtr = src->GetWindowName();
    if (!winNamePtr) {
        ctx.Error(Pos_) << "Failed to use window function: " << OpName_ << " without window";
        return false;
    }

    auto winSpecPtr = src->FindWindowSpecification(ctx, *winNamePtr);
    if (!winSpecPtr) {
        return false;
    }

    const auto& orderSpec = winSpecPtr->OrderBy;
    if (orderSpec.empty()) {
        if (Args_.empty()) {
            if (!ctx.Warning(GetPos(), TIssuesIds::YQL_RANK_WITHOUT_ORDER_BY, [&](auto& out) {
                    out << OpName_ << "() is used with unordered window - all rows will be considered equal to each other";
                })) {
                return false;
            }
        } else {
            if (!ctx.Warning(GetPos(), TIssuesIds::YQL_RANK_WITHOUT_ORDER_BY, [&](auto& out) {
                    out << OpName_ << "(<expression>) is used with unordered window - the result is likely to be undefined";
                })) {
                return false;
            }
        }
    }

    if (Args_.empty()) {
        for (const auto& spec : orderSpec) {
            // It relies on a fact that ORDER BY is
            // already validated at `TSelectCore::InitSelect`.
            if (!spec->OrderExpr->Init(ctx, src)) {
                return false;
            }

            Args_.push_back(spec->Clone()->OrderExpr);
        }

        if (Args_.size() != 1) {
            Args_ = {BuildTuple(GetPos(), Args_)};
        }
    }

    YQL_ENSURE(Args_.size() == 1);

    TVector<TNodePtr> optionsElements;
    if (!ctx.AnsiRankForNullableKeys.Defined()) {
        optionsElements.push_back(BuildTuple(Pos_, {BuildQuotedAtom(Pos_, "warnNoAnsi", NYql::TNodeFlags::Default)}));
    } else if (*ctx.AnsiRankForNullableKeys) {
        optionsElements.push_back(BuildTuple(Pos_, {BuildQuotedAtom(Pos_, "ansi", NYql::TNodeFlags::Default)}));
    }
    Args_.push_back(BuildTuple(Pos_, optionsElements));

    MinArgs_ = MaxArgs_ = 2;
    if (!TWinAggrEmulation::DoInit(ctx, src)) {
        return false;
    }

    YQL_ENSURE(Args_.size() == 2);
    Args_[0] = BuildLambda(Pos_, Y("row"), Args_[0]);
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
        : TAstListNode(other.Pos_)
    {
        Nodes_ = CloneContainer(other.Nodes_);
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
        return L(key, Q(Y(Q("view"), Y("String", BuildQuotedAtom(Pos_, view.ViewName)))));
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

bool MatchDotSuffix(const TSet<TString>& columns, const TString& column) {
    for (const auto& col : columns) {
        const auto pos = col.find_first_of(".");
        if (pos == TString::npos) {
            continue;
        }
        if (column == col.substr(pos + 1)) {
            return true;
        }
    }
    return false;
}

} // namespace

bool TColumns::IsColumnPossible(TContext& ctx, const TString& name) const {
    if (All || Real.contains(name) || Artificial.contains(name)) {
        return true;
    }

    if (ctx.SimpleColumns && !name.Contains('.') && (MatchDotSuffix(Real, name) || MatchDotSuffix(Artificial, name))) {
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
        for (const auto& real : Real) {
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
    , CleanOrderExpr_(orderExpr->Clone())
{
}

TSortSpecificationPtr TSortSpecification::Clone() const {
    return MakeIntrusive<TSortSpecification>(CleanOrderExpr_, Ascending);
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
    for (auto cur : specs) {
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
    , ColumnName_(column)
    , Source_(source)
    , MaybeType_(maybeType)
{
}

TColumnNode::TColumnNode(TPosition pos, const TNodePtr& column, const TString& source)
    : INode(pos)
    , ColumnExpr_(column)
    , Source_(source)
{
}

TColumnNode::~TColumnNode()
{
}

bool TColumnNode::IsAsterisk() const {
    return ColumnName_ == "*";
}

bool TColumnNode::IsArtificial() const {
    return Artificial_;
}

const TString* TColumnNode::GetColumnName() const {
    return UseSourceAsColumn_ ? &Source_ : (ColumnExpr_ ? nullptr : &ColumnName_);
}

const TString* TColumnNode::GetSourceName() const {
    return UseSourceAsColumn_ ? &Empty : &Source_;
}

TColumnNode* TColumnNode::GetColumnNode() {
    return this;
}

const TColumnNode* TColumnNode::GetColumnNode() const {
    return this;
}

bool TColumnNode::DoInit(TContext& ctx, ISource* src) {
    if (src) {
        YQL_ENSURE(!State_.Test(ENodeState::Initialized)); /// should be not initialized or Aggregated already invalid
        if (src->ShouldUseSourceAsColumn(*GetSourceName())) {
            if (!IsAsterisk() && IsReliable()) {
                SetUseSourceAsColumn();
            }
        }

        if (GetColumnName()) {
            auto fullName = Source_ ? DotJoin(Source_, *GetColumnName()) : *GetColumnName();
            if (!ctx.GroupByExprAfterWhere) {
                auto alias = src->GetGroupByColumnAlias(fullName);
                if (alias) {
                    ResetColumn(alias, {});
                }
            }
            Artificial_ = !Source_ && src->IsExprAlias(*GetColumnName());
        }

        if (!src->AddColumn(ctx, *this)) {
            return false;
        }
        if (GetColumnName()) {
            if (src->GetJoin() && Source_) {
                GroupKey_ = src->IsGroupByColumn(DotJoin(Source_, *GetColumnName()));
            } else {
                GroupKey_ = src->IsGroupByColumn(*GetColumnName()) || src->IsAlias(EExprSeat::GroupBy, *GetColumnName());
            }
        }
    }
    if (IsAsterisk()) {
        Node_ = AstNode("row");
    } else {
        TString callable;
        if (MaybeType_) {
            callable = Reliable_ && !UseSource_ ? "SqlPlainColumnOrType" : "SqlColumnOrType";
        } else {
            // TODO: consider replacing Member -> SqlPlainColumn
            callable = Reliable_ && !UseSource_ ? "Member" : "SqlColumn";
        }

        TNodePtr ref = ColumnExpr_
                           ? Y("EvaluateAtom", ColumnExpr_)
                           : BuildQuotedAtom(Pos_, *GetColumnName());

        if (IsYqlRef_) {
            if (!Source_.empty()) {
                Node_ = Y("YqlColumnRef", Q(Source_), ref);
            } else {
                Node_ = Y("YqlColumnRef", ref);
            }
        } else {
            Node_ = Y(callable, "row", ref);
        }

        if (UseSource_) {
            YQL_ENSURE(Source_);
            Node_ = L(Node_, BuildQuotedAtom(Pos_, Source_));
        }
    }
    return Node_->Init(ctx, src);
}

void TColumnNode::SetUseSourceAsColumn() {
    YQL_ENSURE(!State_.Test(ENodeState::Initialized)); /// should be not initialized or Aggregated already invalid
    YQL_ENSURE(!IsAsterisk());
    UseSourceAsColumn_ = true;
}

void TColumnNode::ResetAsReliable() {
    Reliable_ = true;
}

void TColumnNode::SetAsNotReliable() {
    Reliable_ = false;
}

void TColumnNode::SetAsYqlRef() {
    IsYqlRef_ = true;
}

void TColumnNode::SetUseSource() {
    UseSource_ = true;
}

bool TColumnNode::IsUseSourceAsColumn() const {
    return UseSourceAsColumn_;
}

bool TColumnNode::IsUseSource() const {
    return UseSource_;
}

bool TColumnNode::IsReliable() const {
    return Reliable_;
}

bool TColumnNode::CanBeType() const {
    return MaybeType_;
}

TNodePtr TColumnNode::DoClone() const {
    YQL_ENSURE(!Node_, "TColumnNode::Clone: Node should not be initialized");
    auto copy = ColumnExpr_ ? new TColumnNode(Pos_, ColumnExpr_, Source_) : new TColumnNode(Pos_, ColumnName_, Source_, MaybeType_);
    copy->GroupKey_ = GroupKey_;
    copy->Artificial_ = Artificial_;
    copy->Reliable_ = Reliable_;
    copy->UseSource_ = UseSource_;
    copy->UseSourceAsColumn_ = UseSourceAsColumn_;
    copy->IsYqlRef_ = IsYqlRef_;
    return copy;
}

void TColumnNode::DoUpdateState() const {
    State_.Set(ENodeState::Const, false);
    State_.Set(ENodeState::MaybeConst, MaybeType_);
    State_.Set(ENodeState::Aggregated, GroupKey_);
    State_.Set(ENodeState::AggregationKey, GroupKey_);
}

TAstNode* TColumnNode::Translate(TContext& ctx) const {
    return Node_->Translate(ctx);
}

void TColumnNode::ResetColumn(const TString& column, const TString& source) {
    YQL_ENSURE(!State_.Test(ENodeState::Initialized)); /// should be not initialized
    Reliable_ = true;
    UseSource_ = false;
    UseSourceAsColumn_ = false;
    ColumnName_ = column;
    ColumnExpr_ = nullptr;
    Source_ = source;
}

void TColumnNode::ResetColumn(const TNodePtr& column, const TString& source) {
    YQL_ENSURE(!State_.Test(ENodeState::Initialized)); /// should be not initialized
    Reliable_ = true;
    UseSource_ = false;
    UseSourceAsColumn_ = false;
    ColumnName_ = "";
    ColumnExpr_ = column;
    Source_ = source;
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

TNodePtr BuildYqlColumnRef(TPosition pos) {
    TString source = "";
    bool maybeType = true;
    auto* node = new TColumnNode(pos, /* column = */ "", source, maybeType);
    node->SetAsYqlRef();
    return node;
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
    return !DistinctKey_.empty();
}

void IAggregation::DoUpdateState() const {
    State_.Set(ENodeState::Aggregated, AggMode_ == EAggregateMode::Normal);
    State_.Set(ENodeState::OverWindow, AggMode_ == EAggregateMode::OverWindow);
    State_.Set(ENodeState::OverWindowDistinct, AggMode_ == EAggregateMode::OverWindowDistinct);
}

TMaybe<TString> IAggregation::GetGenericKey() const {
    return Nothing();
}

void IAggregation::Join(IAggregation*) {
    YQL_ENSURE(false, "Should not be called");
}

const TString& IAggregation::GetName() const {
    return Name_;
}

EAggregateMode IAggregation::GetAggregationMode() const {
    return AggMode_;
}

void IAggregation::MarkKeyColumnAsGenerated() {
    IsGeneratedKeyColumn_ = true;
}

IAggregation::IAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode)
    : INode(pos)
    , Name_(name)
    , Func_(func)
    , AggMode_(aggMode)
{
}

TAstNode* IAggregation::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(false);
    Y_UNUSED(ctx);
    return nullptr;
}

std::pair<TNodePtr, bool> IAggregation::AggregationTraits(const TNodePtr& type, bool overState, bool many, bool allowAggApply, TContext& ctx) const {
    const bool distinct = AggMode_ == EAggregateMode::Distinct;
    const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos_, DistinctKey_))) : type;
    auto apply = GetApply(listType, many, allowAggApply, ctx);
    if (!apply) {
        return {nullptr, false};
    }

    auto wrapped = WrapIfOverState(apply, overState, many, ctx);
    if (!wrapped) {
        return {nullptr, false};
    }

    return {distinct ? Q(Y(Q(Name_), wrapped, BuildQuotedAtom(Pos_, DistinctKey_))) : Q(Y(Q(Name_), wrapped)), true};
}

TNodePtr IAggregation::WrapIfOverState(const TNodePtr& input, bool overState, bool many, TContext& ctx) const {
    if (!overState) {
        return input;
    }

    auto extractor = GetExtractor(many, ctx);
    if (!extractor) {
        return nullptr;
    }

    return Y(ToString("AggOverState"), extractor, BuildLambda(Pos_, Y(), input));
}

TNodePtr IAggregation::GetExtractor(bool many, TContext& ctx) const {
    return BuildLambda(Pos_, Y("row"), GetExtractorBody(many, ctx));
}

void IAggregation::AddFactoryArguments(TNodePtr& apply) const {
    Y_UNUSED(apply);
}

std::vector<ui32> IAggregation::GetFactoryColumnIndices() const {
    return {0u};
}

TNodePtr IAggregation::WindowTraits(const TNodePtr& type, TContext& ctx) const {
    YQL_ENSURE(AggMode_ == EAggregateMode::OverWindow || AggMode_ == EAggregateMode::OverWindowDistinct, "Windows traits is unavailable");

    const bool distinct = AggMode_ == EAggregateMode::OverWindowDistinct;
    const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos_, DistinctKey_))) : type;
    auto traits = Y(Q(Name_), GetApply(listType, false, false, ctx));
    if (AggMode_ == EAggregateMode::OverWindowDistinct) {
        traits->Add(BuildQuotedAtom(Pos_, DistinctKey_));
    }

    return Q(traits);
}

namespace {
bool UnescapeQuoted(const TString& str, TPosition& pos, char quoteChar, TString& result, TString& error, bool utf8Aware) {
    result = error = {};

    size_t readBytes = 0;
    TStringBuf atom(str);
    TStringOutput sout(result);
    atom.Skip(1);
    result.reserve(str.size());

    auto unescapeResult = UnescapeArbitraryAtom(atom, quoteChar, &sout, &readBytes);
    if (unescapeResult != EUnescapeResult::OK) {
        TTextWalker walker(pos, utf8Aware);
        walker.Advance(atom.Trunc(readBytes));
        error = TStringBuilder()
                << UnescapeResultToString(unescapeResult)
                << " near byte " << readBytes;
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

enum class EStringContentMode: int {
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
            str = str.substr(0, str.size() - 1);
            result.Type = NKikimr::NUdf::EDataSlot::Yson;
        } else if (lower.EndsWith("j")) {
            str = str.substr(0, str.size() - 1);
            result.Type = NKikimr::NUdf::EDataSlot::Json;
        } else if (lower.EndsWith("p")) {
            str = str.substr(0, str.size() - 1);
            result.PgType = "PgText";
        } else if (lower.EndsWith("pt")) {
            str = str.substr(0, str.size() - 2);
            result.PgType = "PgText";
        } else if (lower.EndsWith("pb")) {
            str = str.substr(0, str.size() - 2);
            result.PgType = "PgBytea";
        } else if (lower.EndsWith("pv")) {
            str = str.substr(0, str.size() - 2);
            result.PgType = "PgVarchar";
        } else if (lower.EndsWith("s")) {
            str = str.substr(0, str.size() - 1);
            result.Type = NKikimr::NUdf::EDataSlot::String;
        } else if (lower.EndsWith("u")) {
            str = str.substr(0, str.size() - 1);
            result.Type = NKikimr::NUdf::EDataSlot::Utf8;
        } else {
            if (ctx.Scoped->WarnUntypedStringLiterals) {
                if (!ctx.Warning(pos, TIssuesIds::YQL_UNTYPED_STRING_LITERALS, [](auto& out) {
                        out << "Please add suffix u for Utf8 strings or s for arbitrary binary strings";
                    })) {
                    return {};
                }
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
            if (!UnescapeQuoted(str, pos, str[0], result.Content, error, /*utf8Aware=*/true)) {
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
                                 (ctx.AnsiQuotedIdentifiers && input.StartsWith('"')) ? EStringContentMode::AnsiIdent : EStringContentMode::Default);
}

TTtlSettings::TTierSettings::TTierSettings(const TNodePtr& evictionDelay, const std::optional<TIdentifier>& storageName)
    : EvictionDelay(evictionDelay)
    , StorageName(storageName)
{
}

TTtlSettings::TTtlSettings(const TIdentifier& columnName, const std::vector<TTierSettings>& tiers, const TMaybe<EUnit>& columnUnit)
    : ColumnName(columnName)
    , Tiers(tiers)
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
        TTextWalker walker(pos, /*utf8Aware=*/true);
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
    explicit TInvalidLiteralNode(TPosition pos)
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

} // namespace

TLiteralNode::TLiteralNode(TPosition pos, bool isNull)
    : TAstListNode(pos)
    , Null_(isNull)
    , Void_(!isNull)
{
    Add(isNull ? "Null" : "Void");
}

TLiteralNode::TLiteralNode(TPosition pos, const TString& type, const TString& value)
    : TAstListNode(pos)
    , Null_(false)
    , Void_(false)
    , Type_(type)
    , Value_(value)
{
    if (Type_.StartsWith("Pg")) {
        Add("PgConst", BuildQuotedAtom(Pos_, Value_), Y("PgType", Q(to_lower(Type_.substr(2)))));
    } else {
        Add(Type_, BuildQuotedAtom(Pos_, Value_));
    }
}

TLiteralNode::TLiteralNode(TPosition pos, const TString& value, ui32 nodeFlags)
    : TAstListNode(pos)
    , Null_(false)
    , Void_(false)
    , Type_("String")
    , Value_(value)
{
    Add(Type_, BuildQuotedAtom(pos, Value_, nodeFlags));
}

TLiteralNode::TLiteralNode(TPosition pos, const TString& value, ui32 nodeFlags, const TString& type)
    : TAstListNode(pos)
    , Null_(false)
    , Void_(false)
    , Type_(type)
    , Value_(value)
{
    if (Type_.StartsWith("Pg")) {
        Add("PgConst", BuildQuotedAtom(Pos_, Value_, nodeFlags), Y("PgType", Q(to_lower(Type_.substr(2)))));
    } else {
        Add(Type_, BuildQuotedAtom(pos, Value_, nodeFlags));
    }
}

bool TLiteralNode::IsNull() const {
    return Null_;
}

const TString* TLiteralNode::GetLiteral(const TString& type) const {
    return type == Type_ ? &Value_ : nullptr;
}

bool TLiteralNode::IsLiteral() const {
    return true;
}

TString TLiteralNode::GetLiteralType() const {
    return Type_;
}

TString TLiteralNode::GetLiteralValue() const {
    return Value_;
}

void TLiteralNode::DoUpdateState() const {
    State_.Set(ENodeState::Const);
}

TNodePtr TLiteralNode::DoClone() const {
    auto res = (Null_ || Void_) ? MakeIntrusive<TLiteralNode>(Pos_, Null_) : MakeIntrusive<TLiteralNode>(Pos_, Type_, Value_);
    res->Nodes_ = Nodes_;
    return res;
}

template <typename T>
TLiteralNumberNode<T>::TLiteralNumberNode(TPosition pos, const TString& type, const TString& value, bool implicitType)
    : TLiteralNode(pos, type, value)
    , ImplicitType_(implicitType)
{
}

template <typename T>
TNodePtr TLiteralNumberNode<T>::DoClone() const {
    return new TLiteralNumberNode<T>(Pos_, Type_, Value_, ImplicitType_);
}

template <typename T>
bool TLiteralNumberNode<T>::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(src);
    T val;
    if (!TryFromString(Value_, val)) {
        ctx.Error(Pos_) << "Failed to parse " << Value_ << " as integer literal of " << Type_ << " type: value out of range for " << Type_;
        return false;
    }
    return true;
}

template <typename T>
bool TLiteralNumberNode<T>::IsIntegerLiteral() const {
    return std::numeric_limits<T>::is_integer;
}

template <typename T>
TNodePtr TLiteralNumberNode<T>::ApplyUnaryOp(TContext& ctx, TPosition pos, const TString& opName) const {
    YQL_ENSURE(!Value_.empty());
    if (opName == "Minus" && IsIntegerLiteral() && Value_[0] != '-') {
        if (ImplicitType_) {
            ui64 val = FromString<ui64>(Value_);
            TString negated = "-" + Value_;
            if (val <= ui64(std::numeric_limits<i32>::max()) + 1) {
                // negated value fits in Int32
                i32 v;
                YQL_ENSURE(TryFromString(negated, v));
                return new TLiteralNumberNode<i32>(pos, Type_.StartsWith("Pg") ? "PgInt4" : "Int32", negated);
            }
            if (val <= ui64(std::numeric_limits<i64>::max()) + 1) {
                // negated value fits in Int64
                i64 v;
                YQL_ENSURE(TryFromString(negated, v));
                return new TLiteralNumberNode<i64>(pos, Type_.StartsWith("Pg") ? "PgInt8" : "Int64", negated);
            }

            ctx.Error(pos) << "Failed to parse negative integer: " << negated << ", number limit overflow";
            return {};
        }

        if (std::numeric_limits<T>::is_signed) {
            return new TLiteralNumberNode<T>(pos, Type_, "-" + Value_);
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
{
}

bool TAsteriskNode::IsAsterisk() const {
    return true;
};

TNodePtr TAsteriskNode::DoClone() const {
    return new TAsteriskNode(Pos_);
}

TAstNode* TAsteriskNode::Translate(TContext& ctx) const {
    ctx.Error(Pos_) << "* is not allowed here";
    return nullptr;
}

TNodePtr BuildEmptyAction(TPosition pos) {
    TNodePtr params = new TAstListNodeImpl(pos);
    TNodePtr arg = new TAstAtomNodeImpl(pos, "x", TNodeFlags::Default);
    params->Add(arg);
    return BuildLambda(pos, params, arg);
}

TDeferredAtom::TDeferredAtom()
{
}

TDeferredAtom::TDeferredAtom(TPosition pos, const TString& str)
{
    Node_ = BuildQuotedAtom(pos, str);
    Explicit_ = str;
    Repr_ = str;
}

TDeferredAtom::TDeferredAtom(TNodePtr node, TContext& ctx)
{
    Node_ = node;
    Repr_ = ctx.MakeName("DeferredAtom");
}

const TString* TDeferredAtom::GetLiteral() const {
    return Explicit_.Get();
}

bool TDeferredAtom::GetLiteral(TString& value, TContext& ctx) const {
    if (Explicit_) {
        value = *Explicit_;
        return true;
    }

    ctx.Error(Node_ ? Node_->GetPos() : ctx.Pos()) << "Expected literal value";
    return false;
}

TNodePtr TDeferredAtom::Build() const {
    return Node_;
}

TString TDeferredAtom::GetRepr() const {
    return Repr_;
}

bool TDeferredAtom::Empty() const {
    return !Node_ || Repr_.empty();
}

bool TDeferredAtom::HasNode() const {
    return !!Node_;
}

TTupleNode::TTupleNode(TPosition pos, const TVector<TNodePtr>& exprs)
    : TAstListNode(pos)
    , Exprs_(exprs)
{
}

bool TTupleNode::IsEmpty() const {
    return Exprs_.empty();
}

const TVector<TNodePtr>& TTupleNode::Elements() const {
    return Exprs_;
}

TTupleNode* TTupleNode::GetTupleNode() {
    return this;
}

const TTupleNode* TTupleNode::GetTupleNode() const {
    return this;
}

bool TTupleNode::DoInit(TContext& ctx, ISource* src) {
    auto node(Y());
    for (auto& expr : Exprs_) {
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
    return Exprs_.size();
}

TNodePtr TTupleNode::GetTupleElement(size_t index) const {
    return Exprs_[index];
}

TNodePtr TTupleNode::DoClone() const {
    return new TTupleNode(Pos_, CloneContainer(Exprs_));
}

void TTupleNode::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    for (auto& expr : Exprs_) {
        expr->CollectPreaggregateExprs(ctx, src, exprs);
    }
}

const TString* TTupleNode::GetSourceName() const {
    return DeriveCommonSourceName(Exprs_);
}

TNodePtr BuildTuple(TPosition pos, const TVector<TNodePtr>& exprs) {
    return new TTupleNode(pos, exprs);
}

TStructNode::TStructNode(TPosition pos, const TVector<TNodePtr>& exprs, const TVector<TNodePtr>& labels, bool ordered)
    : TAstListNode(pos)
    , Exprs_(exprs)
    , Labels_(labels)
    , Ordered_(ordered)
{
    YQL_ENSURE(Labels_.empty() || Labels_.size() == Exprs_.size());
}

bool TStructNode::DoInit(TContext& ctx, ISource* src) {
    Nodes_.push_back(BuildAtom(Pos_, (Ordered_ || Exprs_.size() < 2) ? "AsStruct" : "AsStructUnordered", TNodeFlags::Default));
    size_t i = 0;
    for (const auto& expr : Exprs_) {
        TNodePtr label;
        if (Labels_.empty()) {
            if (!expr->GetLabel()) {
                ctx.Error(expr->GetPos()) << "Structure does not allow anonymous members";
                return false;
            }
            label = BuildQuotedAtom(expr->GetPos(), expr->GetLabel());
        } else {
            label = Labels_[i++];
        }
        Nodes_.push_back(Q(Y(label, expr)));
    }
    return TAstListNode::DoInit(ctx, src);
}

TNodePtr TStructNode::DoClone() const {
    return new TStructNode(Pos_, CloneContainer(Exprs_), CloneContainer(Labels_), Ordered_);
}

TStructNode* TStructNode::GetStructNode() {
    return this;
}

const TStructNode* TStructNode::GetStructNode() const {
    return this;
}

void TStructNode::CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) {
    for (auto& expr : Exprs_) {
        expr->CollectPreaggregateExprs(ctx, src, exprs);
    }
}

const TString* TStructNode::GetSourceName() const {
    return DeriveCommonSourceName(Exprs_);
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
    , Exprs_(std::move(exprs))
{
}

TVector<TNodePtr>* TListOfNamedNodes::ContentListPtr() {
    return &Exprs_;
}

TAstNode* TListOfNamedNodes::Translate(TContext& ctx) const {
    YQL_ENSURE(!"Unexpected usage");
    Y_UNUSED(ctx);
    return nullptr;
}

TNodePtr TListOfNamedNodes::DoClone() const {
    return new TListOfNamedNodes(GetPos(), CloneContainer(Exprs_));
}

void TListOfNamedNodes::DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const {
    for (auto& expr : Exprs_) {
        expr->VisitTree(func, visited);
    }
}

TNodePtr BuildListOfNamedNodes(TPosition pos, TVector<TNodePtr>&& exprs) {
    return new TListOfNamedNodes(pos, std::move(exprs));
}

TArgPlaceholderNode::TArgPlaceholderNode(TPosition pos, const TString& name)
    : INode(pos)
    ,
    Name_(name)
{
}

bool TArgPlaceholderNode::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(src);
    ctx.Error(Pos_) << Name_ << " can't be used as a part of expression.";
    return false;
}

TAstNode* TArgPlaceholderNode::Translate(TContext& ctx) const {
    Y_UNUSED(ctx);
    return nullptr;
}

TString TArgPlaceholderNode::GetName() const {
    return Name_;
}

TNodePtr TArgPlaceholderNode::DoClone() const {
    return new TArgPlaceholderNode(GetPos(), Name_);
}

TNodePtr BuildArgPlaceholder(TPosition pos, const TString& name) {
    return new TArgPlaceholderNode(pos, name);
}

class TAccessNode: public INode {
public:
    TAccessNode(TPosition pos, const TVector<TIdPart>& ids, bool isLookup)
        : INode(pos)
        , Ids_(ids)
        , IsLookup_(isLookup)
        , ColumnOnly_(false)
        , IsColumnRequired_(false)
        , AccessOpName_("AccessNode")
    {
        Y_DEBUG_ABORT_UNLESS(Ids_.size() > 1);
        Y_DEBUG_ABORT_UNLESS(Ids_[0].Expr);
        auto column = Ids_[0].Expr->GetColumnNode();
        if (column) {
            ui32 idx = 1;
            TString source;
            if (Ids_.size() > 2) {
                source = Ids_[idx].Name;
                ++idx;
            }

            ColumnOnly_ = !IsLookup_ && Ids_.size() < 4;
            if (ColumnOnly_ && Ids_[idx].Expr) {
                column->ResetColumn(Ids_[idx].Expr, source);
            } else {
                column->ResetColumn(Ids_[idx].Name, source);
            }
        }
    }

    void AssumeColumn() override {
        IsColumnRequired_ = true;
    }

    TMaybe<TString> TryMakeTable() {
        if (!ColumnOnly_) {
            return Nothing();
        }

        ui32 idx = 1;
        if (Ids_.size() > 2) {
            return Nothing();
        }

        return Ids_[idx].Name;
    }

    const TString* GetColumnName() const override {
        return ColumnOnly_ ? Ids_[0].Expr->GetColumnName() : nullptr;
    }

    bool IsPlainColumn() const override {
        if (GetColumnName()) {
            return true;
        }

        if (Ids_[0].Expr->IsTableRow()) {
            return true;
        }

        return false;
    }

    const TString* GetSourceName() const override {
        return Ids_[0].Expr->GetSourceName();
    }

    TAccessNode* GetAccessNode() override {
        return this;
    }

    const TAccessNode* GetAccessNode() const override {
        return this;
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto expr = Ids_[0].Expr;
        const TPosition pos(expr->GetPos());
        if (expr->IsAsterisk()) {
            ctx.Error(pos) << "Asterisk column does not allow any access";
            return false;
        }
        if (!expr->Init(ctx, src)) {
            return false;
        }
        for (auto& id : Ids_) {
            if (id.Expr && !id.Expr->Init(ctx, src)) {
                return false;
            }
        }
        ui32 idx = 1;
        auto column = expr->GetColumnNode();
        if (column) {
            const bool useSourceAsColumn = column->IsUseSourceAsColumn();
            ColumnOnly_ &= !useSourceAsColumn;
            if (IsColumnRequired_ && !ColumnOnly_) {
                ctx.Error(pos) << "Please use a full form (corellation.struct.field) or an alias (struct.field as alias) to access struct's field in the GROUP BY";
                return false;
            }

            if (Ids_.size() > 2) {
                if (!CheckColumnId(pos, ctx, Ids_[idx], ColumnOnly_ ? "Correlation" : "Column", true)) {
                    return false;
                }
                ++idx;
            }
            if (!useSourceAsColumn) {
                if (!IsLookup_ && !CheckColumnId(pos, ctx, Ids_[idx], ColumnOnly_ ? "Column" : "Member", false)) {
                    return false;
                }
                ++idx;
            }
        }
        for (; idx < Ids_.size(); ++idx) {
            const auto& id = Ids_[idx];
            if (!id.Name.empty()) {
                expr = Y("SqlAccess", Q("struct"), expr, id.Expr ? Y("EvaluateAtom", id.Expr) : BuildQuotedAtom(Pos_, id.Name));
                AccessOpName_ = "AccessStructMember";
            } else if (id.Expr) {
                expr = Y("SqlAccess", Q("dict"), expr, id.Expr);
                AccessOpName_ = "AccessDictMember";
            } else {
                continue;
            }

            if (ctx.PragmaYsonAutoConvert || ctx.PragmaYsonStrict || ctx.PragmaYsonFast) {
                auto ysonOptions = Y();
                if (ctx.PragmaYsonAutoConvert) {
                    ysonOptions->Add(BuildQuotedAtom(Pos_, "yson_auto_convert"));
                }
                if (ctx.PragmaYsonStrict) {
                    ysonOptions->Add(BuildQuotedAtom(Pos_, "yson_strict"));
                }
                if (ctx.PragmaYsonFast) {
                    ysonOptions->Add(BuildQuotedAtom(Pos_, "yson_fast"));
                }
                expr->Add(Q(ysonOptions));
            }
        }
        Node_ = expr;
        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node_, "Oh, no Node! Maybe you forgot to call Init");
        return Node_->Translate(ctx);
    }

    TPtr DoClone() const override {
        YQL_ENSURE(!Node_, "TAccessNode::Clone: Node should not be initialized");
        TVector<TIdPart> cloneIds;
        cloneIds.reserve(Ids_.size());
        for (const auto& id : Ids_) {
            cloneIds.emplace_back(id.Clone());
        }
        auto copy = new TAccessNode(Pos_, cloneIds, IsLookup_);
        copy->ColumnOnly_ = ColumnOnly_;
        return copy;
    }

    const TVector<TIdPart>& GetParts() const {
        return Ids_;
    }

protected:
    void DoUpdateState() const override {
        YQL_ENSURE(Node_);
        State_.Set(ENodeState::Const, Node_->IsConstant());
        State_.Set(ENodeState::MaybeConst, Node_->MaybeConstant());
        State_.Set(ENodeState::Aggregated, Node_->IsAggregated());
        State_.Set(ENodeState::AggregationKey, Node_->HasState(ENodeState::AggregationKey));
        State_.Set(ENodeState::OverWindow, Node_->IsOverWindow());
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node_);
        Node_->VisitTree(func, visited);
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
        return AccessOpName_;
    }

    void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override {
        for (auto& id : Ids_) {
            if (id.Expr) {
                id.Expr->CollectPreaggregateExprs(ctx, src, exprs);
            }
        }
    }

private:
    TNodePtr Node_;
    TVector<TIdPart> Ids_;
    bool IsLookup_;
    bool ColumnOnly_;
    bool IsColumnRequired_;
    TString AccessOpName_;
};

TNodePtr BuildAccess(TPosition pos, const TVector<INode::TIdPart>& ids, bool isLookup) {
    return new TAccessNode(pos, ids, isLookup);
}

bool WarnIfAliasFromSelectIsUsedInGroupBy(TContext& ctx, const TVector<TNodePtr>& selectTerms, const TVector<TNodePtr>& groupByTerms,
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
        return true;
    }

    bool isError = false;
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

                ctx.Warning(current.GetPos(), TIssuesIds::YQL_PROJECTION_ALIAS_IS_REFERENCED_IN_GROUP_BY, [&](auto& out) {
                    out << "GROUP BY will aggregate by column `" << *columnName
                        << "` instead of aggregating by SELECT expression with same alias";
                });

                isError = isError || !ctx.Warning(it->second->GetPos(), TIssuesIds::YQL_PROJECTION_ALIAS_IS_REFERENCED_IN_GROUP_BY, [](auto& out) {
                    out << "You should probably use alias in GROUP BY instead of using it here. "
                        << "Please consult documentation for more details";
                });

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
        if (isError) {
            return false;
        }
        if (found) {
            return true;
        }
    }
    return true;
}

bool ValidateAllNodesForAggregation(TContext& ctx, const TVector<TNodePtr>& nodes) {
    for (auto& node : nodes) {
        if (!node->HasState(ENodeState::Initialized) || node->IsConstant() || node->MaybeConstant()) {
            continue;
        }
        // TODO: "!node->IsOverWindow()" doesn't look right here
        if (!node->IsAggregated() && !node->IsOverWindow() && !node->IsOverWindowDistinct()) {
            // locate column which is not a key column and not aggregated
            const INode* found = nullptr;
            auto visitor = [&found](const INode& current) {
                if (found || current.IsAggregated() || current.IsOverWindow() || current.IsOverWindowDistinct()) {
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
        Nodes_ = CloneContainer(other.Nodes_);
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
        Nodes_ = CloneContainer(other.Nodes_);
    }

    TPtr DoClone() const final {
        return new TLambdaNode(*this);
    }

    void DoUpdateState() const final {
        State_.Set(ENodeState::Const);
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
        ctx.Error(pos) << "Unknown " << (explicitPgType ? "pg" : "simple") << " type '" << typeName << "'";
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
        return new TCallNodeImpl(pos, "PgType", {BuildQuotedAtom(pos, pgType, TNodeFlags::Default)});
    }

    return new TCallNodeImpl(pos, "DataType", {BuildQuotedAtom(pos, type, TNodeFlags::Default)});
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
    , Args_(args)
{
    if (Args_.size()) {
        // If there aren't any named args, args are passed as vector of positional args,
        // else Args has length 2: tuple for positional args and struct for named args,
        // so let's construct tuple of args there. Other type checks will within DoInit call.
        if (!Args_[0]->GetTupleNode()) {
            Args_ = {BuildTuple(pos, args)};
        }
    }
}

bool TUdfNode::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(src);
    if (Args_.size() < 1) {
        ctx.Error(Pos_) << "Udf: expected at least one argument";
        return false;
    }

    TTupleNode* as_tuple = Args_[0]->GetTupleNode();

    if (!as_tuple || as_tuple->GetTupleSize() < 1) {
        ctx.Error(Pos_) << "Udf: first argument must be a callable, like Foo::Bar";
        return false;
    }

    TNodePtr function = as_tuple->GetTupleElement(0);

    if (!function || !function->FuncName()) {
        ctx.Error(Pos_) << "Udf: first argument must be a callable, like Foo::Bar";
        return false;
    }

    FunctionName_ = function->FuncName();
    ModuleName_ = function->ModuleName();
    ScriptUdf_ = function->IsScript();
    if (ScriptUdf_ && as_tuple->GetTupleSize() > 1) {
        ctx.Error(Pos_) << "Udf: user type is not supported for script udfs";
        return false;
    }

    if (ScriptUdf_) {
        for (size_t i = 0; i < function->GetTupleSize(); ++i) {
            ScriptArgs_.push_back(function->GetTupleElement(i));
        }
    }

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

    ExternalTypesTuple_ = new TCallNodeImpl(Pos_, "TupleType", external);

    if (Args_.size() == 1) {
        return true;
    }

    if (TStructNode* named_args = Args_[1]->GetStructNode(); named_args) {
        for (const auto& arg : named_args->GetExprs()) {
            if (arg->GetLabel() == "TypeConfig") {
                if (function->IsScript()) {
                    ctx.Error() << "Udf: TypeConfig is not supported for script udfs";
                    return false;
                }

                TypeConfig_ = MakeAtomFromExpression(Pos_, ctx, arg);
            } else if (arg->GetLabel() == "RunConfig") {
                if (function->IsScript()) {
                    ctx.Error() << "Udf: RunConfig is not supported for script udfs";
                    return false;
                }

                RunConfig_ = arg;
            } else if (arg->GetLabel() == "Cpu") {
                Cpu_ = MakeAtomFromExpression(Pos_, ctx, arg);
            } else if (arg->GetLabel() == "ExtraMem") {
                ExtraMem_ = MakeAtomFromExpression(Pos_, ctx, arg);
            } else if (arg->GetLabel() == "Depends") {
                if (!ctx.EnsureBackwardCompatibleFeatureAvailable(
                        Pos_,
                        "Udf: named argument Depends",
                        NYql::MakeLangVersion(2025, 3)))
                {
                    return false;
                }

                Depends_.push_back(arg);
            } else if (arg->GetLabel() == "Layers") {
                if (!ctx.EnsureBackwardCompatibleFeatureAvailable(
                        Pos_,
                        "Udf: named argument Layers",
                        NYql::MakeLangVersion(2025, 4)))
                {
                    return false;
                }

                Layers_ = arg;
            } else {
                ctx.Error() << "Udf: unexpected named argument: " << arg->GetLabel();
                return false;
            }
        }
    }

    return true;
}

const TNodePtr TUdfNode::GetExternalTypes() const {
    return ExternalTypesTuple_;
}

const TString& TUdfNode::GetFunction() const {
    return *FunctionName_;
}

const TString& TUdfNode::GetModule() const {
    return *ModuleName_;
}

TNodePtr TUdfNode::GetRunConfig() const {
    return RunConfig_;
}

const TDeferredAtom& TUdfNode::GetTypeConfig() const {
    return TypeConfig_;
}

const TVector<TNodePtr>& TUdfNode::GetDepends() const {
    return Depends_;
}

TNodePtr TUdfNode::BuildOptions() const {
    if (Cpu_.Empty() && ExtraMem_.Empty() && !Layers_) {
        return nullptr;
    }

    auto options = Y();
    if (!Cpu_.Empty()) {
        options = L(options, Q(Y(Q("cpu"), Cpu_.Build())));
    }

    if (!ExtraMem_.Empty()) {
        options = L(options, Q(Y(Q("extraMem"), ExtraMem_.Build())));
    }

    if (Layers_) {
        // layer path can be taken from table
        options = L(options, Q(Y(Q("layers"), Y("EvaluateExpr", Layers_))));
    }

    return Q(options);
}

bool TUdfNode::IsScript() const {
    return ScriptUdf_;
}

const TVector<TNodePtr>& TUdfNode::GetScriptArgs() const {
    return ScriptArgs_;
}

TUdfNode* TUdfNode::GetUdfNode() {
    return this;
}

const TUdfNode* TUdfNode::GetUdfNode() const {
    return this;
}

TAstNode* TUdfNode::Translate(TContext& ctx) const {
    ctx.Error(Pos_)
        << "Abstract Udf Node can't be used as a part of expression. "
        << "It should be applied immediately to its arguments";
    return nullptr;
}

TNodePtr TUdfNode::DoClone() const {
    return new TUdfNode(Pos_, CloneContainer(Args_));
}

class TBinaryOpNode final: public TCallNode {
public:
    TBinaryOpNode(TPosition pos, const TString& opName, TNodePtr a, TNodePtr b);

    TNodePtr DoClone() const final {
        YQL_ENSURE(Args_.size() == 2);
        return new TBinaryOpNode(Pos_, OpName_, Args_[0]->Clone(), Args_[1]->Clone());
    }
};

TBinaryOpNode::TBinaryOpNode(TPosition pos, const TString& opName, TNodePtr a, TNodePtr b)
    : TCallNode(pos, opName, 2, 2, {a, b})
{
}

TNodePtr BuildBinaryOp(TContext& ctx, TPosition pos, const TString& opName, TNodePtr a, TNodePtr b) {
    if (!a || !b) {
        return nullptr;
    }

    static const THashSet<TStringBuf> NullSafeOps = {
        "IsDistinctFrom", "IsNotDistinctFrom",
        "EqualsIgnoreCase", "StartsWithIgnoreCase", "EndsWithIgnoreCase", "StringContainsIgnoreCase"};
    if (!NullSafeOps.contains(opName)) {
        const bool bothArgNull = a->IsNull() && b->IsNull();
        const bool oneArgNull = a->IsNull() || b->IsNull();

        if (bothArgNull || (oneArgNull && opName != "Or" && opName != "And")) {
            if (!ctx.Warning(pos, TIssuesIds::YQL_OPERATION_WILL_RETURN_NULL, [&](auto& out) {
                    out << "Binary operation "
                        << opName.substr(0, opName.size() - 7 * opName.EndsWith("MayWarn"))
                        << " will return NULL here";
                })) {
                return nullptr;
            }
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
        , WindowName_(windowName)
        , FuncNode_(node)
    {
    }

    TAstNode* Translate(TContext& ctx) const override {
        return FuncNode_->Translate(ctx);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        YQL_ENSURE(src);
        TSourcePtr overWindowSource = BuildOverWindowSource(ctx.Pos(), WindowName_, src);
        if (!FuncNode_->Init(ctx, overWindowSource.Get())) {
            return false;
        }
        return true;
    }

    TPtr DoClone() const final {
        return new TCalcOverWindow(Pos_, WindowName_, SafeClone(FuncNode_));
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const, FuncNode_->IsConstant());
        State_.Set(ENodeState::MaybeConst, FuncNode_->MaybeConstant());
        State_.Set(ENodeState::Aggregated, FuncNode_->IsAggregated());
        State_.Set(ENodeState::OverWindow, true);
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(FuncNode_);
        FuncNode_->VisitTree(func, visited);
    }

    void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override {
        if (ctx.DistinctOverWindow) {
            FuncNode_->CollectPreaggregateExprs(ctx, src, exprs);
        } else {
            INode::CollectPreaggregateExprs(ctx, src, exprs);
        }
    }

protected:
    const TString WindowName_;
    TNodePtr FuncNode_;
};

TNodePtr BuildCalcOverWindow(TPosition pos, const TString& windowName, TNodePtr call) {
    return new TCalcOverWindow(pos, windowName, call);
}

template <bool Fast>
class TYsonOptionsNode final: public INode {
public:
    TYsonOptionsNode(TPosition pos, bool autoConvert, bool strict)
        : INode(pos)
        , AutoConvert_(autoConvert)
        , Strict_(strict)
    {
        auto udf = Y("Udf", Q(Fast ? "Yson2.Options" : "Yson.Options"));
        auto autoConvertNode = BuildLiteralBool(pos, autoConvert);
        autoConvertNode->SetLabel("AutoConvert");
        auto strictNode = BuildLiteralBool(pos, strict);
        strictNode->SetLabel("Strict");
        Node_ = Y("NamedApply", udf, Q(Y()), BuildStructure(pos, {autoConvertNode, strictNode}));
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Node_->Init(ctx, src)) {
            return false;
        }
        return true;
    }

    TPtr DoClone() const final {
        return new TYsonOptionsNode(Pos_, AutoConvert_, Strict_);
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const, true);
    }

protected:
    TNodePtr Node_;
    const bool AutoConvert_;
    const bool Strict_;
};

TNodePtr BuildYsonOptionsNode(TPosition pos, bool autoConvert, bool strict, bool fastYson) {
    if (fastYson) {
        return new TYsonOptionsNode<true>(pos, autoConvert, strict);
    } else {
        return new TYsonOptionsNode<false>(pos, autoConvert, strict);
    }
}

class TDoCall final: public INode {
public:
    TDoCall(TPosition pos, const TNodePtr& node)
        : INode(pos)
        , Node_(node)
    {
        FakeSource_ = BuildFakeSource(pos);
    }

    ISource* GetSource() final {
        return FakeSource_.Get();
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        Y_UNUSED(src);
        if (!Node_->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const final {
        return Node_->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TDoCall(Pos_, Node_->Clone());
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node_);
        Node_->VisitTree(func, visited);
    }

private:
    TNodePtr Node_;
    TSourcePtr FakeSource_;
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

    auto wrappedNode = new TAstListNodeImpl(pos, {new TAstAtomNodeImpl(pos, "EvaluateAtom", TNodeFlags::Default),
                                                  node});

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

    if (!prefix.empty()) {
        node = node->Y("Concat", node->Y("String", node->Q(prefix)), node);
    }

    auto wrappedNode = new TAstListNodeImpl(pos, {new TAstAtomNodeImpl(pos, "EvaluateAtom", TNodeFlags::Default),
                                                  node});

    table = TDeferredAtom(wrappedNode, ctx);
}

TDeferredAtom MakeAtomFromExpression(TPosition pos, TContext& ctx, TNodePtr node, const TString& prefix) {
    if (auto literal = node->GetLiteral("String")) {
        return TDeferredAtom(node->GetPos(), prefix + *literal);
    }

    if (!prefix.empty()) {
        node = node->Y("Concat", node->Y("String", node->Q(prefix)), node);
    }

    auto wrappedNode = new TAstListNodeImpl(pos, {new TAstAtomNodeImpl(pos, "EvaluateAtom", TNodeFlags::Default),
                                                  node});

    return TDeferredAtom(wrappedNode, ctx);
}

class TTupleResultNode: public INode {
public:
    TTupleResultNode(TNodePtr&& tuple, size_t ensureTupleSize)
        : INode(tuple->GetPos())
        , Node_(std::move(tuple))
        , EnsureTupleSize_(ensureTupleSize)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Node_->Init(ctx, src)) {
            return false;
        }

        Node_ = Y("EnsureTupleSize", Node_, Q(ToString(EnsureTupleSize_)));

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TTupleResultNode(Node_->Clone(), EnsureTupleSize_);
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node_);
        Node_->VisitTree(func, visited);
    }

protected:
    TNodePtr Node_;
    const size_t EnsureTupleSize_;
};

TNodePtr BuildTupleResult(TNodePtr tuple, size_t ensureTupleSize) {
    return new TTupleResultNode(std::move(tuple), ensureTupleSize);
}

class TNamedExprReferenceNode: public IProxyNode {
public:
    TNamedExprReferenceNode(TNodePtr parent, const TString& name, TMaybe<size_t> tupleIndex)
        : IProxyNode(parent->GetPos(), parent)
        , Name_(name)
        , TupleIndex_(tupleIndex)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        Y_UNUSED(src);
        if (!IProxyNode::DoInit(ctx, nullptr) || !IProxyNode::InitReference(ctx)) {
            return false;
        }

        Node_ = BuildAtom(GetPos(), Name_, TNodeFlags::Default);
        if (TupleIndex_.Defined()) {
            Node_ = Y("Nth", Node_, Q(ToString(*TupleIndex_)));
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        YQL_ENSURE(Node_, "Init() should be done before Translate()");
        return Node_->Translate(ctx);
    }

    TPtr DoClone() const final {
        // do not clone Inner here
        return new TNamedExprReferenceNode(Inner_, Name_, TupleIndex_);
    }

private:
    const TString Name_;
    const TMaybe<size_t> TupleIndex_;
    TNodePtr Node_;
};

TNodePtr BuildNamedExprReference(TNodePtr parent, const TString& name, TMaybe<size_t> tupleIndex) {
    YQL_ENSURE(parent);
    return new TNamedExprReferenceNode(parent, name, tupleIndex);
}

class TNamedExprNode: public IProxyNode {
public:
    explicit TNamedExprNode(TNodePtr parent)
        : IProxyNode(parent->GetPos(), parent)
        , FakeSource_(BuildFakeSource(parent->GetPos()))
        , Referenced_(false)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        YQL_ENSURE(!Referenced_, "Refrence is initialized before named expr itself");
        Y_UNUSED(src);
        if (ctx.ValidateUnusedExprs) {
            return IProxyNode::DoInit(ctx, FakeSource_.Get());
        }
        // do actual init in InitReference()
        return true;
    }

    bool InitReference(TContext& ctx) final {
        Referenced_ = true;
        return IProxyNode::DoInit(ctx, FakeSource_.Get());
    }

    TAstNode* Translate(TContext& ctx) const override {
        if (ctx.ValidateUnusedExprs || Referenced_) {
            return Inner_->Translate(ctx);
        }
        auto unused = BuildQuotedAtom(GetPos(), "unused", TNodeFlags::Default);
        return unused->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TNamedExprNode(Inner_->Clone());
    }

private:
    const TSourcePtr FakeSource_;
    bool Referenced_;
};

TNodePtr BuildNamedExpr(TNodePtr parent) {
    YQL_ENSURE(parent);
    return new TNamedExprNode(parent);
}

bool TSecretParameters::ValidateParameters(TContext& ctx, const TPosition stmBeginPos, const TSecretParameters::EOperationMode mode) {
    if (!Value) {
        ctx.Error(stmBeginPos) << "parameter VALUE must be set";
        return false;
    }
    if (mode == EOperationMode::Alter) {
        if (InheritPermissions) {
            ctx.Error(stmBeginPos) << "parameter INHERIT_PERMISSIONS is not supported for alter operation";
            return false;
        }
    }

    return true;
}

} // namespace NSQLTranslationV1
