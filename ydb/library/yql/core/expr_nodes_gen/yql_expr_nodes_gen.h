#pragma once

#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/vector.h>
#include <util/generic/strbuf.h>
#include <util/system/yassert.h>
#include <util/string/cast.h>

#include <functional>
#include <iterator>

namespace NYql {
namespace NNodes {

template<typename TNode>
class TMaybeNode {};

class TExprBase {
public:
    explicit TExprBase(const TExprNode* node)
        : Raw_(node)
    {
        YQL_ENSURE(node);
    }

    explicit TExprBase(TExprNode::TPtr&& node)
        : Raw_(node.Get()), Node(std::move(node))
    {
        YQL_ENSURE(Raw_);
    }

    explicit TExprBase(const TExprNode::TPtr& node)
        : Raw_(node.Get()), Node(node)
    {
        YQL_ENSURE(node);
    }

    const TExprNode* Raw() const {
        return Raw_;
    }

    TExprNode* MutableRaw() const {
        YQL_ENSURE(Node);
        return Node.Get();
    }

    TExprNode::TPtr Ptr() const {
        YQL_ENSURE(Node);
        return Node;
    }

    const TExprNode& Ref() const {
        return *Raw_;
    }

    TExprNode& MutableRef() const {
        YQL_ENSURE(Node);
        return *Node;
    }

    TExprBase NonOwning() const {
        return TExprBase(Raw_);
    }

    TPositionHandle Pos() const {
        return Raw_->Pos();
    }

    template<typename TNode>
    TMaybeNode<TNode> Maybe() const {
        return Cast<TMaybeNode<TNode>>();
    }

    template<typename TNode>
    TNode Cast() const {
        return Node ? TNode(Node) : TNode(Raw_);
    }

private:
    const TExprNode* Raw_;
    TExprNode::TPtr Node;
};

template<>
class TMaybeNode<TExprBase> {
public:
    TMaybeNode(const TExprNode* node = nullptr)
        : Raw_(node) {}

    TMaybeNode(const TExprNode::TPtr& node)
        : Raw_(node.Get()), Node(node) {}

    TMaybeNode(const TExprBase& node)
        : Raw_(node.Raw()), Node(node.Ptr()) {}

    const TExprNode& Ref() const {
        YQL_ENSURE(IsValid());
        return *Raw_;
    }

    TExprNode& MutableRef() const {
        YQL_ENSURE(IsValid());
        YQL_ENSURE(Node);
        return *Node;
    }

    const TExprNode* Raw() const {
        return Raw_;
    }

    TExprNode* MutableRaw() const {
        YQL_ENSURE(IsValid());
        YQL_ENSURE(Node);
        return Node.Get();
    }

    bool IsValid() const {
        return Raw_ != nullptr;
    }

    explicit operator bool() const {
        return IsValid();
    }

    template<typename TNode>
    TMaybeNode<TNode> Maybe() const {
        return Node ? TMaybeNode<TNode>(Node) : TMaybeNode<TNode>(Raw_);
    }

    template<typename TNode>
    TNode Cast() const {
        YQL_ENSURE(IsValid());
        return Node ? TNode(Node) : TNode(Raw_);
    }

    TExprBase Cast() const {
        YQL_ENSURE(IsValid());
        return Node ? TExprBase(Node) : TExprBase(Raw_);
    }

private:
    const TExprNode* Raw_;
    TExprNode::TPtr Node;
};

template<typename TNode>
class TChildIterator : public std::iterator<std::forward_iterator_tag, TNode> {
public:
    TChildIterator()
    {
        CurIt = EndIt = {};
    }

    TChildIterator(const TExprBase &node, size_t startIndex = 0)
        : CurIt(node.Ref().Children().begin() + startIndex)
        , EndIt(node.Ref().Children().end())
    {
        if (CurIt != EndIt) {
            CurNode = *CurIt;
        }
    }

    TChildIterator& operator++ () // Pre-increment
    {
        YQL_ENSURE(CurNode);
        Move();
        return *this;
    }

    TChildIterator operator++ (int) // Post-increment
    {
        YQL_ENSURE(CurNode);
        TChildIterator<TNode> tmp(*this);
        Move();
        return tmp;
    }

    bool operator ==(const TChildIterator<TNode>& rhs) const
    {
        return CurNode == rhs.CurNode;
    }

    bool operator !=(const TChildIterator<TNode>& rhs) const
    {
        return CurNode != rhs.CurNode;
    }

    TNode operator*() const
    {
        YQL_ENSURE(CurNode);
        return TNode(CurNode);
    }

private:
    void Move() {
        ++CurIt;
        CurNode = CurIt == EndIt ? nullptr : *CurIt;
    }

    TExprNode::TPtr CurNode;
    TExprNode::TListType::const_iterator CurIt;
    TExprNode::TListType::const_iterator EndIt;
};

template<typename TItem>
class TListBase : public TExprBase {
public:
    TListBase(const TExprNode* node)
        : TExprBase(node)
    {
        YQL_ENSURE(Match(node));
    }

    TListBase(const TExprNode::TPtr& node)
        : TExprBase(node)
    {
        YQL_ENSURE(Match(node.Get()));
    }

    TItem Item(size_t index) const { return TItem(Ref().ChildPtr(index)); }
    size_t Size() const { return Ref().ChildrenSize(); }
    bool Empty() const { return Size() == 0; }

    TChildIterator<TItem> begin() const { return TChildIterator<TItem>(*this); }
    TChildIterator<TItem> end() const { return TChildIterator<TItem>(); }

public:
    static bool Match(const TExprNode* node) {
        return node && node->IsList();
    }
};

template<typename TItem>
class TMaybeNode<TListBase<TItem>> : public TMaybeNode<TExprBase> {
public:
    TMaybeNode(const TExprNode* node)
        : TMaybeNode<TExprBase>(node && TListBase<TItem>::Match(node) ? node : nullptr) {}

    TMaybeNode(const TExprNode::TPtr& node)
        : TMaybeNode<TExprBase>(node && TListBase<TItem>::Match(node.Get()) ? node : TExprNode::TPtr()) {}

    TListBase<TItem> Cast() const {
        YQL_ENSURE(IsValid());
        return TMaybeNode<TExprBase>::Cast().template Cast<TListBase<TItem>>();
    }

    TMaybeNode<TItem> Item(size_t index) const {
        if (!IsValid()) {
            return TMaybeNode<TItem>();
        }

        auto list = Cast();
        if (index >= list.Size()) {
            return TMaybeNode<TItem>();
        }

        return TMaybeNode<TItem>(Ref().ChildPtr(index));
    }
};

class TCallable : public TExprBase {
public:
    explicit TCallable(const TExprNode* node)
        : TExprBase(node)
    {
        YQL_ENSURE(Match(node));
    }

    explicit TCallable(const TExprNode::TPtr& node)
        : TExprBase(node)
    {
        YQL_ENSURE(Match(node.Get()));
    }

    TStringBuf CallableName() const {
        return Ref().Content();
    }

public:
    static bool Match(const TExprNode* node) {
        return node && node->IsCallable();
    }
};

template<>
class TMaybeNode<TCallable> : public TMaybeNode<TExprBase> {
public:
    TMaybeNode(const TExprNode* node = nullptr)
        : TMaybeNode<TExprBase>(node && TCallable::Match(node) ? node : nullptr) {}

    TMaybeNode(const TExprNode::TPtr& node)
        : TMaybeNode<TExprBase>(node && TCallable::Match(node.Get()) ? node : TExprNode::TPtr()) {}

    TMaybeNode(const TCallable& node)
        : TMaybeNode(node.Ptr()) {}

    TCallable Cast() const {
        YQL_ENSURE(IsValid());
        return TMaybeNode<TExprBase>::Cast().Cast<TCallable>();
    }
};

template<typename TItem>
class TVarArgCallable : public TCallable {
public:
    explicit TVarArgCallable(const TExprNode* node)
        : TCallable(node)
    {
    }

    explicit TVarArgCallable(const TExprNode::TPtr& node)
        : TCallable(node)
    {
    }

    TItem Arg(size_t index) const {
        return TItem(Ref().ChildPtr(index));
    }
    size_t ArgCount() const {
        return Ref().ChildrenSize();
    }
    TExprNode::TChildrenType Args() const {
        return Ref().Children();
    }

    TChildIterator<TItem> begin() const {
        return TChildIterator<TItem>(*this);
    }
    TChildIterator<TItem> end() const {
        return TChildIterator<TItem>();
    }
};

template<typename TItem>
class TMaybeNode<TVarArgCallable<TItem>> : public TMaybeNode<TExprBase> {
public:
    TMaybeNode(const TExprNode* node)
        : TMaybeNode<TExprBase>(node && TVarArgCallable<TItem>::Match(node) ? node : nullptr) {}

    TMaybeNode(const TExprNode::TPtr& node)
        : TMaybeNode<TExprBase>(node && TVarArgCallable<TItem>::Match(node.Get()) ? node : TExprNode::TPtr()) {}

    TVarArgCallable<TItem> Cast() const {
        YQL_ENSURE(IsValid());
        return TMaybeNode<TExprBase>::Cast().template Cast<TVarArgCallable<TItem>>();
    }

    TMaybeNode<TItem> Arg(size_t index) const {
        if (!IsValid()) {
            return TMaybeNode<TItem>();
        }

        auto list = Cast();
        if (index >= list.ArgCount()) {
            return TMaybeNode<TItem>();
        }

        return TMaybeNode<TItem>(Ref().ChildPtr(index));
    }
};

class TArgs
{
public:
    TArgs(const TExprBase& node, size_t startIndex)
        : Node(node)
        , StartIndex(startIndex) {}

    TExprBase Get(size_t index) const { return TExprBase(Node.Ref().ChildPtr(index)); }
    size_t Count() const { return Node.Ref().ChildrenSize(); }
    TChildIterator<TExprBase> begin() const { return TChildIterator<TExprBase>(Node, StartIndex); }
    TChildIterator<TExprBase> end() const { return TChildIterator<TExprBase>(); }

private:
    TExprBase Node;
    size_t StartIndex;
};

template<const size_t FixedArgsCount>
class TFreeArgCallable : public TCallable {
public:
    explicit TFreeArgCallable(const TExprNode* node)
        : TCallable(node)
    {
        YQL_ENSURE(Match(node));
    }

    explicit TFreeArgCallable(const TExprNode::TPtr& node)
        : TCallable(node)
    {
        YQL_ENSURE(Match(node.Get()));
    }

    TArgs Args() const { return TArgs(*this, 0); }
    TArgs FreeArgs() const { return TArgs(*this, FixedArgsCount); }

    TExprBase Arg(size_t index) const { return TArgs(*this, 0).Get(index); }

    static bool Match(const TExprNode* node) {
        return node && node->IsCallable() && node->ChildrenSize() >= FixedArgsCount;
    }
};

template<const size_t FixedArgsCount>
class TMaybeNode<TFreeArgCallable<FixedArgsCount>> : public TMaybeNode<TExprBase> {
public:
    TMaybeNode(const TExprNode* node)
        : TMaybeNode<TExprBase>(node && TFreeArgCallable<FixedArgsCount>::Match(node) ? node : nullptr) {}

    TMaybeNode(const TExprNode::TPtr& node)
        : TMaybeNode<TExprBase>(node && TFreeArgCallable<FixedArgsCount>::Match(node.Get()) ? node : TExprNode::TPtr()) {}

    TMaybeNode(const TExprBase& node)
        : TMaybeNode(node) {}

    TFreeArgCallable<FixedArgsCount> Cast() const {
        YQL_ENSURE(IsValid());
        return TMaybeNode<TExprBase>::Cast().template Cast<TFreeArgCallable<FixedArgsCount>>();
    }
};

template<typename TParent, typename TNode>
class TNodeBuilder {};

class TNodeBuilderBase {
protected:
    typedef std::function<TExprBase (const TStringBuf& arg)> GetArgFuncType;

    TNodeBuilderBase(TExprContext& ctx, TPositionHandle pos, GetArgFuncType getArgFunc)
        : Ctx(ctx)
        , Pos(pos)
        , GetArgFunc(getArgFunc) {}

protected:
    TExprContext& Ctx;
    TPositionHandle Pos;
    GetArgFuncType GetArgFunc;
};

template <typename TParent, typename TDerived, typename TItem>
class TListBuilderBase : public TNodeBuilderBase {
protected:
    typedef std::function<TParent& (const TDerived&)> BuildFuncType;

    TListBuilderBase(TExprContext& ctx, TPositionHandle pos, BuildFuncType buildFunc, GetArgFuncType getArgFunc)
        : TNodeBuilderBase(ctx, pos, getArgFunc)
        , BuildFunc(buildFunc) {}

    TVector<TExprBase> Items;
    BuildFuncType BuildFunc;

public:
    typedef TDerived ResultType;

    TParent& Build() {
        TDerived node = static_cast<TNodeBuilder<TParent, TDerived>*>(this)->DoBuild();
        return BuildFunc(node);
    }

    typename TParent::ResultType Done() {
        TParent& parent = Build();
        return parent.Value();
    }

    TNodeBuilder<TParent, TDerived>& InitFrom(const ResultType& item) {
        Items.assign(item.begin(), item.end());
        return *static_cast<TNodeBuilder<TParent, TDerived>*>(this);
    }

    TNodeBuilder<TParent, TDerived>& Add(const TExprBase& node) {
        Items.push_back(node);
        return *static_cast<TNodeBuilder<TParent, TDerived>*>(this);
    }

    TNodeBuilder<TParent, TDerived>& Add(const TExprNode::TPtr& node) {
        Items.push_back(TExprBase(node));
        return *static_cast<TNodeBuilder<TParent, TDerived>*>(this);
    }

    TNodeBuilder<TNodeBuilder<TParent, TDerived>, TItem> Add() {
        return TNodeBuilder<TNodeBuilder<TParent, TDerived>, TItem>(this->Ctx, this->Pos,
            [this] (const TExprBase& node) mutable -> TNodeBuilder<TParent, TDerived>& {
                return Add(node);
            }, GetArgFunc);
    }

    template<typename TNode>
    TNodeBuilder<TNodeBuilder<TParent, TDerived>, TNode> Add() {
        return TNodeBuilder<TNodeBuilder<TParent, TDerived>, TNode>(this->Ctx, this->Pos,
            [this] (const TNode& node) mutable -> TNodeBuilder<TParent, TDerived>&{
                return Add(node);
            }, GetArgFunc);
    }

    TNodeBuilder<TParent, TDerived>& Add(const TStringBuf& argName) {
        return Add(TExprBase(this->GetArgFunc(argName)));
    }

    template <typename T, typename = std::enable_if_t<!std::is_same<T, TExprBase>::value
        && std::is_same<T, TItem>::value>>
    TNodeBuilder<TParent, TDerived>& Add(const TVector<T>& list) {
        for (auto item : list) {
            Items.push_back(item);
        }
        return *static_cast<TNodeBuilder<TParent, TDerived>*>(this);
    }

    TNodeBuilder<TParent, TDerived>& Add(const TVector<TExprBase>& list) {
        for (auto item : list) {
            Items.push_back(item);
        }
        return *static_cast<TNodeBuilder<TParent, TDerived>*>(this);
    }

    TNodeBuilder<TParent, TDerived>& Add(const TExprNode::TListType& list) {
        for (auto item : list) {
            Items.push_back(TExprBase(item));
        }
        return *static_cast<TNodeBuilder<TParent, TDerived>*>(this);
    }

    TNodeBuilder<TParent, TDerived>& Add(std::initializer_list<TExprBase> list) {
        return Add(TVector<TExprBase>(list));
    }
};

template<typename TParent>
class TNodeBuilder<TParent, TVector<TExprBase>> : public TListBuilderBase<TParent, TVector<TExprBase>, TExprBase> {
public:
    typedef std::function<TParent& (const TVector<TExprBase>&)> BuildFuncType;

    TNodeBuilder<TParent, TVector<TExprBase>>(TExprContext& ctx, TPositionHandle pos, BuildFuncType buildFunc,
        TNodeBuilderBase::GetArgFuncType getArgFunc)
        : TListBuilderBase<TParent, TVector<TExprBase>, TExprBase>(ctx, pos, buildFunc, getArgFunc) {}

    TVector<TExprBase> DoBuild() {
        return this->Items;
    }
};

template <typename TParent, typename TDerived>
class TFreeArgCallableBuilderBase : public TNodeBuilderBase {
protected:
    TFreeArgCallableBuilderBase(TExprContext& ctx, TPositionHandle pos, GetArgFuncType getArgFunc)
        : TNodeBuilderBase(ctx, pos, getArgFunc) {}

    TVector<TExprBase> FreeArgsHolder;

public:
    TNodeBuilder<TNodeBuilder<TParent, TDerived>, TVector<TExprBase>> FreeArgs() {
        return TNodeBuilder<TNodeBuilder<TParent, TDerived>, TVector<TExprBase>>(this->Ctx, this->Pos,
            [this] (const TVector<TExprBase>& freeArgs) mutable -> TNodeBuilder<TParent, TDerived>& {
                FreeArgsHolder = freeArgs;
                return *static_cast<TNodeBuilder<TParent, TDerived>*>(this);
            }, GetArgFunc);
    }
};

template<typename TNode>
class TBuildValueHolder {
public:
    typedef TNode ResultType;

    void SetValue(const TNode& node) {
        Node = node.template Maybe<TNode>();
    }

    TNode Value() const {
        YQL_ENSURE(Node);

        return Node.Cast();
    }

private:
    TMaybeNode<TNode> Node;
};

template<typename TNode>
TNodeBuilder<TBuildValueHolder<TNode>, TNode> Build(TExprContext& ctx, TPositionHandle pos) {
    TBuildValueHolder<TNode> holder;
    TNodeBuilder<TBuildValueHolder<TNode>, TNode> builder(ctx, pos,
        [holder](const TNode& node) mutable -> TBuildValueHolder<TNode>& {
            holder.SetValue(node);
            return holder;
        },
        [] (const TStringBuf& argName) -> TExprBase {
            YQL_ENSURE(false, "Argument not found: " << ToString(argName));
        });

    return builder;
}

} //namespace NNodes
} // namespace NYql
