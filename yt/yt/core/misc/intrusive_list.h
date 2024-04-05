#pragma once

#include <cstdlib>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TIntrusiveNodeDefaultTag
{ };

////////////////////////////////////////////////////////////////////////////////

// NB1: util/intrlist.h inits pointers differently
// and also doesn't provide a way to directly modify
// Next/Prev making it unusable in lockfree.
// NB2: yt/containers/intrusive_linked_list.h doesn't provide tag
// and gives quite a bit of overhead in the list (ItemToNode field, extra pointer
// and size field). It would be a bit more annoying to hardcode in introspection.
// TODO(arkady-e1ppa): Change util/intrlist.h to support lf algos and use it one day?
template <class T, class Tag = TIntrusiveNodeDefaultTag>
class TIntrusiveNode
{
public:
    TIntrusiveNode* Next = nullptr;
    TIntrusiveNode* Prev = nullptr;

    TIntrusiveNode() = default;

    TIntrusiveNode(const TIntrusiveNode& other) = delete;
    TIntrusiveNode& operator=(const TIntrusiveNode& other) = delete;

    void Unlink() noexcept;

    void LinkBefore(TIntrusiveNode* next) noexcept;

    bool IsLinked() const noexcept;

    T* AsItem() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class Tag = TIntrusiveNodeDefaultTag>
class TSimpleIntrusiveList
{
    using TNode = TIntrusiveNode<T, Tag>;
    using TList = TSimpleIntrusiveList<T, Tag>;

public:
    TSimpleIntrusiveList() noexcept;
    TSimpleIntrusiveList(TSimpleIntrusiveList&& other) noexcept;

    TSimpleIntrusiveList(const TSimpleIntrusiveList& other) = delete;
    TSimpleIntrusiveList& operator=(const TSimpleIntrusiveList& other) = delete;
    TSimpleIntrusiveList& operator=(TSimpleIntrusiveList&& other) = delete;

    ~TSimpleIntrusiveList();

    void PushBack(TNode* node) noexcept;
    void PushFront(TNode* node) noexcept;

    T* PopBack() noexcept;
    T* PopFront() noexcept;

    void Append(TList&& other) noexcept;

    bool IsEmpty() const noexcept;

    TNode* Begin();

    TNode* End();

private:
    // Sentinel node.
    TNode Head_;

    void InitializeEmpty();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INTRUSIVE_LIST_INL_H_
#include "intrusive_list-inl.h"
#undef INTRUSIVE_LIST_INL_H_
