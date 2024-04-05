#ifndef INTRUSIVE_LIST_INL_H_
#error "Direct inclusion of this file is not allowed, include intrusive_list.h"
// For the sake of sane code completion.
#include "intrusive_list.h"
#endif

#include <concepts>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class Tag>
void TIntrusiveNode<T, Tag>::Unlink() noexcept
{
    if (Next) {
        Next->Prev = Prev;
    }

    if (Prev) {
        Prev->Next = Next;
    }

    Prev = Next = nullptr;
}

template <class T, class Tag>
void TIntrusiveNode<T, Tag>::LinkBefore(TIntrusiveNode* next) noexcept
{
    YT_VERIFY(!IsLinked());

    Prev = next->Prev;
    Prev->Next = this;
    Next = next;
    next->Prev = this;
}

template <class T, class Tag>
bool TIntrusiveNode<T, Tag>::IsLinked() const noexcept
{
    return (Next != nullptr) || (Prev != nullptr);
}

template <class T, class Tag>
T* TIntrusiveNode<T, Tag>::AsItem() noexcept
{
    return static_cast<T*>(this);
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class Tag>
TSimpleIntrusiveList<T, Tag>::TSimpleIntrusiveList() noexcept
{
    InitializeEmpty();
}

template <class T, class Tag>
TSimpleIntrusiveList<T, Tag>::TSimpleIntrusiveList(TList&& other) noexcept
{
    InitializeEmpty();
    Append(std::move(other));
}

template <class T, class Tag>
void TSimpleIntrusiveList<T, Tag>::PushBack(TNode* node) noexcept
{
    node->LinkBefore(&Head_);
}

template <class T, class Tag>
void TSimpleIntrusiveList<T, Tag>::PushFront(TNode* node) noexcept
{
    node->LinkBefore(Head_.Next);
}

template <class T, class Tag>
T*TSimpleIntrusiveList<T, Tag>::PopBack() noexcept
{
    if (IsEmpty()) {
        return nullptr;
    }

    TNode* back = Head_.Prev;
    back->Unlink();
    return back->AsItem();
}

template <class T, class Tag>
T* TSimpleIntrusiveList<T, Tag>::PopFront() noexcept
{
    if (IsEmpty()) {
        return nullptr;
    }

    TNode* front = Head_.Next;
    front->Unlink();
    return front->AsItem();
}

template <class T, class Tag>
void TSimpleIntrusiveList<T, Tag>::Append(TList&& other) noexcept
{
    if (other.IsEmpty()) {
        return;
    }

    auto* other_front = other.Head_.Next;
    auto* current_back = Head_.Prev;
    current_back->Next = other_front;
    other_front->Prev = current_back;

    auto* other_back = other.Head_.Prev;
    other_back->Next = &Head_;
    Head_.Prev = other_back;

    other.InitializeEmpty();
}

template <class T, class Tag>
bool TSimpleIntrusiveList<T, Tag>::IsEmpty() const noexcept
{
    return Head_.Next == &Head_;
}

template <class T, class Tag>
TIntrusiveNode<T, Tag>* TSimpleIntrusiveList<T, Tag>::Begin()
{
    return Head_.Next;
}

template <class T, class Tag>
TIntrusiveNode<T, Tag>* TSimpleIntrusiveList<T, Tag>::End()
{
    return &Head_;
}

template <class T, class Tag>
TSimpleIntrusiveList<T, Tag>::~TSimpleIntrusiveList()
{
    YT_VERIFY(IsEmpty());
}

template <class T, class Tag>
void TSimpleIntrusiveList<T, Tag>::InitializeEmpty()
{
    Head_.Next = Head_.Prev = &Head_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
