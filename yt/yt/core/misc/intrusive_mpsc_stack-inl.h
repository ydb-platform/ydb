#ifndef INTRUSIVE_MPSC_STACK_INL_H_
#error "Direct inclusion of this file is not allowed, include intrusive_mpsc_stack.h"
// For the sake of sane code completion.
#include "intrusive_mpsc_stack.h"
#endif

#include <concepts>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class Tag>
TIntrusiveMPSCStack<T, Tag>::TIntrusiveMPSCStack() noexcept
{
    static_assert(std::derived_from<T, TIntrusiveNode<T, Tag>>, "Class must inherit from CRTP-base TIntrusiveNode");
}

template <class T, class Tag>
void TIntrusiveMPSCStack<T, Tag>::Push(TNode* item) noexcept
{
    // NB: This saves up extra CAS in case of non-empty stack.
    item->Next = Head_.load(std::memory_order::relaxed);

    while (!Head_.compare_exchange_weak(
        item->Next,
        item,
        std::memory_order::release,
        std::memory_order::relaxed))
    { }
}

template <class T, class Tag>
TSimpleIntrusiveList<T, Tag> TIntrusiveMPSCStack<T, Tag>::PopAll() noexcept
{
    TNode* head = Head_.exchange(nullptr, std::memory_order::acquire);

    TSimpleIntrusiveList<T, Tag> list;

    while (head != nullptr) {
        auto tmp = head;
        head = head->Next;
        tmp->Next = nullptr;
        list.PushFront(tmp);
    }

    return list;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
