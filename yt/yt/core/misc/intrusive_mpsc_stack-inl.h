#ifndef INTRUSIVE_MPSC_STACK_INL_H_
#error "Direct inclusion of this file is not allowed, include intrusive_mpsc_stack.h"
// For the sake of sane code completion.
#include "intrusive_mpsc_stack.h"
#endif

#include <concepts>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class Tag>
TIntrusiveMpscStack<T, Tag>::TIntrusiveMpscStack() noexcept
{
    static_assert(std::derived_from<T, TIntrusiveListItem<T, Tag>>, "Class must inherit from CRTP-base TIntrusiveListItem");
}

template <class T, class Tag>
void TIntrusiveMpscStack<T, Tag>::Push(TNode* item) noexcept
{
    YT_VERIFY(item->Empty());
    // Past this line item is not a valid instance of TInstrusiveListItem.

    // NB: This saves up extra CAS in case of non-empty stack.
    item->MutableNext() = Head_.load(std::memory_order::relaxed);

    while (!Head_.compare_exchange_weak(
        item->MutableNext(),
        item,
        std::memory_order::release,
        std::memory_order::relaxed))
    { }
}

template <class T, class Tag>
TIntrusiveList<T, Tag> TIntrusiveMpscStack<T, Tag>::PopAll() noexcept
{
    TNode* head = Head_.exchange(nullptr, std::memory_order::acquire);

    TIntrusiveList<T, Tag> list;

    while (head) {
        auto tmp = head;
        head = head->Next();

        // From this line tmp is a valid instance of TIntrusiveListItem.
        tmp->ResetItem();
        list.PushFront(tmp);
    }

    return list;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
