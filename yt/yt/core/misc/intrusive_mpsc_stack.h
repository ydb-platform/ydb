#pragma once

#include <util/generic/intrlist.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class Tag = TIntrusiveListDefaultTag>
class TIntrusiveMpscStack
{
    using TNode = TIntrusiveListItem<T, Tag>;

public:
    TIntrusiveMpscStack() noexcept;

    void Push(TNode* item) noexcept;

    TIntrusiveList<T, Tag> PopAll() noexcept;

private:
    std::atomic<TNode*> Head_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INTRUSIVE_MPSC_STACK_INL_H_
#include "intrusive_mpsc_stack-inl.h"
#undef INTRUSIVE_MPSC_STACK_INL_H_
