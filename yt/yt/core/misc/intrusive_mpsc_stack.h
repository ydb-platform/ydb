#pragma once

#include "intrusive_list.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class Tag = TIntrusiveNodeDefaultTag>
class TIntrusiveMPSCStack
{
    using TNode = TIntrusiveNode<T, Tag>;

public:
    TIntrusiveMPSCStack() noexcept;

    void Push(TNode* item) noexcept;

    TSimpleIntrusiveList<T, Tag> PopAll() noexcept;

private:
    std::atomic<TNode*> Head_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INTRUSIVE_MPSC_STACK_INL_H_
#include "intrusive_mpsc_stack-inl.h"
#undef INTRUSIVE_MPSC_STACK_INL_H_
