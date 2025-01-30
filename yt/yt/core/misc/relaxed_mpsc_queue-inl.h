#ifndef RELAXED_MPSC_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include relaxed_mpsc_queue.h"
// For the sake of sane code completion.
#include "relaxed_mpsc_queue.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, TRelaxedMpscQueueHook T::*Hook>
TRelaxedIntrusiveMpscQueue<T, Hook>::~TRelaxedIntrusiveMpscQueue()
{
    while (auto item = TryDequeue()) {
    }
}

template <class T, TRelaxedMpscQueueHook T::*Hook>
void TRelaxedIntrusiveMpscQueue<T, Hook>::Enqueue(std::unique_ptr<T> node)
{
    EnqueueImpl(HookFromNode(node.release()));
}

template <class T, TRelaxedMpscQueueHook T::*Hook>
std::unique_ptr<T> TRelaxedIntrusiveMpscQueue<T, Hook>::TryDequeue()
{
    return std::unique_ptr<T>(NodeFromHook(TryDequeueImpl()));
}

template <class T, TRelaxedMpscQueueHook T::*Hook>
TRelaxedMpscQueueHook* TRelaxedIntrusiveMpscQueue<T, Hook>::HookFromNode(T* node) noexcept
{
    if (!node) {
        return nullptr;
    }
    return &(node->*Hook);
}

template <class T, TRelaxedMpscQueueHook T::*Hook>
T* TRelaxedIntrusiveMpscQueue<T, Hook>::NodeFromHook(TRelaxedMpscQueueHook* hook) noexcept
{
    if (!hook) {
        return nullptr;
    }
    const T* const fakeNode = nullptr;
    const char* const fakeHook = static_cast<const char*>(static_cast<const void*>(&(fakeNode->*Hook)));
    const size_t offset = fakeHook - static_cast<const char*>(static_cast<const void*>(fakeNode));
    return static_cast<T*>(static_cast<void*>(static_cast<char*>(static_cast<void*>(hook)) - offset));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TRelaxedMpscQueue<T>::TNode::TNode(T&& value)
    : Value(std::move(value))
{ }

template <class T>
void TRelaxedMpscQueue<T>::Enqueue(T&& value)
{
    Impl_.Enqueue(std::make_unique<TNode>(std::move(value)));
}

template <class T>
bool TRelaxedMpscQueue<T>::TryDequeue(T* value)
{
    auto node = Impl_.TryDequeue();
    if (!node) {
        return false;
    }
    *value = std::move(node->Value);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
