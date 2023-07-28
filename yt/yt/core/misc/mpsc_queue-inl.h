#ifndef MPSC_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include mpsc_queue.h"
// For the sake of sane code completion.
#include "mpsc_queue.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TMpscQueue<T>::TNode
{
    T Value;
    TNode* Next = nullptr;

    explicit TNode(const T& value)
        : Value(value)
    { }

    explicit TNode(T&& value)
        : Value(std::move(value))
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TMpscQueue<T>::~TMpscQueue()
{
    DeleteNodeList(Head_.load());
    DeleteNodeList(Tail_);
}

template <class T>
void TMpscQueue<T>::Enqueue(const T& value)
{
    DoEnqueue(new TNode(value));
}

template <class T>
void TMpscQueue<T>::Enqueue(T&& value)
{
    DoEnqueue(new TNode(std::move(value)));
}

template <class T>
void TMpscQueue<T>::DoEnqueue(TNode* node)
{
    auto* expectedHead = Head_.load(std::memory_order::relaxed);
    do {
        node->Next = expectedHead;
    } while (!Head_.compare_exchange_weak(expectedHead, node));
}

template <class T>
bool TMpscQueue<T>::TryDequeue(T* value)
{
    if (!Tail_) {
        Tail_ = Head_.exchange(nullptr);
        if (auto* current = Tail_) {
            auto* next = current->Next;
            current->Next = nullptr;
            while (next) {
                auto* second = next->Next;
                next->Next = current;
                current = next;
                next = second;
            }
            Tail_ = current;
        }
    }

    if (!Tail_) {
        return false;
    }

    *value = std::move(Tail_->Value);
    delete std::exchange(Tail_, Tail_->Next);

    return true;
}

template <class T>
bool TMpscQueue<T>::IsEmpty() const
{
    return !Tail_ && !Head_.load();
}

template <class T>
void TMpscQueue<T>::DrainConsumer()
{
    DeleteNodeList(std::exchange(Tail_, nullptr));
    DrainProducer();
}

template <class T>
void TMpscQueue<T>::DrainProducer()
{
    while (auto* head = Head_.exchange(nullptr)) {
        DeleteNodeList(head);
    }
}

template <class T>
void TMpscQueue<T>::DeleteNodeList(TNode* node)
{
    auto* current = node;
    while (current) {
        delete std::exchange(current, current->Next);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
