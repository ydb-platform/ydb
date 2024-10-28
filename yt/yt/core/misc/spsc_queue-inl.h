#ifndef SPSC_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include spsc_queue.h"
// For the sake of sane code completion.
#include "spsc_queue.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TSpscQueue<T>::TNode
{
    std::atomic<TNode*> Next = nullptr;
    size_t Offset = 0;
    T Data[BufferSize];
};

template <class T>
TSpscQueue<T>::TSpscQueue()
{
    auto* initialNode = new TNode();
    Head_ = initialNode;
    Tail_ = initialNode;
}

template <class T>
TSpscQueue<T>::~TSpscQueue()
{
    auto current = Head_;
    while (current) {
        auto next = current->Next.load(std::memory_order::acquire);
        delete current;
        current = next;
    }
}

template <class T>
void TSpscQueue<T>::Push(T&& element)
{
    auto count = Count_.load(std::memory_order::acquire);
    size_t position = count - Tail_->Offset;

    if (Y_UNLIKELY(position == BufferSize)) {
        auto oldTail = Tail_;
        Tail_ = new TNode();
        Tail_->Offset = count;
        oldTail->Next.store(Tail_);
        position = 0;
    }

    Tail_->Data[position] = std::move(element);
    Count_.store(count + 1, std::memory_order::release);
}

template <class T>
T* TSpscQueue<T>::Front() const
{
    if (Y_UNLIKELY(Offset_ >= CachedCount_)) {
        auto count = Count_.load(std::memory_order::acquire);
        CachedCount_ = count;

        if (Offset_ >= count) {
            return nullptr;
        }
    }

    while (Y_UNLIKELY(Offset_ >= Head_->Offset + BufferSize)) {
        auto next = Head_->Next.load(std::memory_order::acquire);
        YT_VERIFY(next);
        delete Head_;
        Head_ = next;
    }

    auto position = Offset_ - Head_->Offset;
    return &Head_->Data[position];
}

template <class T>
void TSpscQueue<T>::Pop()
{
    ++Offset_;
}

template <class T>
bool TSpscQueue<T>::IsEmpty() const
{
    auto count = Count_.load();
    YT_ASSERT(Offset_ <= count);
    return Offset_ == count;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
