#ifndef MPSC_STACK_INL_H_
#error "Direct inclusion of this file is not allowed, include mpsc_stack.h"
// For the sake of sane code completion.
#include "mpsc_stack.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TMpscStack<T>::TNode
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
TMpscStack<T>::~TMpscStack()
{
    auto* current = Head_.load();
    while (current) {
        auto* next = current->Next;
        delete current;
        current = next;
    }
}

template <class T>
void TMpscStack<T>::Enqueue(const T& value)
{
    auto node = new TNode(value);
    DoEnqueue(node, node);
}

template <class T>
void TMpscStack<T>::Enqueue(T&& value)
{
    auto node = new TNode(std::move(value));
    DoEnqueue(node, node);
}

template <class T>
void TMpscStack<T>::DoEnqueue(TNode* head, TNode* tail)
{
    auto* expected = Head_.load(std::memory_order::relaxed);
    do {
        tail->Next = expected;
    } while (!Head_.compare_exchange_weak(expected, head));
}

template <class T>
bool TMpscStack<T>::TryDequeue(T* value)
{
    auto* expected = Head_.load();
    do {
        if (!expected) {
            return false;
        }
    } while (!Head_.compare_exchange_weak(expected, expected->Next));

    *value = std::move(expected->Value);
    delete expected;
    return true;
}

template <class T>
std::vector<T> TMpscStack<T>::DequeueAll(bool reverse)
{
    std::vector<T> results;
    DequeueAll(reverse, [&results] (T& value) {
        results.push_back(std::move(value));
    });
    return results;
}

template <class T>
template <class F>
bool TMpscStack<T>::DoDequeueAll(bool reverse, F&& functor)
{
    auto* current = Head_.exchange(nullptr);
    if (!current) {
        return false;
    }
    if (reverse) {
        auto* next = current->Next;
        current->Next = nullptr;
        while (next) {
            auto* second = next->Next;
            next->Next = current;
            current = next;
            next = second;
        }
    }
    while (current) {
        auto* next = current->Next;
        functor(current);
        current = next;
    }
    return true;
}

template <class T>
template <class F>
bool TMpscStack<T>::DequeueAll(bool reverse, F&& functor)
{
    return DoDequeueAll(reverse, [&] (TNode* node) {
        functor(node->Value);
        delete node;
    });
}

template <class T>
template <class F>
void TMpscStack<T>::FilterElements(F&& functor)
{
    TNode* filteredHead = nullptr;
    TNode* filteredTail = nullptr;

    DoDequeueAll(false, [&] (TNode* node) {
        if (functor(node->Value)) {
            node->Next = nullptr;
            if (filteredTail) {
                filteredTail->Next = node;
            } else {
                filteredHead = node;
            }
            filteredTail = node;
        } else {
            delete node;
        }
    });

    if (filteredHead) {
        DoEnqueue(filteredHead, filteredTail);
    }
}

template <class T>
bool TMpscStack<T>::IsEmpty() const
{
    return !Head_.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
