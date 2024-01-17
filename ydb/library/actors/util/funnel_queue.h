#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/noncopyable.h>

template <typename ElementType>
class TFunnelQueue: private TNonCopyable {
public:
    TFunnelQueue() noexcept
        : Front(nullptr)
        , Back(nullptr)
    {
    }

    virtual ~TFunnelQueue() noexcept {
        for (auto entry = Front; entry; entry = DeleteEntry(entry))
            continue;
    }

    /// Push element. Can be used from many threads. Return true if is first element.
    bool
    Push(ElementType&& element) noexcept {
        TEntry* const next = NewEntry(static_cast<ElementType&&>(element));
        TEntry* const prev = AtomicSwap(&Back, next);
        AtomicSet(prev ? prev->Next : Front, next);
        return !prev;
    }

    /// Extract top element. Must be used only from one thread. Return true if have more.
    bool
    Pop() noexcept {
        if (TEntry* const top = AtomicGet(Front)) {
            const auto last = AtomicCas(&Back, nullptr, top);
            if (last) // This is last element in queue. Queue is empty now.
                AtomicCas(&Front, nullptr, top);
            else // This element is not last.
                for (;;) {
                    if (const auto next = AtomicGet(top->Next)) {
                        AtomicSet(Front, next);
                        break;
                    }
                    // But Next is null. Wait next assignment in spin lock.
                }

            DeleteEntry(top);
            return !last;
        }

        return false;
    }

    /// Peek top element. Must be used only from one thread.
    ElementType&
    Top() const noexcept {
        return AtomicGet(Front)->Data;
    }

    bool
    IsEmpty() const noexcept {
        return !AtomicGet(Front);
    }

protected:
    class TEntry: private TNonCopyable {
        friend class TFunnelQueue;

    private:
        explicit TEntry(ElementType&& element) noexcept
            : Data(static_cast<ElementType&&>(element))
            , Next(nullptr)
        {
        }

        ~TEntry() noexcept {
        }

    public:
        ElementType Data;
        TEntry* volatile Next;
    };

    TEntry* volatile Front;
    TEntry* volatile Back;

    virtual TEntry* NewEntry(ElementType&& element) noexcept {
        return new TEntry(static_cast<ElementType&&>(element));
    }

    virtual TEntry* DeleteEntry(TEntry* entry) noexcept {
        const auto next = entry->Next;
        delete entry;
        return next;
    }

protected:
    struct TEntryIter {
        TEntry* ptr;

        ElementType& operator*() {
            return ptr->Data;
        }

        ElementType* operator->() {
            return &ptr->Data;
        }

        TEntryIter& operator++() {
            ptr = AtomicGet(ptr->Next);
            return *this;
        }

        bool operator!=(const TEntryIter& other) const {
            return ptr != other.ptr;
        }

        bool operator==(const TEntryIter& other) const {
            return ptr == other.ptr;
        }
    };

    struct TConstEntryIter {
        const TEntry* ptr;

        const ElementType& operator*() {
            return ptr->Data;
        }

        const ElementType* operator->() {
            return &ptr->Data;
        }

        TEntryIter& operator++() {
            ptr = AtomicGet(ptr->Next);
            return *this;
        }

        bool operator!=(const TConstEntryIter& other) const {
            return ptr != other.ptr;
        }

        bool operator==(const TConstEntryIter& other) const {
            return ptr == other.ptr;
        }
    };

public:
    using const_iterator = TConstEntryIter;
    using iterator = TEntryIter;

    iterator begin() {
        return {AtomicGet(Front)};
    }
    const_iterator cbegin() {
        return {AtomicGet(Front)};
    }
    const_iterator begin() const {
        return {AtomicGet(Front)};
    }

    iterator end() {
        return {nullptr};
    }
    const_iterator cend() {
        return {nullptr};
    }
    const_iterator end() const {
        return {nullptr};
    }
};

template <typename ElementType>
class TPooledFunnelQueue: public TFunnelQueue<ElementType> {
public:
    TPooledFunnelQueue() noexcept
        : Stack(nullptr)
    {
    }

    virtual ~TPooledFunnelQueue() noexcept override {
        for (auto entry = TBase::Front; entry; entry = TBase::DeleteEntry(entry))
            continue;
        for (auto entry = Stack; entry; entry = TBase::DeleteEntry(entry))
            continue;
        TBase::Back = TBase::Front = Stack = nullptr;
    }

private:
    typedef TFunnelQueue<ElementType> TBase;

    typename TBase::TEntry* volatile Stack;

protected:
    virtual typename TBase::TEntry* NewEntry(ElementType&& element) noexcept override {
        while (const auto top = AtomicGet(Stack))
            if (AtomicCas(&Stack, top->Next, top)) {
                top->Data = static_cast<ElementType&&>(element);
                AtomicSet(top->Next, nullptr);
                return top;
            }

        return TBase::NewEntry(static_cast<ElementType&&>(element));
    }

    virtual typename TBase::TEntry* DeleteEntry(typename TBase::TEntry* entry) noexcept override {
        entry->Data = ElementType();
        const auto next = entry->Next;
        do
            AtomicSet(entry->Next, AtomicGet(Stack));
        while (!AtomicCas(&Stack, entry, entry->Next));
        return next;
    }
};

template <typename ElementType, template <typename T> class TQueueType = TFunnelQueue>
class TCountedFunnelQueue: public TQueueType<ElementType> {
public:
    TCountedFunnelQueue() noexcept
        : Count(0)
    {
    }

    TAtomicBase GetSize() const noexcept {
        return AtomicGet(Count);
    }

private:
    typedef TQueueType<ElementType> TBase;

    virtual typename TBase::TEntry* NewEntry(ElementType&& element) noexcept override {
        AtomicAdd(Count, 1);
        return TBase::NewEntry(static_cast<ElementType&&>(element));
    }

    virtual typename TBase::TEntry* DeleteEntry(typename TBase::TEntry* entry) noexcept override {
        AtomicSub(Count, 1);
        return TBase::DeleteEntry(entry);
    }

    TAtomic Count;
};
