#pragma once

#include "defs.h"
#include <memory>
#include <type_traits>

template<typename T, ui32 TSize>
struct TSimpleQueueChunk {
    static const ui32 EntriesCount = (TSize - sizeof(TSimpleQueueChunk*)) / sizeof(T);
    static_assert(EntriesCount > 0, "expect EntriesCount > 0");

    union {
        T Entries[EntriesCount];
        char Data[EntriesCount * sizeof(T)];
    };
    TSimpleQueueChunk* Next = nullptr;

    TSimpleQueueChunk() {}
    ~TSimpleQueueChunk() {}
};

template<typename T, ui32 TSize, typename TChunk = TSimpleQueueChunk<T, TSize>>
class TQueueInplace {
    TChunk* ReadFrom;
    ui32 ReadPosition;
    ui32 WritePosition;
    TChunk* WriteTo;
    size_t Size;

public:
    TQueueInplace() noexcept
        : ReadFrom(nullptr)
        , ReadPosition(0)
        , WritePosition(0)
        , WriteTo(nullptr)
        , Size(0)
    {}

    TQueueInplace(TQueueInplace&& rhs) noexcept
        : ReadFrom(rhs.ReadFrom)
        , ReadPosition(rhs.ReadPosition)
        , WritePosition(rhs.WritePosition)
        , WriteTo(rhs.WriteTo)
        , Size(rhs.Size)
    {
        rhs.ReadFrom = nullptr;
        rhs.ReadPosition = 0;
        rhs.WritePosition = 0;
        rhs.WriteTo = nullptr;
        rhs.Size = 0;
    }

    TQueueInplace& operator=(TQueueInplace&& rhs) noexcept {
        if (this != &rhs) [[likely]] {
            Clear();
            ReadFrom = rhs.ReadFrom;
            ReadPosition = rhs.ReadPosition;
            WritePosition = rhs.WritePosition;
            WriteTo = rhs.WriteTo;
            Size = rhs.Size;
            rhs.ReadFrom = nullptr;
            rhs.ReadPosition = 0;
            rhs.WritePosition = 0;
            rhs.WriteTo = nullptr;
            rhs.Size = 0;
        }
        return *this;
    }

    ~TQueueInplace() {
        Clear();
    }

    void Clear() noexcept {
        TChunk* head = ReadFrom;
        if (head) {
            if constexpr (std::is_trivially_destructible_v<T>) {
                do {
                    TChunk* next = head->Next;
                    delete head;
                    head = next;
                } while (head);
            } else {
                ui32 start = ReadPosition;
                do {
                    TChunk* next = head->Next;
                    ui32 end = next ? TChunk::EntriesCount : WritePosition;
                    for (ui32 index = start; index != end; ++index) {
                        std::destroy_at(&head->Entries[index]);
                    }
                    delete head;
                    head = next;
                    start = 0;
                } while (head);
            }
            ReadFrom = nullptr;
            ReadPosition = 0;
            WritePosition = 0;
            WriteTo = nullptr;
            Size = 0;
        }
    }

    void Push(const T& x) {
        ::new (NewEntry()) T(x);
        ++WritePosition;
        ++Size;
    }

    void Push(T&& x) {
        ::new (NewEntry()) T(std::move(x));
        ++WritePosition;
        ++Size;
    }

    template<class... TArgs>
    T& Emplace(TArgs&&... args) {
        T& result = *::new (NewEntry()) T(std::forward<TArgs>(args)...);
        ++WritePosition;
        ++Size;
        return result;
    }

    T* Head() noexcept {
        TChunk* head = ReadFrom;
        if (head == WriteTo && ReadPosition == WritePosition) {
            // Note: this also handles ReadFrom == WriteTo == nullptr
            return nullptr;
        }
        if (ReadPosition == TChunk::EntriesCount) [[unlikely]] {
            TChunk* next = head->Next;
            if (!next) {
                return nullptr;
            }
            delete head;
            head = next;
            ReadFrom = next;
            ReadPosition = 0;
        }
        return &head->Entries[ReadPosition];
    }

    void Pop() {
        if (T* x = Head()) [[likely]] {
            std::destroy_at(x);
            ++ReadPosition;
            --Size;
        }
    }

    T PopDefault() {
        if (T* x = Head()) [[likely]] {
            T result(std::move(*x));
            std::destroy_at(x);
            ++ReadPosition;
            --Size;
            return result;
        } else {
            return T{};
        }
    }

    explicit operator bool() const {
        return Size > 0;
    }

    size_t GetSize() const {
        return Size;
    }

private:
    void* NewEntry() {
        if (WriteTo) [[likely]] {
            if (WritePosition == TChunk::EntriesCount) [[unlikely]] {
                TChunk* next = new TChunk;
                WriteTo->Next = next;
                WriteTo = next;
                WritePosition = 0;
            }
        } else {
            // Note: ReadPosition == WritePosition == 0
            ReadFrom = WriteTo = new TChunk;
        }
        return &WriteTo->Entries[WritePosition];
    }
};
