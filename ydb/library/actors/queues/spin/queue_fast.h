template <ui32 MaxSizeBits, typename TObserver=void>
struct TMPMCRingQueue {

    static constexpr ui32 MaxSize = 1 << MaxSizeBits;

    struct alignas(ui64) TSlot {
        static constexpr ui64 EmptyBit = 1ull << 63;
        ui64 Generation = 0;
        ui64 Value = 0;
        bool IsEmpty;

        static constexpr ui64 MakeEmpty(ui64 generation) {
            return EmptyBit | generation;
        }

        static constexpr TSlot Recognise(ui64 slotValue) {
            if (slotValue & EmptyBit) {
                return {.Generation = (EmptyBit ^ slotValue), .IsEmpty=true};
            }
            return {.Value=slotValue, .IsEmpty=false};
        }
    };

    std::atomic<ui64> Tail{0};
    std::atomic<ui64> Head{0};
    TArrayHolder<std::atomic<ui64>> Buffer;

    TMPMCRingQueue()
        : Buffer(new std::atomic<ui64>[MaxSize])
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            Buffer[idx] = TSlot::MakeEmpty(0);
        }
    }

    void TryIncrementTail(ui64 &currentTail) {
        ui64 expectedTail = currentTail;
        while (expectedTail <= currentTail) {
            if (Tail.compare_exchange_weak(expectedTail, currentTail + 1)) {
                currentTail++;
                return;
            }
        }
        currentTail = expectedTail;
    }

    void TryIncrementHead(ui64 &currentHead) {
        ui64 expectedHead = currentHead;
        while (expectedHead <= currentHead) {
            if (Head.compare_exchange_weak(expectedHead, currentHead + 1)) {
                currentHead++;
                return;
            }
        }
        currentHead = expectedHead;
    }

    bool TryPushSlow(ui32 val) {
        ui64 currentTail = Tail.load(std::memory_order_acquire);

        for (ui32 it = 0;; ++it) {
            ui64 generation = currentTail / MaxSize;
            std::atomic<ui64> &currentSlot = Buffer[currentTail % MaxSize];
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(generation);
            do {
                if (currentSlot.compare_exchange_weak(expected, val)) {
                    Tail.compare_exchange_strong(currentTail, currentTail + 1);
                    return true;
                }
                slot = TSlot::Recognise(expected);
            } while (slot.Generation <= generation && slot.IsEmpty);

            if (!slot.IsEmpty) {
                ui64 currentHead = Head.load(std::memory_order_acquire);
                if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
                    return false;
                }
            }

            TryIncrementTail(currentTail);
            generation = currentTail / MaxSize;
        }
    }

    bool TryPushFast(ui32 val) {
        ui64 currentTail = Tail.fetch_add(1, std::memory_order_release);
        ui64 generation = currentTail / MaxSize;

        std::atomic<ui64> &currentSlot = Buffer[currentTail % MaxSize];
        TSlot slot;
        ui64 expected = TSlot::MakeEmpty(generation);
        do {
            if (currentSlot.compare_exchange_weak(expected, val)) {
                return true;
            }
        } while (slot.Generation <= generation && slot.IsEmpty);

        if (!slot.IsEmpty) {
            ui64 currentHead = Head.load(std::memory_order_acquire);
            if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
                return false;
            }
        }

        return TryPushSlow(val);
    }

    void TryIncrementHead(ui64 &currentHead) {
        ui64 expectedHead = currentHead;
        while (expectedHead <= currentHead) {
            if (Head.compare_exchange_weak(expectedHead, currentHead + 1)) {
                currentHead++;
                return;
            }
        }
        currentHead = expectedHead;
    }

    std::optional<ui32> TryPopReallySlow() {
        ui64 currentHead = Head.load(std::memory_order_acquire);
        ui64 currentTail = Tail.load(std::memory_order_acquire);
        while (currentHead > currentTail) {
            if (Tail.compare_exchange_weak(currentTail, currentHead)) {
                currentTail = currentHead;
            }
        }
        if (currentHead == currentTail) {
            return std::nullopt;
        }

        return TryPopSlow(currentHead);
    }

    std::optional<ui32> TryPopSlow(ui64 currentHead) {
        if (!currentHead) {
            currentHead = Head.load(std::memory_order_acquire);
        }
        ui32 generation = currentHead / MaxSize;
        for (ui32 it = 0;; ++it) {
            std::atomic<ui64> &currentSlot = Buffer[currentHead % MaxSize];
            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            if (slot.Generation > generation) {
                TryIncrementHead(currentHead);
                continue;
            }

            while (generation > slot.Generation || !slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        TryIncrementHead(currentHead);
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                TryIncrementHead(currentHead);
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail <= currentHead) {
                return std::nullopt;
            }

            while (slot.Generation <= generation || !slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        TryIncrementHead(currentHead);
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            currentHead = Head.load(std::memory_order_acquire);
            generation = currentHead / MaxSize;
        }
        return std::nullopt;
    }

    std::optional<ui32> TryPopSlow() {
        return TryPopSlow(0);
    }

    std::optional<ui32> TryPopFast() {
        for (ui32 it = 0;; ++it) {
            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            ui64 generation = currentHead / MaxSize;
            std::atomic<ui64> &currentSlot = Buffer[currentHead % MaxSize];
            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            while (generation >= slot.Generation) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail > currentHead) {
                continue;
            }

            while (currentTail <= currentHead) {
                if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                    return std::nullopt;
                }
            }
        }
    }

};