#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/thread/singleton.h>

#include <atomic>

namespace NKikimr {

    namespace NDetail {
        /**
         * Provides per-thread storage to store pointers
         */
        class TPerThreadStorage
            : public TThrRefBase
            , private TNonCopyable
        {
        public:
            static constexpr size_t FAST_COUNT = 1024;
            static constexpr size_t FAST_MASK = FAST_COUNT - 1;

        public:
            TPerThreadStorage() noexcept;

            void* Get(size_t key) {
                if (key < FAST_COUNT) {
                    return Values[key].load(std::memory_order_acquire);
                }

                auto it = Slow.find(key);
                if (Y_LIKELY(it != Slow.end())) {
                    void* ptr = it->second.load(std::memory_order_acquire);
                    Y_DEBUG_ABORT_UNLESS(ptr != nullptr, "Unexpected null current value in a slow slot");
                    return ptr;
                }

                return nullptr;
            }

            /**
             * Sets key to the specified pointer and returns a non-zero token
             */
            uintptr_t Set(size_t key, void* ptr);

            /**
             * Clears a previously acquired token, may be called from other threads
             */
            void Clear(uintptr_t token, void* ptr);

            static size_t AcquireKey() noexcept;

            static void ReleaseKey(size_t key) noexcept;

            static const TIntrusivePtr<TPerThreadStorage>& Current() {
                struct TCurrent {
                    TIntrusivePtr<TPerThreadStorage> Current;
                };

                auto& tls = *FastTlsSingleton<TCurrent>();
                if (Y_UNLIKELY(!tls.Current)) {
                    tls.Current = new TPerThreadStorage;
                }
                return tls.Current;
            }

        private:
            using TSlowSlot = std::atomic<void*>;
            using TSlowHashMap = THashMap<size_t, TSlowSlot>;

        private:
            std::atomic<void*> Values[FAST_COUNT] = { { 0 } };

            TSlowHashMap Slow;
            std::atomic<size_t> SlowGarbage{ 0 };
        };
    }

    /**
     * Fast thread local storage suitable for short lived objects
     */
    template<class T>
    class TFastThreadLocal : private TNonCopyable {
    private:
        class TValueStorage : private TNonCopyable {
        public:
            TValueStorage(const TFastThreadLocal<T>* owner, TIntrusivePtr<NDetail::TPerThreadStorage> storage)
                : Next(nullptr)
                , Owner(owner)
                , Storage(std::move(storage))
                , Token(0)
                , Value()
            { }

            ~TValueStorage() {
                if (Token) {
                    Storage->Clear(Token, this);
                }
            }

            T* Data() noexcept {
                return &Value;
            }

            const T* Data() const noexcept {
                return &Value;
            }

            void Initialize(size_t key) {
                Token = Storage->Set(key, this);
                Y_ABORT_UNLESS(Token != 0);
            }

        public:
            TValueStorage* Next;
            const TFastThreadLocal<T>* const Owner;

        private:
            const TIntrusivePtr<NDetail::TPerThreadStorage> Storage;
            uintptr_t Token;
            T Value;
        };

    public:
        class TIterator {
        public:
            TIterator(const TValueStorage* ptr = nullptr)
                : Current(ptr)
            { }

            const T& operator*() const {
                return *Current->Data();
            }

            const T* operator->() const {
                return Current->Data();
            }

            bool IsValid() const {
                return Current;
            }

            void Next() {
                if (Current) {
                    Current = Current->Next;
                }
            }

        private:
            const TValueStorage* Current;
        };

    public:
        TFastThreadLocal()
            : Key(NDetail::TPerThreadStorage::AcquireKey())
        { }

        ~TFastThreadLocal() {
            // N.B. acquire semantics needed because destructor may run from any thread
            auto* head = Head.exchange((TValueStorage*)-1, std::memory_order_acquire);
            while (head) {
                auto* next = head->Next;
                delete head;
                head = next;
            }

            NDetail::TPerThreadStorage::ReleaseKey(Key);
        }

        TIterator Iterator() const {
            return TIterator(Head.load(std::memory_order_acquire));
        }

        T* GetPtr() {
            return this->GetValueStorage().Data();
        }

        const T* GetPtr() const {
            return this->GetValueStorage().Data();
        }

        T& Get() { return *this->GetPtr(); }
        const T& Get() const { return *this->GetPtr(); }

        T* operator->() { return GetPtr(); }
        const T* operator->() const { return GetPtr(); }

        operator T&() { return Get(); }
        operator const T&() const { return Get(); }

        template<class TArg>
        T& operator=(TArg&& value) {
            return Get() = std::forward<TArg>(value);
        }

    private:
        TValueStorage& GetValueStorage() const {
            auto& tls = NDetail::TPerThreadStorage::Current();
            TValueStorage* ptr = reinterpret_cast<TValueStorage*>(tls->Get(Key));

            if (Y_UNLIKELY(!ptr)) {
                THolder<TValueStorage> storage = MakeHolder<TValueStorage>(this, tls);
                storage->Initialize(Key);
                ptr = storage.Release();

                auto* head = Head.load(std::memory_order_relaxed);
                do {
                    Y_ABORT_UNLESS(head != (TValueStorage*)-1, "TFastThreadLocal::GetValueStorage() race with destructor");
                    ptr->Next = head;
                } while (!Head.compare_exchange_weak(head, ptr, std::memory_order_release));
            }

            Y_ABORT_UNLESS(ptr->Owner == this, "TFastThreadLocal::GetValueStorage() found unexpected storage pointer");

            return *ptr;
        }

    private:
        const size_t Key;
        mutable std::atomic<TValueStorage*> Head{ nullptr };
    };

}
