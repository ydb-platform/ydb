#pragma once

#include <util/generic/buffer.h>
#include <util/generic/deque.h>
#include <util/generic/noncopyable.h>
#include <util/generic/strbuf.h>
#include <util/system/tls.h>
#include <util/thread/singleton.h>

namespace NCodecs {
    template <class TItem>
    struct TClear {
        void operator()(TItem& item) const {
            item.Clear();
        }
    };

    template <class TItem, class TCleaner = TClear<TItem>>
    class TTlsCache {
        using TSelf = TTlsCache<TItem, TCleaner>;

        struct TItemHolder: public TIntrusiveListItem<TItemHolder> {
            TItemHolder(TSelf& factory)
                : Factory(factory)
            {
            }

            void Release() {
                Factory.Release(*this);
            }

            TSelf& Factory;
            TItem Item;
        };

        class TItemGuard {
        public:
            explicit TItemGuard(TSelf& fact)
                : Holder(fact.Acquire())
            {
            }

            TItemGuard(TItemGuard&& other) noexcept {
                *this = std::move(other);
            }

            TItemGuard& operator=(TItemGuard&& other) noexcept {
                if (&other != this) {
                    std::swap(Holder, other.Holder);
                }
                return *this;
            }

            ~TItemGuard() {
                if (Holder) {
                    Holder->Release();
                }
            }

            TItem& Get() & {
                Y_ASSERT(Holder);
                return Holder->Item;
            }

            TItem& Get() && = delete;

        private:
            TItemHolder* Holder = nullptr;
        };

    public:
        TItemGuard Item() {
            return TItemGuard(*this);
        }

        static TSelf& TlsInstance() {
            return *FastTlsSingleton<TSelf>();
        }

    private:
        TItemHolder* Acquire() {
            if (Free.Empty()) {
                return new TItemHolder(*this);
            } else {
                return Free.PopBack();
            }
        }

        void Release(TItemHolder& item) {
            Cleaner(item.Item);
            Free.PushBack(&item);
        }

    private:
        TIntrusiveListWithAutoDelete<TItemHolder, TDelete> Free;
        TCleaner Cleaner;
    };

    using TBufferTlsCache = TTlsCache<TBuffer>;
}
