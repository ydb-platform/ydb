#pragma once

#include "scheme.h"

#include <util/thread/singleton.h>

namespace NSc {
    namespace NImpl {
        template <typename TContext>
        static inline TContext& GetTlsInstance() {
            return *FastTlsSingleton<TContext>();
        }

        template <typename TContext>
        class TContextGuard : TNonCopyable {
            using TElement = typename TContext::TElement;
            using TTarget = typename TContext::TTarget;
            using TVectorType = TVector<TElement>;

        public:
            TContextGuard(TContext& ctx, TTarget& target)
                : Ctx(ctx)
                , Active(TContext::Needed(target))
            {
                if (Active) {
                    Begin = Ctx.Vector.size();
                    Ok = Ctx.Process(target);
                    End = Ctx.Vector.size();
                }
            }

            ~TContextGuard() noexcept {
                if (Active) {
                    Ctx.Vector.resize(Begin);
                }
            }

            const TVectorType& GetVector() const {
                return Ctx.Vector;
            }

            using const_iterator = size_t;

            size_t begin() const {
                return Begin;
            }

            size_t end() const {
                return End;
            }

            bool Ok = true;

        private:
            TContext& Ctx;
            size_t Begin = 0;
            size_t End = 0;
            bool Active = false;
        };

        template <typename TElem, typename TTgt>
        class TBasicContext {
        public:
            using TElement = TElem;
            using TTarget = TTgt;

            TBasicContext() {
                Vector.reserve(64);
            }

            TVector<TElement> Vector;
        };

        class TKeySortContext: public TBasicContext<TStringBuf, const TDict> {
        public:
            using TGuard = TContextGuard<TKeySortContext>;

            bool Process(const TDict& self);

            static bool Needed(const TDict& self) {
                return self.size();
            }
        };

        class TSelfOverrideContext: public TBasicContext<TValue, TValue::TScCore> {
        public:
            using TGuard = TContextGuard<TSelfOverrideContext>;

            bool Process(TValue::TScCore& self);

            static bool Needed(const TValue::TScCore& self) {
                return self.HasChildren();
            }
        };

        class TSelfLoopContext: public TBasicContext<const void*, const TValue::TScCore> {
        public:
            enum class EMode {
                Assert, Throw, Abort, Stderr
            };

            using TGuard = TContextGuard<TSelfLoopContext>;

            bool Process(const TValue::TScCore& self);

            static bool Needed(const TValue::TScCore& self) {
                return self.HasChildren();
            }

        public:
            EMode ReportingMode = EMode::Assert;
        };
    }
}
