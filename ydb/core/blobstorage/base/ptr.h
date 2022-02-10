#pragma once

#include "defs.h"
#include <ydb/core/base/appdata.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TRefCountWithDeleter
    // A modified version of TRefCount from util/generic/ptr.h, because
    // original version doesn't support TDeleter with state
    ////////////////////////////////////////////////////////////////////////////
    template <class T, class TCounter, class TDeleter>
    class TRefCountWithDeleter {
    public:
        inline TRefCountWithDeleter(long initval = 0) noexcept
            : Counter_(initval)
            , Deleter_()
        {
        }

        inline TRefCountWithDeleter(const TDeleter &deleter) noexcept
            : Counter_(0)
            , Deleter_(deleter)
        {
        }

        inline ~TRefCountWithDeleter() {
        }

        inline void Ref(TAtomicBase d) noexcept {
            Counter_.Add(d);
        }

        inline void Ref() noexcept {
            Counter_.Inc();
        }

        inline void UnRef(TAtomicBase d) noexcept {
            TAtomicBase resultCount = Counter_.Sub(d);
            Y_ASSERT(resultCount >= 0);
            if (resultCount == 0) {
                Deleter_.Destroy(std::unique_ptr<T>(static_cast<T*>(this)));
            }
        }

        inline void UnRef() noexcept {
            UnRef(1);
        }

        inline long RefCount() const noexcept {
            return Counter_.Val();
        }

        inline void DecRef() noexcept {
            Counter_.Dec();
        }

        TRefCountWithDeleter(const TRefCountWithDeleter& r)
            : Counter_(0)
            , Deleter_(r.Deleter_)
        {
        }

        void operator=(const TRefCountWithDeleter& r) {
            Deleter_ = r.Deleter_;
        }

    private:
        TCounter Counter_;
        TDeleter Deleter_;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TAtomicRefCountWithDeleter
    ////////////////////////////////////////////////////////////////////////////
    template <class T, class D>
    using TAtomicRefCountWithDeleter = TRefCountWithDeleter<T, TAtomicCounter, D>;


    ////////////////////////////////////////////////////////////////////////////
    // TObjDeleterActor
    ////////////////////////////////////////////////////////////////////////////
    template <class T>
    class TObjDeleterActor : public TActorBootstrapped<TObjDeleterActor<T>> {
        using TThis = ::NKikimr::TObjDeleterActor<T>;
        friend class TActorBootstrapped<TThis>;

        void Bootstrap(const TActorContext &ctx) {
            Obj.reset();
            TThis::Die(ctx);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        TObjDeleterActor(std::unique_ptr<T> obj)
            : TActorBootstrapped<TThis>()
            , Obj(std::move(obj))
        {}

    private:
        std::unique_ptr<T> Obj;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TDeleteInBatchPool
    ////////////////////////////////////////////////////////////////////////////
    class TDeleteInBatchPool {
    public:
        TDeleteInBatchPool(TActorSystem *actorSystem)
            : ActorSystem(actorSystem)
        {}

        template <class T>
        inline void Destroy(std::unique_ptr<T> t) noexcept {
            AssertTypeComplete<T>();

            if (ActorSystem) {
                ui32 poolId = ActorSystem->AppData<::NKikimr::TAppData>()->BatchPoolId;
                ActorSystem->Register(new TObjDeleterActor<T>(std::move(t)), TMailboxType::HTSwap, poolId);
            }
        }

    private:
        TActorSystem *ActorSystem;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TAtomicRefCountWithDeleter
    ////////////////////////////////////////////////////////////////////////////
    template <class T>
    using TAtomicRefCountWithDeleterInBatchPool = TRefCountWithDeleter<T, TAtomicCounter, TDeleteInBatchPool>;

} // NKikimr
