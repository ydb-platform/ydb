#pragma once

#include "fwd.h"

#include <util/generic/ptr.h>

namespace NYql::NPureCalc::NPrivate {
    template <typename TNew, typename TOld, typename TFunctor>
    class TMappingStream final: public IStream<TNew> {
    private:
        THolder<IStream<TOld>> Old_;
        TFunctor Functor_;

    public:
        TMappingStream(THolder<IStream<TOld>> old, TFunctor functor)
            : Old_(std::move(old))
            , Functor_(std::move(functor))
        {
        }

    public:
        TNew Fetch() override {
            return Functor_(Old_->Fetch());
        }
    };

    template <typename TNew, typename TOld, typename TFunctor>
    class TMappingConsumer final: public IConsumer<TNew> {
    private:
        THolder<IConsumer<TOld>> Old_;
        TFunctor Functor_;

    public:
        TMappingConsumer(THolder<IConsumer<TOld>> old, TFunctor functor)
            : Old_(std::move(old))
            , Functor_(std::move(functor))
    {
    }

    public:
        void OnObject(TNew object) override {
            Old_->OnObject(Functor_(object));
        }

        void OnFinish() override {
            Old_->OnFinish();
        }
    };

    template <typename T, typename C>
    class TNonOwningConsumer final: public IConsumer<T> {
    private:
        C Consumer;

    public:
        explicit TNonOwningConsumer(const C& consumer)
            : Consumer(consumer)
        {
        }

    public:
        void OnObject(T t) override {
            Consumer->OnObject(t);
        }

        void OnFinish() override {
            Consumer->OnFinish();
        }
    };
}
