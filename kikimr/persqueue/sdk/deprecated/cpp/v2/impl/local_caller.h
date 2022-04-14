#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include "interface_common.h"
#include "internals.h"
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iproducer.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>
#include "persqueue_p.h"

namespace NPersQueue {

template <class TImpl>
class TCancelCaller: public TImpl {
public:
    using TImpl::TImpl;

    std::shared_ptr<TCancelCaller> MakeSharedRef() {
        return std::static_pointer_cast<TCancelCaller>(this->shared_from_this());
    }

    void Cancel() override {
        auto guard = Guard(this->DestroyLock);
        if (!this->IsCanceling) {
            this->IsCanceling = true;
            AddCancelCaller();
        }
    }

protected:
    const TImpl* QueueTag() const { // we must have exactly the same thread tag as TImpl object
        return this;
    }

    static void MakeCanceledError(TError& error) {
        error.SetDescription(GetCancelReason());
        error.SetCode(NErrorCode::ERROR);
    }

    template <class TResponseProto>
    static void MakeCanceledError(TResponseProto& resp) {
        MakeCanceledError(*resp.MutableError());
    }

    template <class TResponse, class... TArgs>
    static void MakeCanceledError(NThreading::TPromise<TResponse>& promise, TArgs&&...) { // the most common case for calls
        decltype(promise.GetValue().Response) responseProto;
        MakeCanceledError(responseProto);
        promise.SetValue(TResponse(std::move(responseProto)));
    }

    template <class TResponse>
    static TResponse MakeCanceledError() {
        TResponse resp;
        MakeCanceledError(*resp.MutableError());
        return resp;
    }

private:
    void AddCancelCaller() {
        std::weak_ptr<TCancelCaller> selfWeak = MakeSharedRef();
        auto caller = [selfWeak]() mutable {
            auto selfShared = selfWeak.lock();
            if (selfShared) {
                selfShared->TImpl::Cancel();
            }
        };
        Y_VERIFY(this->PQLib->GetQueuePool().GetQueue(QueueTag()).AddFunc(caller));
    }
};

template <class TImpl, class TStartResponse>
class TLocalStartDeadImplCaller: public TCancelCaller<TImpl> {
protected:
    using TCancelCaller<TImpl>::TCancelCaller;

    NThreading::TFuture<TStartResponse> Start(TInstant deadline) noexcept override {
        IsDead(); // subscribe for impl future
        auto guard = Guard(this->DestroyLock);
        if (this->IsCanceling) {
            guard.Release();
            decltype(reinterpret_cast<TStartResponse*>(0)->Response) respProto;
            this->MakeCanceledError(respProto);
            return NThreading::MakeFuture(TStartResponse(std::move(respProto)));
        }
        if (StartPromise.Initialized()) {
            guard.Release();
            decltype(reinterpret_cast<TStartResponse*>(0)->Response) respProto;
            TError& error = *respProto.MutableError();
            error.SetDescription("Start was already called");
            error.SetCode(NErrorCode::BAD_REQUEST);
            return NThreading::MakeFuture(TStartResponse(std::move(respProto)));
        }
        StartPromise = NThreading::NewPromise<TStartResponse>();
        AddStartCaller(deadline);
        return StartPromise.GetFuture();
    }

    NThreading::TFuture<TError> IsDead() noexcept override {
        auto guard = Guard(this->DestroyLock);
        if (this->IsCanceling && !IsDeadCalled) {
            guard.Release();
            TError err;
            this->MakeCanceledError(err);
            return NThreading::MakeFuture(err);
        }
        if (!IsDeadCalled) {
            AddIsDeadCaller();
        }
        return DeadPromise.GetFuture();
    }

private:
    std::shared_ptr<TLocalStartDeadImplCaller> MakeSharedRef() {
        return std::static_pointer_cast<TLocalStartDeadImplCaller>(this->shared_from_this());
    }

    void AddIsDeadCaller() {
        IsDeadCalled = true;
        std::shared_ptr<TLocalStartDeadImplCaller> selfShared = MakeSharedRef();
        auto isDeadCaller = [selfShared]() mutable {
            selfShared->CallIsDead();
        };
        Y_VERIFY(this->PQLib->GetQueuePool().GetQueue(this->QueueTag()).AddFunc(isDeadCaller));
    }

    void CallIsDead() {
        auto deadPromise = DeadPromise;
        TImpl::IsDead().Subscribe([deadPromise](const auto& future) mutable {
            deadPromise.SetValue(future.GetValue());
        });
    }

    void AddStartCaller(TInstant deadline) {
        std::shared_ptr<TLocalStartDeadImplCaller> selfShared = MakeSharedRef();
        auto startCaller = [selfShared, deadline]() mutable {
            selfShared->CallStart(deadline);
        };
        Y_VERIFY(this->PQLib->GetQueuePool().GetQueue(this->QueueTag()).AddFunc(startCaller));
    }

    void CallStart(TInstant deadline) {
        auto startPromise = StartPromise;
        TImpl::Start(deadline).Subscribe([startPromise](const auto& future) mutable {
            startPromise.SetValue(future.GetValue());
        });
    }

private:
    NThreading::TPromise<TStartResponse> StartPromise;
    NThreading::TPromise<TError> DeadPromise = NThreading::NewPromise<TError>();
    bool IsDeadCalled = false;
};

#define LOCAL_CALLER(Func, Args, DeclTail, ...)                 \
    void Func Args DeclTail {                                   \
        auto guard = Guard(this->DestroyLock);                  \
        if (this->IsCanceling) {                                \
            guard.Release();                                    \
            this->MakeCanceledError(__VA_ARGS__);               \
            return;                                             \
        }                                                       \
        auto selfShared = MakeSharedRef();                      \
        auto caller = [selfShared, __VA_ARGS__]() mutable {     \
            selfShared->TImpl::Func(__VA_ARGS__);               \
        };                                                      \
        Y_VERIFY(this->PQLib->GetQueuePool().GetQueue(this->QueueTag()).AddFunc(caller));\
    }

// Assumes that there is function with promise interface
// first arg must be promise
#define LOCAL_CALLER_WITH_FUTURE(ReturnType, Func, Args, DeclTail, ...) \
    NThreading::TFuture<ReturnType> Func Args DeclTail {                \
        auto promise = NThreading::NewPromise<ReturnType>();            \
        Func(__VA_ARGS__);                                              \
        return promise.GetFuture();                                     \
    }

// Helper classes for producer/consumer/processor impls.
// Wrappers that delegate calls to its methods to proper thread pool local threads.
// The result is that all calls to impl are serialized and locks are no more needed.
//
// Implies:
// That impl class is std::enable_shared_from_this descendant.

template <class TImpl>
class TLocalProducerImplCaller: public TLocalStartDeadImplCaller<TImpl, TProducerCreateResponse> {
public:
    using TLocalStartDeadImplCaller<TImpl, TProducerCreateResponse>::TLocalStartDeadImplCaller;

    LOCAL_CALLER(Write,
                 (NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, TData data),
                 noexcept override,
                 promise, seqNo, data)

    LOCAL_CALLER(Write,
                 (NThreading::TPromise<TProducerCommitResponse>& promise, TData data),
                 noexcept override,
                 promise, data)

    LOCAL_CALLER_WITH_FUTURE(TProducerCommitResponse,
                             Write,
                             (TProducerSeqNo seqNo, TData data),
                             noexcept override,
                             promise, seqNo, data)

    LOCAL_CALLER_WITH_FUTURE(TProducerCommitResponse,
                             Write,
                             (TData data),
                             noexcept override,
                             promise, data)

protected:
    using TLocalStartDeadImplCaller<TImpl, TProducerCreateResponse>::MakeCanceledError;
    static void MakeCanceledError(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, TData data) {
        TWriteResponse responseProto;
        MakeCanceledError(responseProto);
        promise.SetValue(TProducerCommitResponse(seqNo, std::move(data), std::move(responseProto)));
    }

    static void MakeCanceledError(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) {
        MakeCanceledError(promise, TProducerSeqNo(0), std::move(data));
    }

private:
    std::shared_ptr<TLocalProducerImplCaller> MakeSharedRef() {
        return std::static_pointer_cast<TLocalProducerImplCaller>(this->shared_from_this());
    }
};

template <class TImpl>
class TLocalConsumerImplCaller: public TLocalStartDeadImplCaller<TImpl, TConsumerCreateResponse> {
public:
    using TLocalStartDeadImplCaller<TImpl, TConsumerCreateResponse>::TLocalStartDeadImplCaller;

    LOCAL_CALLER(GetNextMessage,
                 (NThreading::TPromise<TConsumerMessage>& promise),
                 noexcept override,
                 promise)

    LOCAL_CALLER_WITH_FUTURE(TConsumerMessage,
                             GetNextMessage,
                             (),
                             noexcept override,
                             promise)

    LOCAL_CALLER(Commit,
                 (const TVector<ui64>& cookies),
                 noexcept override,
                 cookies)

    LOCAL_CALLER(RequestPartitionStatus,
                 (const TString& topic, ui64 partition, ui64 generation),
                 noexcept override,
                 topic, partition, generation)

private:
    using TLocalStartDeadImplCaller<TImpl, TConsumerCreateResponse>::MakeCanceledError;
    static void MakeCanceledError(const TVector<ui64>& cookies) {
        Y_UNUSED(cookies);
        // Do nothing, because this method doesn't return future
    }

    static void MakeCanceledError(const TString&, ui64, ui64) {
        // Do nothing, because this method doesn't return future
    }


    std::shared_ptr<TLocalConsumerImplCaller> MakeSharedRef() {
        return std::static_pointer_cast<TLocalConsumerImplCaller>(this->shared_from_this());
    }
};

template <class TImpl>
class TLocalProcessorImplCaller: public TCancelCaller<TImpl> {
public:
    using TCancelCaller<TImpl>::TCancelCaller;

    LOCAL_CALLER(GetNextData,
                 (NThreading::TPromise<TOriginData>& promise),
                 noexcept override,
                 promise)

    LOCAL_CALLER_WITH_FUTURE(TOriginData,
                             GetNextData,
                             (),
                             noexcept override,
                             promise)

private:
    using TCancelCaller<TImpl>::MakeCanceledError;
    static void MakeCanceledError(NThreading::TPromise<TOriginData>& promise) {
        promise.SetValue(TOriginData()); // with empty data
    }

    std::shared_ptr<TLocalProcessorImplCaller> MakeSharedRef() {
        return std::static_pointer_cast<TLocalProcessorImplCaller>(this->shared_from_this());
    }
};

#undef LOCAL_CALLER
#undef LOCAL_CALLER_WITH_FUTURE

} // namespace NPersQueue
