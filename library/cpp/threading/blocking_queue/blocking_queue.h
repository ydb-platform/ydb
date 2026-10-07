#pragma once

#include <util/generic/deque.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>
#include <util/system/compiler.h>
#include <util/system/condvar.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <utility>

namespace NThreading {
    template <class TValue>
    struct TUniformSizeProvider {
        size_t operator()(const TValue&) const {
            return 1;
        }
    };

    ///
    /// TBlockingQueue is a queue of elements of limited or unlimited size.
    /// Queue provides Push and Pop operations that block if operation can't be executed
    /// (queue is empty or maximum size is reached).
    ///
    /// Size of each element is determined by TSizeProvider (default: 1 per element).
    /// Maximum size is compared against the sum of element sizes (TotalSize).
    /// Capacity policy: TotalSize + SizeProvider(e) must not exceed maxSize, except that
    /// an empty queue always accepts one element — even if SizeProvider(e) > maxSize —
    /// so progress is possible when nothing else fits.
    ///
    /// TSizeProvider must be thread-safe: Push / ElementSize may call it without holding
    /// the queue mutex.
    ///
    /// Queue can be stopped, in that case all blocked operation will return `Nothing` / false.
    ///
    /// All operations are thread safe.
    ///
    ///
    /// Example of usage:
    ///     TBlockingQueue<int> queue;
    ///
    ///     ...
    ///
    ///     // thread 1
    ///     queue.Push(42);
    ///     queue.Push(100500);
    ///
    ///     ...
    ///
    ///     // thread 2
    ///     while (TMaybe<int> number = queue.Pop()) {
    ///         ProcessNumber(number.GetRef());
    ///     }
    template <class TElement, class TSizeProvider = TUniformSizeProvider<TElement>>
    class TBlockingQueue {
    public:
        ///
        /// Creates blocking queue with given maxSize
        /// if maxSize == 0 then queue is unlimited
        TBlockingQueue(size_t maxSize, TSizeProvider sizeProvider = TSizeProvider())
            : MaxSize(maxSize == 0 ? Max<size_t>() : maxSize)
            , SizeProvider(sizeProvider)
            , TotalSize_(0)
            , Stopped(false)
        {
        }

        ///
        /// Blocks until queue has some elements or queue is stopped or deadline is reached.
        /// Returns `Nothing` if queue is stopped or deadline is reached.
        /// Returns element otherwise.
        TMaybe<TElement> Pop(TInstant deadline = TInstant::Max()) {
            TGuard<TMutex> g(Lock);

            const auto canPop = [this]() { return CanPop(); };
            if (!CanPopCV.WaitD(Lock, deadline, canPop)) {
                return Nothing();
            }

            if (Stopped && Queue.empty()) {
                return Nothing();
            }
            TotalSize_ -= SizeProvider(Queue.front());
            TElement e = std::move(Queue.front());
            Queue.pop_front();
            CanPushCV.Signal();
            return std::move(e);
        }

        TMaybe<TElement> Pop(TDuration duration) {
            return Pop(TInstant::Now() + duration);
        }

        ///
        /// Blocks until queue has some elements or queue is stopped or deadline is reached.
        /// Returns empty internal deque if queue is stopped or deadline is reached.
        /// Returns iternal deque element otherwise.
        TDeque<TElement> Drain(TInstant deadline = TInstant::Max()) {
            TGuard<TMutex> g(Lock);

            const auto canPop = [this]() { return CanPop(); };
            if (!CanPopCV.WaitD(Lock, deadline, canPop)) {
                return {};
            }

            TDeque<TElement> result;
            std::swap(result, Queue);
            TotalSize_ = 0;

            CanPushCV.BroadCast();

            return result;
        }

        TDeque<TElement> Drain(TDuration duration) {
            return Drain(TInstant::Now() + duration);
        }

        ///
        /// Blocks until queue has space for the element or queue is stopped or deadline is reached.
        /// Accepts if TotalSize + SizeProvider(e) <= maxSize, or if the queue is empty
        /// (always allow at least one element, even when SizeProvider(e) > maxSize).
        /// Returns false if queue is stopped and push failed or deadline is reached.
        /// Pushes element to queue and returns true otherwise.
        bool Push(const TElement& e, TInstant deadline = TInstant::Max()) {
            return PushRef(e, deadline);
        }

        bool Push(TElement&& e, TInstant deadline = TInstant::Max()) {
            return PushRef(std::move(e), deadline);
        }

        bool Push(const TElement& e, TDuration duration) {
            return Push(e, TInstant::Now() + duration);
        }

        bool Push(TElement&& e, TDuration duration) {
            return Push(std::move(e), TInstant::Now() + duration);
        }

        ///
        /// Stops the queue, all blocked operations will be aborted.
        void Stop() {
            TGuard<TMutex> g(Lock);
            Stopped = true;
            CanPopCV.BroadCast();
            CanPushCV.BroadCast();
        }

        ///
        /// Checks whether queue is empty.
        bool Empty() const {
            TGuard<TMutex> g(Lock);
            return Queue.empty();
        }

        ///
        /// Returns size of the queue.
        size_t Size() const {
            TGuard<TMutex> g(Lock);
            return Queue.size();
        }

        ///
        /// Returns total size of elements according to TSizeProvider.
        size_t TotalSize() const {
            TGuard<TMutex> g(Lock);
            return TotalSize_;
        }

        ///
        /// Checks whether queue is stopped.
        bool IsStopped() const {
            TGuard<TMutex> g(Lock);
            return Stopped;
        }

        ///
        /// Returns SizeProvider(e). Does not take the queue mutex.
        size_t ElementSize(const TElement& e) const {
            return SizeProvider(e);
        }

    private:
        bool CanPush(size_t elemSize) const {
            return Stopped || Queue.empty() || TotalSize_ + elemSize <= MaxSize;
        }

        bool CanPop() const {
            return !Queue.empty() || Stopped;
        }

        template <typename Ref>
        bool PushRef(Ref e, TInstant deadline) {
            const size_t elemSize = SizeProvider(e);
            TGuard<TMutex> g(Lock);
            const auto canPush = [this, elemSize]() { return CanPush(elemSize); };
            if (!CanPushCV.WaitD(Lock, deadline, canPush)) {
                return false;
            }
            if (Stopped) {
                return false;
            }
            TotalSize_ += elemSize;
            Queue.push_back(std::forward<TElement>(e));
            CanPopCV.Signal();
            return true;
        }

    private:
        TMutex Lock;
        TCondVar CanPopCV;
        TCondVar CanPushCV;
        TDeque<TElement> Queue;
        size_t MaxSize;
        Y_NO_UNIQUE_ADDRESS TSizeProvider SizeProvider;
        size_t TotalSize_; // Trailing underscore: public accessor is TotalSize()
        bool Stopped;
    };

}
