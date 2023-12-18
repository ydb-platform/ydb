#pragma once

#include <util/generic/deque.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>
#include <util/system/condvar.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <utility>

namespace NThreading {
    ///
    /// TBlockingQueue is a queue of elements of limited or unlimited size.
    /// Queue provides Push and Pop operations that block if operation can't be executed
    /// (queue is empty or maximum size is reached).
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
    template <class TElement>
    class TBlockingQueue {
    public:
        ///
        /// Creates blocking queue with given maxSize
        /// if maxSize == 0 then queue is unlimited
        TBlockingQueue(size_t maxSize)
            : MaxSize(maxSize == 0 ? Max<size_t>() : maxSize)
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

            CanPushCV.BroadCast();

            return result;
        }

        TDeque<TElement> Drain(TDuration duration) {
            return Drain(TInstant::Now() + duration);
        }

        ///
        /// Blocks until queue has space for new elements or queue is stopped or deadline is reached.
        /// Returns false exception if queue is stopped and push failed or deadline is reached.
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
        /// Checks whether queue is stopped.
        bool IsStopped() const {
            TGuard<TMutex> g(Lock);
            return Stopped;
        }

    private:
        bool CanPush() const {
            return Queue.size() < MaxSize || Stopped;
        }

        bool CanPop() const {
            return !Queue.empty() || Stopped;
        }

        template <typename Ref>
        bool PushRef(Ref e, TInstant deadline) {
            TGuard<TMutex> g(Lock);
            const auto canPush = [this]() { return CanPush(); };
            if (!CanPushCV.WaitD(Lock, deadline, canPush)) {
                return false;
            }
            if (Stopped) {
                return false;
            }
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
        bool Stopped;
    };

}
