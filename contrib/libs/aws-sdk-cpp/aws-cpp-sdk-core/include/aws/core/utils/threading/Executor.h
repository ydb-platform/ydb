/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSQueue.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/threading/Semaphore.h>
#include <functional>
#include <future>
#include <mutex>
#include <atomic>

namespace Aws
{
    namespace Utils
    {
        namespace Threading
        {
            class ThreadTask;

            /**
            * Interface for implementing an Executor, to implement a custom thread execution strategy, inherit from this class
            * and override SubmitToThread().
            */
            class AWS_CORE_API Executor
            {
            public:                
                virtual ~Executor() = default;

                /**
                 * Send function and its arguments to the SubmitToThread function.
                 */
                template<class Fn, class ... Args>
                bool Submit(Fn&& fn, Args&& ... args)
                {
                    std::function<void()> callable{ std::bind(std::forward<Fn>(fn), std::forward<Args>(args)...) };
                    return SubmitToThread(std::move(callable));
                }

            protected:
                /**
                * To implement your own executor implementation, then simply subclass Executor and implement this method.
                */
                virtual bool SubmitToThread(std::function<void()>&&) = 0;
            };


            /**
            * Default Executor implementation. Simply spawns a thread and detaches it.
            */
            class AWS_CORE_API DefaultExecutor : public Executor
            {
            public:
                DefaultExecutor() : m_state(State::Free) {}
                ~DefaultExecutor();
            protected:
                enum class State
                {
                    Free, Locked, Shutdown
                };
                bool SubmitToThread(std::function<void()>&&) override;
                void Detach(std::thread::id id);
                std::atomic<State> m_state;
                Aws::UnorderedMap<std::thread::id, std::thread> m_threads;
            };

            enum class OverflowPolicy
            {
                QUEUE_TASKS_EVENLY_ACCROSS_THREADS,
                REJECT_IMMEDIATELY
            };

            /**
            * Thread Pool Executor implementation.
            */
            class AWS_CORE_API PooledThreadExecutor : public Executor
            {
            public:
                PooledThreadExecutor(size_t poolSize, OverflowPolicy overflowPolicy = OverflowPolicy::QUEUE_TASKS_EVENLY_ACCROSS_THREADS);
                ~PooledThreadExecutor();

                /**
                * Rule of 5 stuff.
                * Don't copy or move
                */
                PooledThreadExecutor(const PooledThreadExecutor&) = delete;
                PooledThreadExecutor& operator =(const PooledThreadExecutor&) = delete;
                PooledThreadExecutor(PooledThreadExecutor&&) = delete;
                PooledThreadExecutor& operator =(PooledThreadExecutor&&) = delete;

            protected:
                bool SubmitToThread(std::function<void()>&&) override;

            private:
                Aws::Queue<std::function<void()>*> m_tasks;
                std::mutex m_queueLock;
                Aws::Utils::Threading::Semaphore m_sync;
                Aws::Vector<ThreadTask*> m_threadTaskHandles;
                size_t m_poolSize;
                OverflowPolicy m_overflowPolicy;

                /**
                 * Once you call this, you are responsible for freeing the memory pointed to by task.
                 */
                std::function<void()>* PopTask();
                bool HasTasks();

                friend class ThreadTask;
            };


        } // namespace Threading
    } // namespace Utils
} // namespace Aws
