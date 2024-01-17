/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/threading/ThreadTask.h>
#include <thread>
#include <cassert>

static const char* POOLED_CLASS_TAG = "PooledThreadExecutor";

using namespace Aws::Utils::Threading;

bool DefaultExecutor::SubmitToThread(std::function<void()>&&  fx)
{
    // Generalized lambda capture is C++14, using std::bind as a workaround to force moving fx (instead of copying)
    std::function<void()> main = std::bind(
            [this](std::function<void()>& storedFx)
            {
                storedFx();
                Detach(std::this_thread::get_id());
            },
            std::move(fx)
        );

    State expected;
    do
    {
        expected = State::Free;
        if(m_state.compare_exchange_strong(expected, State::Locked))
        {
            std::thread t(std::move(main));
            const auto id = t.get_id(); // copy the id before we std::move the thread
            m_threads.emplace(id, std::move(t));
            m_state = State::Free;
            return true;
        }
    }
    while(expected != State::Shutdown);
    return false;
}

void DefaultExecutor::Detach(std::thread::id id)
{
    State expected;
    do
    {
        expected = State::Free;
        if(m_state.compare_exchange_strong(expected, State::Locked))
        {
            auto it = m_threads.find(id);
            assert(it != m_threads.end());
            it->second.detach();
            m_threads.erase(it);
            m_state = State::Free;
            return;
        }
    } 
    while(expected != State::Shutdown);
}

DefaultExecutor::~DefaultExecutor()
{
    auto expected = State::Free;
    while(!m_state.compare_exchange_strong(expected, State::Shutdown))
    {
        //spin while currently detaching threads finish
        assert(expected == State::Locked);
        expected = State::Free; 
    }

    auto it = m_threads.begin();
    while(!m_threads.empty())
    {
        it->second.join();
        it = m_threads.erase(it);
    }
}

PooledThreadExecutor::PooledThreadExecutor(size_t poolSize, OverflowPolicy overflowPolicy) :
    m_sync(0, poolSize), m_poolSize(poolSize), m_overflowPolicy(overflowPolicy)
{
    for (size_t index = 0; index < m_poolSize; ++index)
    {
        m_threadTaskHandles.push_back(Aws::New<ThreadTask>(POOLED_CLASS_TAG, *this));
    }
}

PooledThreadExecutor::~PooledThreadExecutor()
{
    for(auto threadTask : m_threadTaskHandles)
    {
        threadTask->StopProcessingWork();
    }

    m_sync.ReleaseAll();

    for (auto threadTask : m_threadTaskHandles)
    {
        Aws::Delete(threadTask);
    }

    while(m_tasks.size() > 0)
    {
        std::function<void()>* fn = m_tasks.front();
        m_tasks.pop();

        if(fn)
        {
            Aws::Delete(fn);
        }
    }

}

bool PooledThreadExecutor::SubmitToThread(std::function<void()>&& fn)
{
    //avoid the need to do copies inside the lock. Instead lets do a pointer push.
    std::function<void()>* fnCpy = Aws::New<std::function<void()>>(POOLED_CLASS_TAG, std::forward<std::function<void()>>(fn));

    {
        std::lock_guard<std::mutex> locker(m_queueLock);

        if (m_overflowPolicy == OverflowPolicy::REJECT_IMMEDIATELY && m_tasks.size() >= m_poolSize)
        {
            Aws::Delete(fnCpy);
            return false;
        }

        m_tasks.push(fnCpy);
    }

    m_sync.Release();

    return true;
}

std::function<void()>* PooledThreadExecutor::PopTask()
{
    std::lock_guard<std::mutex> locker(m_queueLock);

    if (m_tasks.size() > 0)
    {
        std::function<void()>* fn = m_tasks.front();
        if (fn)
        {           
            m_tasks.pop();
            return fn;
        }
    }

    return nullptr;
}

bool PooledThreadExecutor::HasTasks()
{
    std::lock_guard<std::mutex> locker(m_queueLock);
    return m_tasks.size() > 0;
}
