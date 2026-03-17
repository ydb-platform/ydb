/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com
 *****************************************************************************/

#include "threading.h"

#if defined(_WIN32) && (_WIN32_WINNT < 0x0600) // _WIN32_WINNT_VISTA

namespace X265_NS {
/* Mimic CONDITION_VARIABLE functions only supported on Vista+ */

int WINAPI cond_init(ConditionVariable *cond)
{ // InitializeConditionVariable
    cond->semaphore = CreateSemaphore(NULL, 0, 0x7fffffff, NULL);
    if (!cond->semaphore)
        return -1;
    cond->waitersDone = CreateEvent(NULL, FALSE, FALSE, NULL);
    if (!cond->waitersDone)
        return -1;

    InitializeCriticalSection(&cond->waiterCountMutex);
    InitializeCriticalSection(&cond->broadcastMutex);
    cond->waiterCount = 0;
    cond->bIsBroadcast = false;

    return 0;
}

void WINAPI cond_broadcast(ConditionVariable *cond)
{ // WakeAllConditionVariable
    EnterCriticalSection(&cond->broadcastMutex);
    EnterCriticalSection(&cond->waiterCountMutex);
    int haveWaiter = 0;

    if (cond->waiterCount)
    {
        cond->bIsBroadcast = 1;
        haveWaiter = 1;
    }

    if (haveWaiter)
    {
        ReleaseSemaphore(cond->semaphore, cond->waiterCount, NULL);
        LeaveCriticalSection(&cond->waiterCountMutex);
        WaitForSingleObject(cond->waitersDone, INFINITE);
        cond->bIsBroadcast = 0;
    }
    else
        LeaveCriticalSection(&cond->waiterCountMutex);

    LeaveCriticalSection(&cond->broadcastMutex);
}

void WINAPI cond_signal(ConditionVariable *cond)
{ // WakeConditionVariable
    EnterCriticalSection(&cond->broadcastMutex);
    EnterCriticalSection(&cond->waiterCountMutex);
    int haveWaiter = cond->waiterCount;
    LeaveCriticalSection(&cond->waiterCountMutex);

    if (haveWaiter)
    {
        ReleaseSemaphore(cond->semaphore, 1, NULL);
        WaitForSingleObject(cond->waitersDone, INFINITE);
    }

    LeaveCriticalSection(&cond->broadcastMutex);
}

BOOL WINAPI cond_wait(ConditionVariable *cond, CRITICAL_SECTION *mutex, DWORD wait)
{ // SleepConditionVariableCS
    EnterCriticalSection(&cond->broadcastMutex);
    EnterCriticalSection(&cond->waiterCountMutex);
    cond->waiterCount++;
    LeaveCriticalSection(&cond->waiterCountMutex);
    LeaveCriticalSection(&cond->broadcastMutex);

    // unlock the external mutex
    LeaveCriticalSection(mutex);
    BOOL ret = WaitForSingleObject(cond->semaphore, wait);

    EnterCriticalSection(&cond->waiterCountMutex);
    cond->waiterCount--;
    int last_waiter = !cond->waiterCount || !cond->bIsBroadcast;
    LeaveCriticalSection(&cond->waiterCountMutex);

    if (last_waiter)
        SetEvent(cond->waitersDone);

    // lock the external mutex
    EnterCriticalSection(mutex);

    // returns false on timeout or error
    return ret;
}

/* Native CONDITION_VARIABLE instances are not freed, so this is a special case */
void cond_destroy(ConditionVariable *cond)
{
    CloseHandle(cond->semaphore);
    CloseHandle(cond->waitersDone);
    DeleteCriticalSection(&cond->broadcastMutex);
    DeleteCriticalSection(&cond->waiterCountMutex);
}
} // namespace X265_NS

#elif defined(_MSC_VER)

namespace { int _avoid_linker_warnings = 0; }

#endif // _WIN32_WINNT <= _WIN32_WINNT_WINXP
