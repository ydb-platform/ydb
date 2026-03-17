//-----------------------------------------------------------------------------
// Copyright (c) 2017, 2022, Oracle and/or its affiliates.
//
// This software is dual-licensed to you under the Universal Permissive License
// (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
// 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
// either license.
//
// If you elect to accept the software under the Apache License, Version 2.0,
// the following applies:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// dpiHandlePool.c
//   Implementation of a pool of handles which can be acquired and released in
// a thread-safe manner. The pool is a circular queue where handles are
// acquired from the front and released to the back.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

//-----------------------------------------------------------------------------
// dpiHandlePool__acquire() [INTERNAL]
//   Acquire a handle from the pool. If a handle is available, it will be
// cleared out of the pool and returned to the caller. It is the caller's
// responsibility to return the handle back to the pool when it is finished
// with it. If no handle is available, a NULL value is returned. The caller is
// expected to create a new handle and return it to the pool when it is
// finished with it.
//-----------------------------------------------------------------------------
int dpiHandlePool__acquire(dpiHandlePool *pool, void **handle, dpiError *error)
{
    void **tempHandles;
    uint32_t numSlots;

    dpiMutex__acquire(pool->mutex);
    if (pool->acquirePos != pool->releasePos) {
        *handle = pool->handles[pool->acquirePos];
        pool->handles[pool->acquirePos++] = NULL;
        if (pool->acquirePos == pool->numSlots)
            pool->acquirePos = 0;
    } else {
        *handle = NULL;
        pool->numUsedSlots++;
        if (pool->numUsedSlots > pool->numSlots) {
            numSlots = pool->numSlots + 8;
            if (dpiUtils__allocateMemory(numSlots, sizeof(void*), 1,
                    "allocate slots", (void**) &tempHandles, error) < 0) {
                dpiMutex__release(pool->mutex);
                return DPI_FAILURE;
            }
            memcpy(tempHandles, pool->handles, pool->numSlots * sizeof(void*));
            dpiUtils__freeMemory(pool->handles);
            pool->handles = tempHandles;
            pool->numSlots = numSlots;
        }
    }
    dpiMutex__release(pool->mutex);

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiHandlePool__create() [INTERNAL]
//   Create a new handle pool.
//-----------------------------------------------------------------------------
int dpiHandlePool__create(dpiHandlePool **pool, dpiError *error)
{
    dpiHandlePool *tempPool;

    if (dpiUtils__allocateMemory(1, sizeof(dpiHandlePool), 0,
            "allocate handle pool", (void**) &tempPool, error) < 0)
        return DPI_FAILURE;
    tempPool->numSlots = 8;
    tempPool->numUsedSlots = 0;
    if (dpiUtils__allocateMemory(tempPool->numSlots, sizeof(void*), 1,
            "allocate handle pool slots", (void**) &tempPool->handles,
            error) < 0) {
        dpiUtils__freeMemory(tempPool);
        return DPI_FAILURE;
    }
    dpiMutex__initialize(tempPool->mutex);
    tempPool->acquirePos = 0;
    tempPool->releasePos = 0;
    *pool = tempPool;
    return DPI_SUCCESS;
}

//-----------------------------------------------------------------------------
// dpiHandlePool__free() [INTERNAL]
//   Free the memory associated with the error pool.
//-----------------------------------------------------------------------------
void dpiHandlePool__free(dpiHandlePool *pool)
{
    if (pool->handles) {
        dpiUtils__freeMemory(pool->handles);
        pool->handles = NULL;
    }
    dpiMutex__destroy(pool->mutex);
    dpiUtils__freeMemory(pool);
}


//-----------------------------------------------------------------------------
// dpiHandlePool__release() [INTERNAL]
//   Release a handle back to the pool. No checks are performed on the handle
// that is being returned to the pool; It will simply be placed back in the
// pool. The handle is then NULLed in order to avoid multiple attempts to
// release the handle back to the pool.
//-----------------------------------------------------------------------------
void dpiHandlePool__release(dpiHandlePool *pool, void **handle)
{
    dpiMutex__acquire(pool->mutex);
    pool->handles[pool->releasePos++] = *handle;
    *handle = NULL;
    if (pool->releasePos == pool->numSlots)
        pool->releasePos = 0;
    dpiMutex__release(pool->mutex);
}
