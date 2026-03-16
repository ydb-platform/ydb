/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "apr.h"
#include "apr_atomic.h"
#include "apr_thread_mutex.h"

APR_DECLARE(apr_uint64_t) apr_atomic_add64(volatile apr_uint64_t *mem, apr_uint64_t val)
{
#if (defined(_M_IA64) || defined(_M_AMD64))
    return InterlockedExchangeAdd64(mem, val);
#else
    return InterlockedExchangeAdd64((long *)mem, val);
#endif
}

/* Of course we want the 2's compliment of the unsigned value, val */
#ifdef _MSC_VER
#pragma warning(disable: 4146)
#endif

APR_DECLARE(void) apr_atomic_sub64(volatile apr_uint64_t *mem, apr_uint64_t val)
{
#if (defined(_M_IA64) || defined(_M_AMD64))
    InterlockedExchangeAdd64(mem, -val);
#else
    InterlockedExchangeAdd64((long *)mem, -val);
#endif
}

APR_DECLARE(apr_uint64_t) apr_atomic_inc64(volatile apr_uint64_t *mem)
{
    /* we return old value, win64 returns new value :( */
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    return InterlockedIncrement64(mem) - 1;
#else
    return InterlockedIncrement64((long *)mem) - 1;
#endif
}

APR_DECLARE(int) apr_atomic_dec64(volatile apr_uint64_t *mem)
{
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    return InterlockedDecrement64(mem);
#else
    return InterlockedDecrement64((long *)mem);
#endif
}

APR_DECLARE(void) apr_atomic_set64(volatile apr_uint64_t *mem, apr_uint64_t val)
{
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    InterlockedExchange64(mem, val);
#else
    InterlockedExchange64((long*)mem, val);
#endif
}

APR_DECLARE(apr_uint64_t) apr_atomic_read64(volatile apr_uint64_t *mem)
{
    return *mem;
}

APR_DECLARE(apr_uint64_t) apr_atomic_cas64(volatile apr_uint64_t *mem, apr_uint64_t with,
                                           apr_uint64_t cmp)
{
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    return InterlockedCompareExchange64(mem, with, cmp);
#else
    return InterlockedCompareExchange64((long*)mem, with, cmp);
#endif
}

APR_DECLARE(apr_uint64_t) apr_atomic_xchg64(volatile apr_uint64_t *mem, apr_uint64_t val)
{
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    return InterlockedExchange64(mem, val);
#else
    return InterlockedExchange64((long *)mem, val);
#endif
}
