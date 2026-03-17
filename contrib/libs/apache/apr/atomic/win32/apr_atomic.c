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

#include "apr_arch_atomic.h"

APR_DECLARE(apr_status_t) apr_atomic_init(apr_pool_t *p)
{
#if defined (NEED_ATOMICS_GENERIC64)
    return apr__atomic_generic64_init(p);
#else
    return APR_SUCCESS;
#endif
}

APR_DECLARE(apr_uint32_t) apr_atomic_add32(volatile apr_uint32_t *mem, apr_uint32_t val)
{
#if (defined(_M_IA64) || defined(_M_AMD64))
    return InterlockedExchangeAdd(mem, val);
#else
    return InterlockedExchangeAdd((long *)mem, val);
#endif
}

/* Of course we want the 2's compliment of the unsigned value, val */
#ifdef _MSC_VER
#pragma warning(disable: 4146)
#endif

APR_DECLARE(void) apr_atomic_sub32(volatile apr_uint32_t *mem, apr_uint32_t val)
{
#if (defined(_M_IA64) || defined(_M_AMD64))
    InterlockedExchangeAdd(mem, -val);
#else
    InterlockedExchangeAdd((long *)mem, -val);
#endif
}

APR_DECLARE(apr_uint32_t) apr_atomic_inc32(volatile apr_uint32_t *mem)
{
    /* we return old value, win32 returns new value :( */
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    return InterlockedIncrement(mem) - 1;
#else
    return InterlockedIncrement((long *)mem) - 1;
#endif
}

APR_DECLARE(int) apr_atomic_dec32(volatile apr_uint32_t *mem)
{
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    return InterlockedDecrement(mem);
#else
    return InterlockedDecrement((long *)mem);
#endif
}

APR_DECLARE(void) apr_atomic_set32(volatile apr_uint32_t *mem, apr_uint32_t val)
{
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    InterlockedExchange(mem, val);
#else
    InterlockedExchange((long*)mem, val);
#endif
}

APR_DECLARE(apr_uint32_t) apr_atomic_read32(volatile apr_uint32_t *mem)
{
    return *mem;
}

APR_DECLARE(apr_uint32_t) apr_atomic_cas32(volatile apr_uint32_t *mem, apr_uint32_t with,
                                           apr_uint32_t cmp)
{
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    return InterlockedCompareExchange(mem, with, cmp);
#else
    return InterlockedCompareExchange((long*)mem, with, cmp);
#endif
}

APR_DECLARE(void *) apr_atomic_casptr(volatile void **mem, void *with, const void *cmp)
{
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    return InterlockedCompareExchangePointer((void* volatile*)mem, with, (void*)cmp);
#else
    return InterlockedCompareExchangePointer((void**)mem, with, (void*)cmp);
#endif
}

APR_DECLARE(apr_uint32_t) apr_atomic_xchg32(volatile apr_uint32_t *mem, apr_uint32_t val)
{
#if (defined(_M_IA64) || defined(_M_AMD64)) && !defined(RC_INVOKED)
    return InterlockedExchange(mem, val);
#else
    return InterlockedExchange((long *)mem, val);
#endif
}

APR_DECLARE(void*) apr_atomic_xchgptr(volatile void **mem, void *with)
{
    return InterlockedExchangePointer((void**)mem, with);
}
