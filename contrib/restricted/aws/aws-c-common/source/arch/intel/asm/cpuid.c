/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/cpuid.h>

void aws_run_cpuid(uint32_t eax, uint32_t ecx, uint32_t *abcd) {
    uint32_t ebx = 0;
    uint32_t edx = 0;

#if defined(__i386__) && defined(__PIC__)
    /* in case of PIC under 32-bit EBX cannot be clobbered */
    __asm__ __volatile__("movl %%ebx, %%edi \n\t "
                         "cpuid \n\t "
                         "xchgl %%ebx, %%edi"
                         : "=D"(ebx),
#else
    __asm__ __volatile__("cpuid"
                         : "+b"(ebx),
#endif
                           "+a"(eax),
                           "+c"(ecx),
                           "=d"(edx));
    abcd[0] = eax;
    abcd[1] = ebx;
    abcd[2] = ecx;
    abcd[3] = edx;
}

uint64_t aws_run_xgetbv(uint32_t xcr) {
    /* NOTE: we could have used the _xgetbv() intrinsic in <immintrin.h>, but it's missing from GCC < 9.0:
     * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=71659 */

    /* xgetbv writes high and low of 64bit value to EDX:EAX */
    uint32_t xcrhigh;
    uint32_t xcrlow;
    __asm__ __volatile__("xgetbv" : "=a"(xcrlow), "=d"(xcrhigh) : "c"(xcr));
    return (((uint64_t)xcrhigh) << 32) | xcrlow;
}
