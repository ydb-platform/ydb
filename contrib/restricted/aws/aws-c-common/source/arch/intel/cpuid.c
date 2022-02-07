/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*
 * MSVC wants us to use the non-portable _dupenv_s instead; since we need
 * to remain portable, tell MSVC to suppress this warning.
 */
#define _CRT_SECURE_NO_WARNINGS

#include <aws/common/cpuid.h>
#include <stdlib.h>

extern void aws_run_cpuid(uint32_t eax, uint32_t ecx, uint32_t *abcd);

typedef bool(has_feature_fn)(void);

static bool s_has_clmul(void) {
    uint32_t abcd[4];
    uint32_t clmul_mask = 0x00000002;
    aws_run_cpuid(1, 0, abcd);

    if ((abcd[2] & clmul_mask) != clmul_mask)
        return false;

    return true;
}

static bool s_has_sse41(void) {
    uint32_t abcd[4];
    uint32_t sse41_mask = 0x00080000;
    aws_run_cpuid(1, 0, abcd);

    if ((abcd[2] & sse41_mask) != sse41_mask)
        return false;

    return true;
}

static bool s_has_sse42(void) {
    uint32_t abcd[4];
    uint32_t sse42_mask = 0x00100000;
    aws_run_cpuid(1, 0, abcd);

    if ((abcd[2] & sse42_mask) != sse42_mask)
        return false;

    return true;
}

static bool s_has_avx2(void) {
    uint32_t abcd[4];
    uint32_t avx2_bmi12_mask = (1 << 5) | (1 << 3) | (1 << 8);
    /* CPUID.(EAX=01H, ECX=0H):ECX.FMA[bit 12]==1   &&
       CPUID.(EAX=01H, ECX=0H):ECX.MOVBE[bit 22]==1 &&
       CPUID.(EAX=01H, ECX=0H):ECX.OSXSAVE[bit 27]==1 */
    aws_run_cpuid(7, 0, abcd);

    if ((abcd[1] & avx2_bmi12_mask) != avx2_bmi12_mask)
        return false;

    return true;
}

has_feature_fn *s_check_cpu_feature[AWS_CPU_FEATURE_COUNT] = {
    [AWS_CPU_FEATURE_CLMUL] = s_has_clmul,
    [AWS_CPU_FEATURE_SSE_4_1] = s_has_sse41,
    [AWS_CPU_FEATURE_SSE_4_2] = s_has_sse42,
    [AWS_CPU_FEATURE_AVX2] = s_has_avx2,
};

bool aws_cpu_has_feature(enum aws_cpu_feature_name feature_name) {
    if (s_check_cpu_feature[feature_name])
        return s_check_cpu_feature[feature_name]();
    return false;
}

#define CPUID_AVAILABLE 0
#define CPUID_UNAVAILABLE 1
static int cpuid_state = 2;

bool aws_common_private_has_avx2(void) {
    if (AWS_LIKELY(cpuid_state == 0)) {
        return true;
    }
    if (AWS_LIKELY(cpuid_state == 1)) {
        return false;
    }

    /* Provide a hook for testing fallbacks and benchmarking */
    const char *env_avx2_enabled = getenv("AWS_COMMON_AVX2");
    if (env_avx2_enabled) {
        int is_enabled = atoi(env_avx2_enabled);
        cpuid_state = !is_enabled;
        return is_enabled;
    }

    bool available = aws_cpu_has_feature(AWS_CPU_FEATURE_AVX2);
    cpuid_state = available ? CPUID_AVAILABLE : CPUID_UNAVAILABLE;

    return available;
}
