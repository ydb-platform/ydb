#ifndef AWS_COMMON_PRIVATE_DLLOADS_H
#define AWS_COMMON_PRIVATE_DLLOADS_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*
 * definition is here: https://linux.die.net/man/2/set_mempolicy
 */
#define AWS_MPOL_PREFERRED_ALIAS 1

extern long (*g_set_mempolicy_ptr)(int, const unsigned long *, unsigned long);
extern void *g_libnuma_handle;

#endif /* AWS_COMMON_PRIVATE_DLLOADS_H */
