/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 RehiveTech. All rights reserved.
 */

#ifndef _RTE_ATOMIC_ARM_H_
#define _RTE_ATOMIC_ARM_H_

#ifdef RTE_ARCH_64
#include <rte_atomic_64.h>
#else
#error #include <rte_atomic_32.h>
#endif

#endif /* _RTE_ATOMIC_ARM_H_ */
