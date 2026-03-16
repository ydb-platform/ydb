/*
 * Copyright 2014-2023 NVIDIA Corporation.  All rights reserved.
 *
 * NOTICE TO LICENSEE:
 *
 * This source code and/or documentation ("Licensed Deliverables") are
 * subject to NVIDIA intellectual property rights under U.S. and
 * international Copyright laws.
 *
 * These Licensed Deliverables contained herein is PROPRIETARY and
 * CONFIDENTIAL to NVIDIA and is being provided under the terms and
 * conditions of a form of NVIDIA software license agreement by and
 * between NVIDIA and Licensee ("License Agreement") or electronically
 * accepted by Licensee.  Notwithstanding any terms or conditions to
 * the contrary in the License Agreement, reproduction or disclosure
 * of the Licensed Deliverables to any third party without the express
 * written consent of NVIDIA is prohibited.
 *
 * NOTWITHSTANDING ANY TERMS OR CONDITIONS TO THE CONTRARY IN THE
 * LICENSE AGREEMENT, NVIDIA MAKES NO REPRESENTATION ABOUT THE
 * SUITABILITY OF THESE LICENSED DELIVERABLES FOR ANY PURPOSE.  IT IS
 * PROVIDED "AS IS" WITHOUT EXPRESS OR IMPLIED WARRANTY OF ANY KIND.
 * NVIDIA DISCLAIMS ALL WARRANTIES WITH REGARD TO THESE LICENSED
 * DELIVERABLES, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY,
 * NONINFRINGEMENT, AND FITNESS FOR A PARTICULAR PURPOSE.
 * NOTWITHSTANDING ANY TERMS OR CONDITIONS TO THE CONTRARY IN THE
 * LICENSE AGREEMENT, IN NO EVENT SHALL NVIDIA BE LIABLE FOR ANY
 * SPECIAL, INDIRECT, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, OR ANY
 * DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
 * WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
 * ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE
 * OF THESE LICENSED DELIVERABLES.
 *
 * U.S. Government End Users.  These Licensed Deliverables are a
 * "commercial item" as that term is defined at 48 C.F.R. 2.101 (OCT
 * 1995), consisting of "commercial computer software" and "commercial
 * computer software documentation" as such terms are used in 48
 * C.F.R. 12.212 (SEPT 1995) and is provided to the U.S. Government
 * only as a commercial end item.  Consistent with 48 C.F.R.12.212 and
 * 48 C.F.R. 227.7202-1 through 227.7202-4 (JUNE 1995), all
 * U.S. Government End Users acquire the Licensed Deliverables with
 * only those rights set forth herein.
 *
 * Any use of the Licensed Deliverables in individual and commercial
 * software must include, in the user documentation and internal
 * comments to the code, the above Disclaimer and U.S. Government End
 * Users Notice.
 */

/**
 * \file: The master cuDNN version file.
 */

#ifndef CUDNN_VERSION_H_
#define CUDNN_VERSION_H_

#define CUDNN_MAJOR 8
#define CUDNN_MINOR 9
#define CUDNN_PATCHLEVEL 7

#define CUDNN_VERSION (CUDNN_MAJOR * 1000 + CUDNN_MINOR * 100 + CUDNN_PATCHLEVEL)

/* cannot use constexpr here since this is a C-only file */
/* Below is the max SM version this cuDNN library is aware of and supports natively */

#define CUDNN_MAX_SM_MAJOR_NUMBER 9
#define CUDNN_MAX_SM_MINOR_NUMBER 0
#define CUDNN_MAX_DEVICE_VERSION (CUDNN_MAX_SM_MAJOR_NUMBER * 100 + CUDNN_MAX_SM_MINOR_NUMBER * 10)

/* Here are constants for each of the SM Architectures we support to use in code where device version checks must be
 * made */

/* MAXWELL SM 50 52 53 */
#define CUDNN_SM_50 500
#define CUDNN_SM_52 520
#define CUDNN_SM_53 530

/* PASCAL SM 60 61 62 */
#define CUDNN_SM_60 600
#define CUDNN_SM_61 610
#define CUDNN_SM_62 620

/* VOLTA SM 70 72 */
#define CUDNN_SM_70 700
#define CUDNN_SM_72 720

/* TURING SM 75 */
#define CUDNN_SM_75 750

/* AMPERE SM 80 86 87 */
#define CUDNN_SM_80 800
#define CUDNN_SM_86 860
#define CUDNN_SM_87 870

/* ADA LOVELACE SM 89 */
#define CUDNN_SM_89 890

/* HOPPER SM 90 */
#define CUDNN_SM_90 900

/* END MARKER for last known version.
 * This can be replaced after support for 1000 is added
 */
#define CUDNN_SM_9X_END 999

/* This is the minimum version we support devices below this will return CUDNN_STATUS_ARCH_MISMATCH */
#define CUDNN_MIN_DEVICE_VERSION CUDNN_SM_50

#endif /* CUDNN_VERSION_H */
