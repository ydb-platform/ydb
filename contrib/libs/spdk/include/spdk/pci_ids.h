/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/** \file
 * PCI device ID list
 */

#ifndef SPDK_PCI_IDS
#define SPDK_PCI_IDS

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPDK_PCI_ANY_ID			0xffff
#define SPDK_PCI_VID_INTEL		0x8086
#define SPDK_PCI_VID_MEMBLAZE		0x1c5f
#define SPDK_PCI_VID_SAMSUNG		0x144d
#define SPDK_PCI_VID_VIRTUALBOX		0x80ee
#define SPDK_PCI_VID_VIRTIO		0x1af4
#define SPDK_PCI_VID_CNEXLABS		0x1d1d
#define SPDK_PCI_VID_VMWARE		0x15ad

#define SPDK_PCI_CLASS_ANY_ID		0xffffff
/**
 * PCI class code for NVMe devices.
 *
 * Base class code 01h: mass storage
 * Subclass code 08h: non-volatile memory
 * Programming interface 02h: NVM Express
 */
#define SPDK_PCI_CLASS_NVME		0x010802

#define PCI_DEVICE_ID_INTEL_IDXD	0x0b25

#define PCI_DEVICE_ID_INTEL_IOAT_SNB0	0x3c20
#define PCI_DEVICE_ID_INTEL_IOAT_SNB1	0x3c21
#define PCI_DEVICE_ID_INTEL_IOAT_SNB2	0x3c22
#define PCI_DEVICE_ID_INTEL_IOAT_SNB3	0x3c23
#define PCI_DEVICE_ID_INTEL_IOAT_SNB4	0x3c24
#define PCI_DEVICE_ID_INTEL_IOAT_SNB5	0x3c25
#define PCI_DEVICE_ID_INTEL_IOAT_SNB6	0x3c26
#define PCI_DEVICE_ID_INTEL_IOAT_SNB7	0x3c27
#define PCI_DEVICE_ID_INTEL_IOAT_SNB8	0x3c2e
#define PCI_DEVICE_ID_INTEL_IOAT_SNB9	0x3c2f

#define PCI_DEVICE_ID_INTEL_IOAT_IVB0	0x0e20
#define PCI_DEVICE_ID_INTEL_IOAT_IVB1	0x0e21
#define PCI_DEVICE_ID_INTEL_IOAT_IVB2	0x0e22
#define PCI_DEVICE_ID_INTEL_IOAT_IVB3	0x0e23
#define PCI_DEVICE_ID_INTEL_IOAT_IVB4	0x0e24
#define PCI_DEVICE_ID_INTEL_IOAT_IVB5	0x0e25
#define PCI_DEVICE_ID_INTEL_IOAT_IVB6	0x0e26
#define PCI_DEVICE_ID_INTEL_IOAT_IVB7	0x0e27
#define PCI_DEVICE_ID_INTEL_IOAT_IVB8	0x0e2e
#define PCI_DEVICE_ID_INTEL_IOAT_IVB9	0x0e2f

#define PCI_DEVICE_ID_INTEL_IOAT_HSW0	0x2f20
#define PCI_DEVICE_ID_INTEL_IOAT_HSW1	0x2f21
#define PCI_DEVICE_ID_INTEL_IOAT_HSW2	0x2f22
#define PCI_DEVICE_ID_INTEL_IOAT_HSW3	0x2f23
#define PCI_DEVICE_ID_INTEL_IOAT_HSW4	0x2f24
#define PCI_DEVICE_ID_INTEL_IOAT_HSW5	0x2f25
#define PCI_DEVICE_ID_INTEL_IOAT_HSW6	0x2f26
#define PCI_DEVICE_ID_INTEL_IOAT_HSW7	0x2f27
#define PCI_DEVICE_ID_INTEL_IOAT_HSW8	0x2f2e
#define PCI_DEVICE_ID_INTEL_IOAT_HSW9	0x2f2f

#define PCI_DEVICE_ID_INTEL_IOAT_BWD0	0x0C50
#define PCI_DEVICE_ID_INTEL_IOAT_BWD1	0x0C51
#define PCI_DEVICE_ID_INTEL_IOAT_BWD2	0x0C52
#define PCI_DEVICE_ID_INTEL_IOAT_BWD3	0x0C53

#define PCI_DEVICE_ID_INTEL_IOAT_BDXDE0	0x6f50
#define PCI_DEVICE_ID_INTEL_IOAT_BDXDE1	0x6f51
#define PCI_DEVICE_ID_INTEL_IOAT_BDXDE2	0x6f52
#define PCI_DEVICE_ID_INTEL_IOAT_BDXDE3	0x6f53

#define PCI_DEVICE_ID_INTEL_IOAT_BDX0	0x6f20
#define PCI_DEVICE_ID_INTEL_IOAT_BDX1	0x6f21
#define PCI_DEVICE_ID_INTEL_IOAT_BDX2	0x6f22
#define PCI_DEVICE_ID_INTEL_IOAT_BDX3	0x6f23
#define PCI_DEVICE_ID_INTEL_IOAT_BDX4	0x6f24
#define PCI_DEVICE_ID_INTEL_IOAT_BDX5	0x6f25
#define PCI_DEVICE_ID_INTEL_IOAT_BDX6	0x6f26
#define PCI_DEVICE_ID_INTEL_IOAT_BDX7	0x6f27
#define PCI_DEVICE_ID_INTEL_IOAT_BDX8	0x6f2e
#define PCI_DEVICE_ID_INTEL_IOAT_BDX9	0x6f2f

#define PCI_DEVICE_ID_INTEL_IOAT_SKX	0x2021

#define PCI_DEVICE_ID_INTEL_IOAT_ICX	0x0b00

#define PCI_DEVICE_ID_VIRTIO_BLK_LEGACY	0x1001
#define PCI_DEVICE_ID_VIRTIO_SCSI_LEGACY 0x1004
#define PCI_DEVICE_ID_VIRTIO_BLK_MODERN	0x1042
#define PCI_DEVICE_ID_VIRTIO_SCSI_MODERN 0x1048

#define PCI_DEVICE_ID_VIRTIO_VHOST_USER 0x1017

#define PCI_DEVICE_ID_INTEL_VMD		0x201d

#ifdef __cplusplus
}
#endif

#endif /* SPDK_PCI_IDS */
