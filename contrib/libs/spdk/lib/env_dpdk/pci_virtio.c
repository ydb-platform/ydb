#include <contrib/libs/spdk/ndebug.h>
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

#include "env_internal.h"

#include "spdk/pci_ids.h"

static struct spdk_pci_id virtio_pci_driver_id[] = {
	{ SPDK_PCI_DEVICE(SPDK_PCI_VID_VIRTIO, PCI_DEVICE_ID_VIRTIO_SCSI_MODERN) },
	{ SPDK_PCI_DEVICE(SPDK_PCI_VID_VIRTIO, PCI_DEVICE_ID_VIRTIO_BLK_MODERN) },
	{ SPDK_PCI_DEVICE(SPDK_PCI_VID_VIRTIO, PCI_DEVICE_ID_VIRTIO_SCSI_LEGACY) },
	{ SPDK_PCI_DEVICE(SPDK_PCI_VID_VIRTIO, PCI_DEVICE_ID_VIRTIO_BLK_LEGACY) },
	{ .vendor_id = 0, /* sentinel */ },
};

struct spdk_pci_driver *
spdk_pci_virtio_get_driver(void)
{
	return spdk_pci_get_driver("virtio");
}

SPDK_PCI_DRIVER_REGISTER(virtio, virtio_pci_driver_id,
			 SPDK_PCI_DRIVER_NEED_MAPPING | SPDK_PCI_DRIVER_WC_ACTIVATE);
