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

#include "nvme_internal.h"

struct nvme_quirk {
	struct spdk_pci_id	id;
	uint64_t		flags;
};

static const struct nvme_quirk nvme_quirks[] = {
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_INTEL, 0x0953, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_INTEL_QUIRK_READ_LATENCY |
		NVME_INTEL_QUIRK_WRITE_LATENCY |
		NVME_INTEL_QUIRK_STRIPING |
		NVME_QUIRK_READ_ZERO_AFTER_DEALLOCATE |
		NVME_QUIRK_DELAY_BEFORE_INIT |
		NVME_QUIRK_MINIMUM_IO_QUEUE_SIZE
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_INTEL, 0x0A53, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_INTEL_QUIRK_READ_LATENCY |
		NVME_INTEL_QUIRK_WRITE_LATENCY |
		NVME_INTEL_QUIRK_STRIPING |
		NVME_QUIRK_READ_ZERO_AFTER_DEALLOCATE |
		NVME_QUIRK_DELAY_BEFORE_INIT |
		NVME_QUIRK_MINIMUM_IO_QUEUE_SIZE
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_INTEL, 0x0A54, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_INTEL_QUIRK_READ_LATENCY |
		NVME_INTEL_QUIRK_WRITE_LATENCY |
		NVME_INTEL_QUIRK_STRIPING |
		NVME_QUIRK_READ_ZERO_AFTER_DEALLOCATE |
		NVME_QUIRK_MINIMUM_IO_QUEUE_SIZE
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_INTEL, 0x0A55, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_INTEL_QUIRK_READ_LATENCY |
		NVME_INTEL_QUIRK_WRITE_LATENCY |
		NVME_INTEL_QUIRK_STRIPING |
		NVME_QUIRK_READ_ZERO_AFTER_DEALLOCATE |
		NVME_QUIRK_MINIMUM_IO_QUEUE_SIZE
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_INTEL, 0x0B60, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_INTEL_QUIRK_READ_LATENCY |
		NVME_INTEL_QUIRK_WRITE_LATENCY |
		NVME_INTEL_QUIRK_STRIPING |
		NVME_QUIRK_MINIMUM_IO_QUEUE_SIZE |
		NVME_QUIRK_NO_SGL_FOR_DSM
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_MEMBLAZE, 0x0540, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_QUIRK_DELAY_BEFORE_CHK_RDY
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_SAMSUNG, 0xa821, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_QUIRK_DELAY_BEFORE_CHK_RDY
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_SAMSUNG, 0xa822, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_QUIRK_DELAY_BEFORE_CHK_RDY
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_VIRTUALBOX, 0x4e56, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_QUIRK_DELAY_AFTER_QUEUE_ALLOC
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_INTEL, 0x5845, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_INTEL_QUIRK_NO_LOG_PAGES |
		NVME_QUIRK_MAXIMUM_PCI_ACCESS_WIDTH
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_CNEXLABS, 0x1f1f, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_QUIRK_IDENTIFY_CNS |
		NVME_QUIRK_OCSSD
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_VMWARE, 0x07f0, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_QUIRK_SHST_COMPLETE
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_INTEL, 0x2700, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_QUIRK_OACS_SECURITY
	},
	{	{SPDK_PCI_CLASS_NVME, SPDK_PCI_VID_INTEL, 0x4140, SPDK_PCI_ANY_ID, SPDK_PCI_ANY_ID},
		NVME_QUIRK_MDTS_EXCLUDE_MD
	},
	{	{0x000000, 0x0000, 0x0000, 0x0000, 0x0000}, 0}
};

/* Compare each field. SPDK_PCI_ANY_ID in s1 matches everything */
static bool
pci_id_match(const struct spdk_pci_id *s1, const struct spdk_pci_id *s2)
{
	if ((s1->class_id == SPDK_PCI_CLASS_ANY_ID || s1->class_id == s2->class_id) &&
	    (s1->vendor_id == SPDK_PCI_ANY_ID || s1->vendor_id == s2->vendor_id) &&
	    (s1->device_id == SPDK_PCI_ANY_ID || s1->device_id == s2->device_id) &&
	    (s1->subvendor_id == SPDK_PCI_ANY_ID || s1->subvendor_id == s2->subvendor_id) &&
	    (s1->subdevice_id == SPDK_PCI_ANY_ID || s1->subdevice_id == s2->subdevice_id)) {
		return true;
	}
	return false;
}

uint64_t
nvme_get_quirks(const struct spdk_pci_id *id)
{
	const struct nvme_quirk *quirk = nvme_quirks;

	SPDK_DEBUGLOG(nvme, "Searching for %04x:%04x [%04x:%04x]...\n",
		      id->vendor_id, id->device_id,
		      id->subvendor_id, id->subdevice_id);

	while (quirk->id.vendor_id) {
		if (pci_id_match(&quirk->id, id)) {
			SPDK_DEBUGLOG(nvme, "Matched quirk %04x:%04x [%04x:%04x]:\n",
				      quirk->id.vendor_id, quirk->id.device_id,
				      quirk->id.subvendor_id, quirk->id.subdevice_id);

#define PRINT_QUIRK(quirk_flag) \
			do { \
				if (quirk->flags & (quirk_flag)) { \
					SPDK_DEBUGLOG(nvme, "Quirk enabled: %s\n", #quirk_flag); \
				} \
			} while (0)

			PRINT_QUIRK(NVME_INTEL_QUIRK_READ_LATENCY);
			PRINT_QUIRK(NVME_INTEL_QUIRK_WRITE_LATENCY);
			PRINT_QUIRK(NVME_QUIRK_DELAY_BEFORE_CHK_RDY);
			PRINT_QUIRK(NVME_INTEL_QUIRK_STRIPING);
			PRINT_QUIRK(NVME_QUIRK_DELAY_AFTER_QUEUE_ALLOC);
			PRINT_QUIRK(NVME_QUIRK_READ_ZERO_AFTER_DEALLOCATE);
			PRINT_QUIRK(NVME_QUIRK_IDENTIFY_CNS);
			PRINT_QUIRK(NVME_QUIRK_OCSSD);

			return quirk->flags;
		}
		quirk++;
	}

	SPDK_DEBUGLOG(nvme, "No quirks found.\n");

	return 0;
}
