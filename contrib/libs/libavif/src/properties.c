// Copyright 2024 Brad Hards. All rights reserved.
// SPDX-License-Identifier: BSD-2-Clause

#include "avif/internal.h"

#include <string.h>

struct avifKnownProperty
{
    uint8_t fourcc[4];
};

static const struct avifKnownProperty knownProperties[] = {
    { { 'f', 't', 'y', 'p' } }, { { 'u', 'u', 'i', 'd' } }, { { 'm', 'e', 't', 'a' } }, { { 'h', 'd', 'l', 'r' } },
    { { 'p', 'i', 't', 'm' } }, { { 'd', 'i', 'n', 'f' } }, { { 'd', 'r', 'e', 'f' } }, { { 'i', 'd', 'a', 't' } },
    { { 'i', 'l', 'o', 'c' } }, { { 'i', 'i', 'n', 'f' } }, { { 'i', 'n', 'f', 'e' } }, { { 'i', 'p', 'r', 'p' } },
    { { 'i', 'p', 'c', 'o' } }, { { 'a', 'v', '1', 'C' } }, { { 'a', 'v', '2', 'C' } }, { { 'i', 's', 'p', 'e' } },
    { { 'p', 'i', 'x', 'i' } }, { { 'p', 'a', 's', 'p' } }, { { 'c', 'o', 'l', 'r' } }, { { 'a', 'u', 'x', 'C' } },
    { { 'c', 'l', 'a', 'p' } }, { { 'i', 'r', 'o', 't' } }, { { 'i', 'm', 'i', 'r' } }, { { 'c', 'l', 'l', 'i' } },
    { { 'c', 'c', 'l', 'v' } }, { { 'm', 'd', 'c', 'v' } }, { { 'a', 'm', 'v', 'e' } }, { { 'r', 'e', 'v', 'e' } },
    { { 'n', 'd', 'w', 't' } }, { { 'a', '1', 'o', 'p' } }, { { 'l', 's', 'e', 'l' } }, { { 'a', '1', 'l', 'x' } },
    { { 'c', 'm', 'i', 'n' } }, { { 'c', 'm', 'e', 'x' } }, { { 'i', 'p', 'm', 'a' } }, { { 'i', 'r', 'e', 'f' } },
    { { 'a', 'u', 'x', 'l' } }, { { 't', 'h', 'm', 'b' } }, { { 'd', 'i', 'm', 'g' } }, { { 'p', 'r', 'e', 'm' } },
    { { 'c', 'd', 's', 'c' } }, { { 'g', 'r', 'p', 'l' } }, { { 'a', 'l', 't', 'r' } }, { { 's', 't', 'e', 'r' } },
    { { 'm', 'd', 'a', 't' } },
};

static const size_t numKnownProperties = sizeof(knownProperties) / sizeof(knownProperties[0]);

static const size_t FOURCC_BYTES = 4;
static const size_t UUID_BYTES = 16;

static const uint8_t ISO_UUID_SUFFIX[12] = { 0x00, 0x01, 0x00, 0x10, 0x80, 0x00, 0x00, 0xAA, 0x00, 0x38, 0x9b, 0x71 };

avifBool avifIsKnownPropertyType(const uint8_t boxtype[4])
{
    for (size_t i = 0; i < numKnownProperties; i++) {
        if (memcmp(knownProperties[i].fourcc, boxtype, FOURCC_BYTES) == 0) {
            return AVIF_TRUE;
        }
    }
    return AVIF_FALSE;
}

avifBool avifIsValidUUID(const uint8_t uuid[16])
{
    // This check is to reject encoding a known property via the UUID mechanism
    // See ISO/IEC 14496-12 Section 4.2.3
    for (size_t i = 0; i < numKnownProperties; i++) {
        if ((memcmp(knownProperties[i].fourcc, uuid, FOURCC_BYTES) == 0) &&
            (memcmp(ISO_UUID_SUFFIX, uuid + FOURCC_BYTES, UUID_BYTES - FOURCC_BYTES) == 0)) {
            return AVIF_FALSE;
        }
    }
    // This check rejects UUIDs with unexpected variant field values, including Nil UUID and Max UUID.
    // See RFC 9562 Section 4.1
    uint8_t variant = uuid[8] >> 4;
    if ((variant < 0x08) || (variant > 0x0b)) {
        return AVIF_FALSE;
    }
    // This check rejects UUIDs with unexpected version field values.
    // See RFC 9562 Section 4.2
    uint8_t version = uuid[6] >> 4;
    if ((version < 1) || (version > 8)) {
        return AVIF_FALSE;
    }
    // The rest of a UUID is pretty much a bucket of bits, so assume its OK.
    return AVIF_TRUE;
}
