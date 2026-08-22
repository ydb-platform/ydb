/**
 * Definitions from virtio spec version 1.0
 * http://docs.oasis-open.org/virtio/virtio/v1.0/virtio-v1.0.html.
 *
 * Type naming and style is preserved verbatim from virtio spec.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef uint8_t  u8;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
typedef uint16_t le16;
typedef uint32_t le32;
typedef uint64_t le64;
#else
#   error Implement me
#endif

#ifdef __cplusplus
}
#endif
