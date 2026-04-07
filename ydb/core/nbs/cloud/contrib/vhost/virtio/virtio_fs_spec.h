/*
 * virtio-fs protocol definitions
 */

#pragma once

#include "virtio_spec.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Device configuration layout.
 */
struct VHD_PACKED virtio_fs_config {
    /* Filesystem name (UTF-8, not NUL-terminated, padded with NULs) */
    u8 tag[36];

    /* Number of request queues exposed by the device. */
    le32 num_request_queues;
};

/*
 * Generic FUSE request in/out headers.
 * FIXME: these are duplicates of fuse_in_header/fuse_out_header, and should be
 * removed in favor of the latter.
 */
struct virtio_fs_in_header {
    le32 len;
    le32 opcode;
    le64 unique;
    le64 nodeid;
    le32 uid;
    le32 gid;
    le32 pid;
    le32 padding;
};

struct virtio_fs_out_header {
    le32 len;
    le32 error;
    le64 unique;
};

/*
 * Device operation request.
 *
 * Request is a variable sized structure:
 * struct virtio_fs_req {
 *     // Device-readable part
 *     struct virtio_fs_in_header in;
 *     u8 datain[];
 *
 *     // Device-writable part
 *     struct virtio_fs_out_header out;
 *     u8 dataout[];
 * };
 */

#ifdef __cplusplus
}
#endif
