#pragma once

#include <stdint.h>
#include <stddef.h>
#include "vhost/types.h"

#ifdef __cplusplus
extern "C" {
#endif

struct vhd_io;
struct vhd_request_queue;
struct vhd_vdev;

#define VHD_SECTOR_SHIFT    (9)
#define VHD_SECTOR_SIZE     (1ull << VHD_SECTOR_SHIFT)

#define VHD_BDEV_F_READONLY     (1ull << 0)
#define VHD_BDEV_F_DISCARD      (1ull << 1)
#define VHD_BDEV_F_WRITE_ZEROES (1ull << 2)

/**
 * Client-supplied block device backend definition
 */
struct vhd_bdev_info {
    /* Blockdev serial */
    const char *serial;

    /* Path to create listen sockets */
    const char *socket_path;

    /* Block size in bytes */
    uint32_t block_size;

    /* Optimal io size in bytes */
    uint32_t optimal_io_size;

    /* Total number of backend queues this device supports */
    uint32_t num_queues;

    /* Device size in blocks */
    uint64_t total_blocks;

    /* Supported VHD_BDEV_F_* features */
    uint64_t features;

    /* Gets called after mapping guest memory region */
    int (*map_cb)(void *addr, size_t len);

    /* Gets called before unmapping guest memory region */
    int (*unmap_cb)(void *addr, size_t len);

    /*
     * If set to a non-zero value, PTEs backing the guest memory regions
     * for this blockdev are flushed (unmapped and mapped back) every
     * N bytes processed by the backend. E.g. if this value is 1024, PTEs
     * will be flushed after the guest reads/writes 2 blocks.
     */
    size_t pte_flush_byte_threshold;
};

static inline bool vhd_blockdev_is_readonly(const struct vhd_bdev_info *bdev)
{
    return bdev->features & VHD_BDEV_F_READONLY;
}

static inline bool vhd_blockdev_has_discard(const struct vhd_bdev_info *bdev)
{
    return bdev->features & VHD_BDEV_F_DISCARD;
}

static inline bool vhd_blockdev_has_write_zeroes(
        const struct vhd_bdev_info *bdev)
{
    return bdev->features & VHD_BDEV_F_WRITE_ZEROES;
}

/**
 * Block io request type
 */
enum vhd_bdev_io_type {
    VHD_BDEV_READ,
    VHD_BDEV_WRITE,
    VHD_BDEV_DISCARD,
    VHD_BDEV_WRITE_ZEROES,
};

/**
 * In-flight blockdev io request
 */
struct vhd_bdev_io {
    enum vhd_bdev_io_type type;

    uint64_t first_sector;
    uint64_t total_sectors;
    struct vhd_sglist sglist;
};

struct vhd_bdev_io *vhd_get_bdev_io(struct vhd_io *io);

/**
 * Register a vhost block device.
 *
 * After registering a device, it will be accessible to clients through a vhost
 * socket.
 * All requests are submitted to attacher request queues for caller to process.
 *
 * @bdev        Caller block device info. The structure is used only for
 *              initialization and may be freed by caller after
 *              vhd_register_blockdev() returns.
 * @rqs         An array of request queues to use for dispatching device I/O
 *              requests.
 * @num_rqs     Number of request queues in the @rqs array.
 * @priv        Caller private data to associate with resulting vdev.
 */
struct vhd_vdev *vhd_register_blockdev(const struct vhd_bdev_info *bdev,
                                       struct vhd_request_queue **rqs,
                                       int num_rqs, void *priv);

/**
 * Unregister a vhost block device.
 */
void vhd_unregister_blockdev(struct vhd_vdev *vdev,
                             void (*unregister_complete)(void *), void *arg);

/**
 * Resize a vhost block device.
 *
 * The function change virtio config, that client may read by
 * VHOST_USER_GET_CONFIG command.
 *
 * Note, that client is not notified about config change, the caller is
 * responsible for this.
 */
void vhd_blockdev_set_total_blocks(struct vhd_vdev *vdev,
                                   uint64_t total_blocks);

#ifdef __cplusplus
}
#endif
