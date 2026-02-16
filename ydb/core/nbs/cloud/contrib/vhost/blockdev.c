#include <inttypes.h>
#include <stdint.h>

#include "vhost/blockdev.h"
#include "server_internal.h"
#include "vdev.h"
#include "logging.h"

#include "bio.h"
#include "virtio/virtio_blk.h"

struct vhd_bdev {
    /* Base vdev */
    struct vhd_vdev vdev;

    /* VM-facing interface type */
    struct virtio_blk_dev vblk;

    LIST_ENTRY(vhd_bdev) blockdevs;
};

static LIST_HEAD(, vhd_bdev) g_bdev_list = LIST_HEAD_INITIALIZER(g_bdev_list);

#define VHD_BLOCKDEV_FROM_VDEV(ptr) containerof(ptr, struct vhd_bdev, vdev)

/*////////////////////////////////////////////////////////////////////////////*/

static uint64_t vblk_get_features(struct vhd_vdev *vdev)
{
    struct vhd_bdev *dev = VHD_BLOCKDEV_FROM_VDEV(vdev);
    return virtio_blk_get_features(&dev->vblk);
}

static int vblk_set_features(struct vhd_vdev *vdev, uint64_t features)
{
    return 0;
}

/* vhost_get_config assumes that config is less than VHOST_USER_CONFIG_SPACE_MAX */
VHD_STATIC_ASSERT(sizeof(struct virtio_blk_config) <= VHOST_USER_CONFIG_SPACE_MAX);

static size_t vblk_get_config(struct vhd_vdev *vdev, void *cfgbuf,
                              size_t bufsize, size_t offset)
{
    struct vhd_bdev *dev = VHD_BLOCKDEV_FROM_VDEV(vdev);

    return virtio_blk_get_config(&dev->vblk, cfgbuf, bufsize, offset);
}

static int vblk_dispatch(struct vhd_vdev *vdev, struct vhd_vring *vring)
{
    struct vhd_bdev *dev = VHD_BLOCKDEV_FROM_VDEV(vdev);
    return virtio_blk_dispatch_requests(&dev->vblk, &vring->vq);
}

static void vblk_free(struct vhd_vdev *vdev)
{
    struct vhd_bdev *bdev = VHD_BLOCKDEV_FROM_VDEV(vdev);

    LIST_REMOVE(bdev, blockdevs);
    virtio_blk_destroy_dev(&bdev->vblk);
    vhd_free(bdev);
}

static const struct vhd_vdev_type g_virtio_blk_vdev_type = {
    .desc               = "virtio-blk",
    .get_features       = vblk_get_features,
    .set_features       = vblk_set_features,
    .get_config         = vblk_get_config,
    .dispatch_requests  = vblk_dispatch,
    .free               = vblk_free,
};

struct set_total_blocks {
    struct vhd_vdev *vdev;
    uint64_t total_blocks;
};

static void set_total_blocks_entry(struct vhd_work *work, void *opaque)
{
    struct set_total_blocks *stb = opaque;
    struct vhd_bdev *dev = VHD_BLOCKDEV_FROM_VDEV(stb->vdev);

    virtio_blk_set_total_blocks(&dev->vblk, stb->total_blocks);
    vhd_complete_work(work, 0);
}

void vhd_blockdev_set_total_blocks(struct vhd_vdev *vdev, uint64_t total_blocks)
{
    struct set_total_blocks stb = {
        .vdev = vdev,
        .total_blocks = total_blocks,
    };

    VHD_OBJ_INFO(vdev, "Set total blocks %" PRIu64, total_blocks);

    /*
     * Modify virtio config in g_vhost_evloop, to not interfere with .get_config
     *
     * We don't need vdev_submit_work_and_wait() logic here, as setting
     * total_blocks in config is unrelated stopping process, so it should not be
     * a problem intersect with wdev_stop_work work.
     */
    int ret = vhd_submit_ctl_work_and_wait(set_total_blocks_entry, &stb);
    VHD_VERIFY(ret == 0);
}

static bool blockdev_validate_features(const struct vhd_bdev_info *bdev)
{
    const uint64_t valid_features = VHD_BDEV_F_READONLY |
                                    VHD_BDEV_F_DISCARD |
                                    VHD_BDEV_F_WRITE_ZEROES;
    return (bdev->features & valid_features) == bdev->features;
}

struct vhd_vdev *vhd_register_blockdev(const struct vhd_bdev_info *bdev,
                                       struct vhd_request_queue **rqs,
                                       int num_rqs, void *priv)
{
    int res;

    if (!bdev->total_blocks || !bdev->block_size) {
        VHD_LOG_ERROR("Zero blockdev capacity %" PRIu64 " * %" PRIu32,
                      bdev->total_blocks, bdev->block_size);
        return NULL;
    }

    if ((bdev->block_size & (bdev->block_size - 1)) ||
        bdev->block_size % VHD_SECTOR_SIZE) {
        VHD_LOG_ERROR("Block size %" PRIu32 " is not"
                      " a power of two multiple of sector size (%llu)",
                      bdev->block_size, VHD_SECTOR_SIZE);
        return NULL;
    }

    if (bdev->optimal_io_size % bdev->block_size) {
        VHD_LOG_ERROR("Optimal io size %" PRIu32 " is not"
                      " a multiple of block size (%" PRIu32 ")",
                      bdev->optimal_io_size, bdev->block_size);
        return NULL;
    }

    if (!blockdev_validate_features(bdev)) {
        VHD_LOG_ERROR("Invalid blockdev features %" PRIu64, bdev->features);
        return NULL;
    }

    struct vhd_bdev *dev = vhd_zalloc(sizeof(*dev));

    virtio_blk_init_dev(&dev->vblk, bdev);

    res = vhd_vdev_init_server(&dev->vdev, bdev->socket_path,
                               &g_virtio_blk_vdev_type,
                               bdev->num_queues, rqs, num_rqs, priv,
                               bdev->map_cb, bdev->unmap_cb,
                               bdev->pte_flush_byte_threshold);
    if (res != 0) {
        goto error_out;
    }

    LIST_INSERT_HEAD(&g_bdev_list, dev, blockdevs);
    return &dev->vdev;

error_out:
    virtio_blk_destroy_dev(&dev->vblk);
    vhd_free(dev);
    return NULL;
}

void vhd_unregister_blockdev(struct vhd_vdev *vdev,
                             void (*unregister_complete)(void *), void *arg)
{
    vhd_vdev_stop_server(vdev, unregister_complete, arg);
}
