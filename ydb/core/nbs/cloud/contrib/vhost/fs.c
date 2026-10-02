#include "vhost/fs.h"
#include "virtio/virtio_fs.h"

#include "bio.h"
#include "logging.h"
#include "server_internal.h"
#include "vdev.h"

/******************************************************************************/

struct vhd_fsdev {
    /* Base vdev */
    struct vhd_vdev vdev;

    /* VM-facing interface type */
    struct virtio_fs_dev vfs;

    LIST_ENTRY(vhd_fsdev) fsdevs;
};

static LIST_HEAD(, vhd_fsdev) g_fsdev_list = LIST_HEAD_INITIALIZER(g_fsdev_list);

#define VHD_FSDEV_FROM_VDEV(ptr) containerof(ptr, struct vhd_fsdev, vdev)

/******************************************************************************/

static uint64_t vfs_get_features(struct vhd_vdev *vdev)
{
    return VIRTIO_FS_DEFAULT_FEATURES;
}

static int vfs_set_features(struct vhd_vdev *vdev, uint64_t features)
{
    return 0;
}

static size_t vfs_get_config(struct vhd_vdev *vdev, void *cfgbuf,
                             size_t bufsize, size_t offset)
{
    struct vhd_fsdev *dev = VHD_FSDEV_FROM_VDEV(vdev);

    if (offset >= sizeof(dev->vfs.config)) {
        return 0;
    }

    size_t data_size = MIN(bufsize, sizeof(dev->vfs.config) - offset);

    memcpy(cfgbuf, (char *)(&dev->vfs.config) + offset, data_size);

    return data_size;
}

static int vfs_dispatch_requests(struct vhd_vdev *vdev,
                                 struct vhd_vring *vring)
{
    struct vhd_fsdev *dev = VHD_FSDEV_FROM_VDEV(vdev);
    return virtio_fs_dispatch_requests(&dev->vfs, &vring->vq);
}

static void vfs_free(struct vhd_vdev *vdev)
{
    struct vhd_fsdev *dev = VHD_FSDEV_FROM_VDEV(vdev);

    LIST_REMOVE(dev, fsdevs);
    vhd_free(dev);
}

static const struct vhd_vdev_type g_virtio_fs_vdev_type = {
    .desc               = "virtio-fs",
    .get_features       = vfs_get_features,
    .set_features       = vfs_set_features,
    .get_config         = vfs_get_config,
    .dispatch_requests  = vfs_dispatch_requests,
    .free               = vfs_free,
};

/******************************************************************************/

struct vhd_vdev *vhd_register_fs(struct vhd_fsdev_info *fsdev,
                                 struct vhd_request_queue *rq,
                                 void *priv)
{
    return vhd_register_fs_mq(fsdev, &rq, 1, priv);
}

struct vhd_vdev *vhd_register_fs_mq(struct vhd_fsdev_info *fsdev,
                                    struct vhd_request_queue **rqs,
                                    int num_rqs,
                                    void *priv)
{
    VHD_VERIFY(fsdev);
    VHD_VERIFY(rqs);

    struct vhd_fsdev *dev = vhd_zalloc(sizeof(*dev));

    int res = virtio_fs_init_dev(&dev->vfs, fsdev);
    if (res != 0) {
        goto error_out;
    }

    res = vhd_vdev_init_server(&dev->vdev, fsdev->socket_path, &g_virtio_fs_vdev_type,
                               fsdev->num_queues, rqs, num_rqs, priv, NULL, NULL, 0);
    if (res != 0) {
        goto error_out;
    }

    LIST_INSERT_HEAD(&g_fsdev_list, dev, fsdevs);
    return &dev->vdev;

error_out:
    vhd_free(dev);
    return NULL;
}

void vhd_unregister_fs(struct vhd_vdev *vdev,
                       void (*unregister_complete)(void *),
                       void *arg)
{
    vhd_vdev_stop_server(vdev, unregister_complete, arg);
}
