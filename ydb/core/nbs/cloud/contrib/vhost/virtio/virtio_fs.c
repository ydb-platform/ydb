#include <string.h>

#include "vhost/fs.h"

#include "virtio_fs.h"
#include "virtio_fs_spec.h"

#include "bio.h"
#include "virt_queue.h"
#include "logging.h"
#include "server_internal.h"
#include "vdev.h"

/******************************************************************************/

struct virtio_fs_io {
    struct virtio_virtq *vq;
    struct virtio_iov *iov;

    struct vhd_io io;
    struct vhd_fs_io fs_io;
};

/******************************************************************************/

static void complete_request(struct vhd_io *io)
{
    struct virtio_fs_io *vbio = containerof(io, struct virtio_fs_io, io);
    struct virtio_iov *viov = vbio->iov;
    /* if IN iov has at least one buffer it accomodates fuse_out_header */
    struct virtio_fs_out_header *out =
                        viov->niov_in ? viov->iov_in[0].base : NULL;
    uint32_t len = out ? out->len : 0;

    if (likely(io->status != VHD_BDEV_CANCELED)) {
        virtq_push(vbio->vq, vbio->iov, len);
    }

    virtio_free_iov(viov);
    vhd_free(vbio);
}

static int virtio_fs_handle_request(struct virtio_virtq *vq,
                                    struct vhd_io *io)
{
    io->vring = VHD_VRING_FROM_VQ(vq);
    return vhd_enqueue_request(vhd_get_rq_for_vring(io->vring), io);
}

static void handle_buffers(void *arg, struct virtio_virtq *vq, struct virtio_iov *iov)
{
    uint16_t niov = iov->niov_in + iov->niov_out;
    (void)arg;

    /*
     * Assume legacy message framing without VIRTIO_F_ANY_LAYOUT:
     * - virtio IN / FUSE OUT segments, with the first one fully containing
     *   fuse_in_header
     * - virtio OUT / FUSE IN segments, with the first one fully containing
     *   fuse_out_header (except FUSE_FORGET and FUSE_BATCH_FORGET which have
     *   no response part at all)
     */

    struct virtio_fs_in_header *in;
    struct virtio_fs_out_header *out;

    if (iov->niov_in && iov->iov_in[0].len < sizeof(*out)) {
        VHD_LOG_ERROR("No room for response in the request");
        abort_request(vq, iov);
        return;
    }

    if (!iov->niov_out || iov->iov_out[0].len < sizeof(*in)) {
        VHD_LOG_ERROR("Malformed request header");
        abort_request(vq, iov);
        return;
    }

    in = iov->iov_out[0].base;
    out = iov->niov_in ? iov->iov_in[0].base : NULL;

    struct virtio_fs_io *bio = vhd_zalloc(sizeof(*bio));
    bio->vq = vq;
    bio->iov = iov;
    bio->io.completion_handler = complete_request;

    bio->fs_io.sglist.nbuffers = niov;
    bio->fs_io.sglist.buffers = iov->buffers;

    int res = virtio_fs_handle_request(bio->vq, &bio->io);
    if (res != 0) {
        VHD_LOG_ERROR("request submission failed with %d", res);

        if (out) {
            out->len = sizeof(*out);
            out->error = res;
            out->unique = in->unique;
        }

        complete_request(&bio->io);
        return;
    }
}

/******************************************************************************/

int virtio_fs_init_dev(
    struct virtio_fs_dev *dev,
    struct vhd_fsdev_info *fsdev)
{
    VHD_VERIFY(dev);
    VHD_VERIFY(fsdev);

    dev->fsdev = fsdev;

    dev->config = (struct virtio_fs_config) {
        .num_request_queues = fsdev->num_queues,
    };
    if (fsdev->tag) {
        memcpy(dev->config.tag, fsdev->tag,
               MIN(strlen(fsdev->tag), sizeof(dev->config.tag)));
    }

    return 0;
}

int virtio_fs_dispatch_requests(struct virtio_fs_dev *dev,
                                struct virtio_virtq *vq)
{
    VHD_VERIFY(dev);
    VHD_VERIFY(vq);

    return virtq_dequeue_many(vq, handle_buffers, dev);
}

struct vhd_fs_io *vhd_get_fs_io(struct vhd_io *io)
{
    struct virtio_fs_io *bio = containerof(io, struct virtio_fs_io, io);
    return &bio->fs_io;
}
