#include <string.h>
#include <inttypes.h>

#include "vhost/blockdev.h"

#include "virtio_blk.h"
#include "virtio_blk_spec.h"

#include "bio.h"
#include "virt_queue.h"
#include "logging.h"
#include "server_internal.h"
#include "vdev.h"

/* virtio blk data for bdev io */
struct virtio_blk_io {
    struct virtio_virtq *vq;
    struct virtio_iov *iov;

    struct vhd_io io;
    struct vhd_bdev_io bdev_io;
};

static size_t iov_size(const struct vhd_buffer *iov, unsigned niov)
{
    size_t len;
    unsigned int i;

    len = 0;
    for (i = 0; i < niov; i++) {
        len += iov[i].len;
    }
    return len;
}

static uint8_t translate_status(enum vhd_bdev_io_result status)
{
    switch (status) {
    case VHD_BDEV_SUCCESS:
        return VIRTIO_BLK_S_OK;
    default:
        return VIRTIO_BLK_S_IOERR;
    }
}

static void set_status(struct virtio_iov *iov, uint8_t status)
{
    struct vhd_buffer *last_iov = &iov->iov_in[iov->niov_in - 1];
    *((uint8_t *)last_iov->base) = status;
}

static void complete_req(struct vhd_vdev *vdev, struct virtio_virtq *vq,
                         struct virtio_iov *iov, uint8_t status)
{
    size_t in_size;

    set_status(iov, status);
    /*
     * the last byte in the IN buffer is always written (for status), so pass
     * the total length of the IN buffer to virtq_push()
     */
    in_size = iov_size(iov->iov_in, iov->niov_in);
    virtq_push(vq, iov, in_size);

    if (status == VHD_BDEV_SUCCESS && vdev != NULL &&
        vdev->pte_flush_byte_threshold) {
        size_t out_size;

        out_size = iov_size(iov->iov_out, iov->niov_out);
        catomic_sub(&vdev->bytes_left_before_pte_flush, in_size + out_size);
    }

    virtio_free_iov(iov);
}

static void complete_io(struct vhd_io *io)
{
    struct virtio_blk_io *bio = containerof(io, struct virtio_blk_io, io);

    if (likely(bio->io.status != VHD_BDEV_CANCELED)) {
        complete_req(io->vring->vdev, bio->vq, bio->iov,
                     translate_status(bio->io.status));
    } else {
        virtio_free_iov(bio->iov);
    }

    vhd_free(bio);
}

static bool is_valid_block_range_req(uint64_t sector, size_t nsectors,
                                     uint64_t capacity)
{
    if (nsectors > capacity || sector > capacity - nsectors) {
        VHD_LOG_ERROR("Request (%" PRIu64 "s, +%zus) spans"
                      " beyond device capacity %" PRIu64,
                      sector, nsectors, capacity);
        return false;
    }

    return true;
}

static bool is_valid_req(uint64_t sector, size_t len, uint64_t capacity)
{
    size_t nsectors = len / VIRTIO_BLK_SECTOR_SIZE;

    if (len == 0) {
        VHD_LOG_ERROR("Zero size request");
        return false;
    }
    if (len % VIRTIO_BLK_SECTOR_SIZE) {
        VHD_LOG_ERROR("Request length %zu"
                      " is not a multiple of sector size %u",
                      len, VIRTIO_BLK_SECTOR_SIZE);
        return false;
    }

    return is_valid_block_range_req(sector, nsectors, capacity);
}

static bool bio_submit(struct virtio_blk_io *bio)
{
    int res = virtio_blk_handle_request(bio->vq, &bio->io);
    if (res != 0) {
        VHD_LOG_ERROR("bdev request submission failed with %d", res);
        vhd_free(bio);
        return false;
    }

    return true;
}

static void handle_inout(struct virtio_blk_dev *dev,
                         struct virtio_blk_req_hdr *req,
                         struct virtio_virtq *vq,
                         struct virtio_iov *iov)
{
    size_t len;
    uint16_t ndatabufs;
    struct vhd_buffer *pdata;
    enum vhd_bdev_io_type io_type;

    if (req->type == VIRTIO_BLK_T_IN) {
        io_type = VHD_BDEV_READ;
        pdata = &iov->iov_in[0];
        ndatabufs = iov->niov_in - 1;
    } else {
        if (virtio_blk_is_readonly(dev)) {
            VHD_LOG_ERROR("Write request to readonly device");
            goto fail_request;
        }
        io_type = VHD_BDEV_WRITE;
        pdata = &iov->iov_out[1];
        ndatabufs = iov->niov_out - 1;
    }

    len = iov_size(pdata, ndatabufs);

    if (!is_valid_req(req->sector, len, dev->config.capacity)) {
        goto fail_request;
    }

    struct virtio_blk_io *bio = vhd_zalloc(sizeof(*bio));
    bio->vq = vq;
    bio->iov = iov;
    bio->io.completion_handler = complete_io;

    bio->bdev_io.type = io_type;
    bio->bdev_io.first_sector = req->sector;
    bio->bdev_io.total_sectors = len / VIRTIO_BLK_SECTOR_SIZE;
    bio->bdev_io.sglist.nbuffers = ndatabufs;
    bio->bdev_io.sglist.buffers = pdata;

    if (!bio_submit(bio)) {
        goto fail_request;
    }

    /* request will be completed asynchronously */
    return;

fail_request:
    complete_req(NULL, vq, iov, VIRTIO_BLK_S_IOERR);
}

static void handle_discard_or_write_zeroes(struct virtio_blk_dev *dev,
                                           le32 type,
                                           struct virtio_virtq *vq,
                                           struct virtio_iov *iov)
{
    struct virtio_blk_discard_write_zeroes seg;
    struct virtio_blk_io *bio;
    enum vhd_bdev_io_type io_type;
    le32 max_sectors;
    bool is_discard = type == VIRTIO_BLK_T_DISCARD;
    const char *type_str = is_discard ? "discard" : "write-zeroes";
    VHD_ASSERT(is_discard || type == VIRTIO_BLK_T_WRITE_ZEROES);

    if (virtio_blk_is_readonly(dev)) {
        VHD_LOG_ERROR("%s request to readonly device", type_str);
        goto fail_request;
    }

    /*
     * The data used for discard, secure erase or write zeroes commands
     * consists of one or more segments. We support only one at the moment.
     */
    if (iov->niov_out != 2) {
        VHD_LOG_ERROR("Invalid number of segments for a "
                      "%s request %"PRIu16,
                      type_str, iov->niov_out);
        goto fail_request;
    }

    if (iov->iov_out[1].len != sizeof(seg)) {
        VHD_LOG_ERROR("Invalid %s segment size: "
                      "expected %zu, got %zu!", type_str,
                      sizeof(seg), iov->iov_out[1].len);
        goto fail_request;
    }

    memcpy(&seg, iov->iov_out[1].base, sizeof(seg));
    if (!is_valid_block_range_req(seg.sector, seg.num_sectors,
                                  dev->config.capacity)) {
        goto fail_request;
    }

    if (is_discard) {
        le32 alignment = dev->config.discard_sector_alignment;

        if (!VHD_IS_ALIGNED(seg.num_sectors, alignment)) {
            VHD_LOG_ERROR("Discard request sector count %"PRIu32
                          " not aligned to %"PRIu32,
                          seg.num_sectors, alignment);
            goto fail_request;
        }

        if (!VHD_IS_ALIGNED(seg.sector, alignment)) {
            VHD_LOG_ERROR("Discard request sector %"PRIu64
                          " not aligned to %"PRIu32,
                          seg.sector, alignment);
            goto fail_request;
        }

        io_type = VHD_BDEV_DISCARD;
        max_sectors = dev->config.max_discard_sectors;
    } else {
        io_type = VHD_BDEV_WRITE_ZEROES;
        max_sectors = dev->config.max_write_zeroes_sectors;
    }

    if (seg.num_sectors > max_sectors) {
        VHD_LOG_ERROR("%s request too large: "
                      "%"PRIu32" (max is %"PRIu32")",
                      type_str, seg.num_sectors, max_sectors);
        goto fail_request;
    }

    bio = vhd_zalloc(sizeof(*bio));
    bio->vq = vq;
    bio->iov = iov;
    bio->io.completion_handler = complete_io;
    bio->bdev_io.type = io_type;
    bio->bdev_io.first_sector = seg.sector;
    bio->bdev_io.total_sectors = seg.num_sectors;

    if (!bio_submit(bio)) {
        goto fail_request;
    }

    /* request will be completed asynchronously */
    return;

fail_request:
    complete_req(NULL, vq, iov, VIRTIO_BLK_S_IOERR);
}

static uint8_t handle_getid(struct virtio_blk_dev *dev,
                            struct virtio_iov *iov)
{
    if (iov->niov_in != 2) {
        VHD_LOG_ERROR("Bad number of IN segments %u in request", iov->niov_in);
        return VIRTIO_BLK_S_IOERR;
    }

    struct vhd_buffer *id_buf = &iov->iov_in[0];

    if (id_buf->len != VIRTIO_BLK_DISKID_LENGTH) {
        VHD_LOG_ERROR("Bad id buffer (len %zu)", id_buf->len);
        return VIRTIO_BLK_S_IOERR;
    }

    /*
     * strncpy will not add a null-term if src length is >= desc->len, which is
     * what we need
     */
    strncpy((char *) id_buf->base, dev->serial, id_buf->len);

    return VIRTIO_BLK_S_OK;
}

static bool dev_supports_req(struct virtio_blk_dev *dev, le32 type)
{
    int feature;

    switch (type) {
    case VIRTIO_BLK_T_IN:
    case VIRTIO_BLK_T_OUT:
    case VIRTIO_BLK_T_GET_ID:
        return true;
    case VIRTIO_BLK_T_DISCARD:
        feature = VIRTIO_BLK_F_DISCARD;
        break;
    case VIRTIO_BLK_T_WRITE_ZEROES:
        feature = VIRTIO_BLK_F_WRITE_ZEROES;
        break;
    default:
        return false;
    }

    return virtio_blk_has_feature(dev, feature);
}

static void handle_buffers(void *arg, struct virtio_virtq *vq,
                           struct virtio_iov *iov)
{
    uint8_t status;
    struct virtio_blk_dev *dev = arg;
    struct virtio_blk_req_hdr *req;
    le32 type;

    /*
     * Assume legacy message framing without VIRTIO_F_ANY_LAYOUT:
     * - one 16-byte device-readable segment for header
     * - data segments
     * - one 1-byte device-writable segment for status
     * FIXME: get rid of this assumption and support VIRTIO_F_ANY_LAYOUT
     */

    if (!iov->niov_in || iov->iov_in[iov->niov_in - 1].len != 1) {
        VHD_LOG_ERROR("No room for status response in the request");
        abort_request(vq, iov);
        return;
    }

    if (!iov->niov_out || iov->iov_out[0].len != sizeof(*req)) {
        VHD_LOG_ERROR("Malformed request header");
        abort_request(vq, iov);
        return;
    }

    req = iov->iov_out[0].base;
    type = req->type;

    if (!dev_supports_req(dev, type)) {
        VHD_LOG_WARN("Unknown or unsupported request type %"PRIu32, type);
        status = VIRTIO_BLK_S_UNSUPP;
        goto out;
    }

    switch (type) {
    case VIRTIO_BLK_T_IN:
    case VIRTIO_BLK_T_OUT:
        handle_inout(dev, req, vq, iov);
        return;         /* async completion */
    case VIRTIO_BLK_T_GET_ID:
        status = handle_getid(dev, iov);
        break;
    case VIRTIO_BLK_T_DISCARD:
    case VIRTIO_BLK_T_WRITE_ZEROES:
        handle_discard_or_write_zeroes(dev, type, vq, iov);
        return;         /* async completion */
    default:  /* unreachable because of dev_supports_req() */
        VHD_UNREACHABLE();
    };

out:
    complete_req(NULL, vq, iov, status);
}

/*////////////////////////////////////////////////////////////////////////////*/

int virtio_blk_dispatch_requests(struct virtio_blk_dev *dev,
                                 struct virtio_virtq *vq)
{
    return virtq_dequeue_many(vq, handle_buffers, dev);
}

__attribute__((weak))
int virtio_blk_handle_request(struct virtio_virtq *vq, struct vhd_io *io)
{
    io->vring = VHD_VRING_FROM_VQ(vq);
    return vhd_enqueue_request(vhd_get_rq_for_vring(io->vring), io);
}

size_t virtio_blk_get_config(struct virtio_blk_dev *dev, void *cfgbuf,
                             size_t bufsize, size_t offset)
{
    if (offset >= sizeof(dev->config)) {
        return 0;
    }

    size_t data_size = MIN(bufsize, sizeof(dev->config) - offset);

    memcpy(cfgbuf, (char *)(&dev->config) + offset, data_size);

    return data_size;
}

uint64_t virtio_blk_get_features(struct virtio_blk_dev *dev)
{
    return dev->features;
}

bool virtio_blk_has_feature(struct virtio_blk_dev *dev, int feature)
{
    const uint64_t mask = 1ull << feature;
    return (virtio_blk_get_features(dev) & mask) == mask;
}

bool virtio_blk_is_readonly(struct virtio_blk_dev *dev)
{
    return virtio_blk_has_feature(dev, VIRTIO_BLK_F_RO);
}

static void refresh_config_geometry(struct virtio_blk_config *config)
{
    /*
     * Here we use same max values like we did for blockstor-plugin.
     * But it seems that the real world max values are:
     */
    /* 63 for sectors */
    const uint8_t max_sectors = 255;
    /* 16 for heads */
    const uint8_t max_heads = 255;
    /* 16383 for cylinders */
    const uint16_t max_cylinders = 65535;

    config->geometry.sectors = MIN(config->capacity, max_sectors);
    config->geometry.heads =
        MIN(1 + (config->capacity - 1) / max_sectors, max_heads);
    config->geometry.cylinders =
        MIN(1 + (config->capacity - 1) / (max_sectors * max_heads),
            max_cylinders);
}

uint64_t virtio_blk_get_total_blocks(struct virtio_blk_dev *dev)
{
    return dev->config.capacity >> dev->config.topology.physical_block_exp;
}

void virtio_blk_set_total_blocks(struct virtio_blk_dev *dev,
                                 uint64_t total_blocks)
{
    uint64_t new_capacity =
        total_blocks << dev->config.topology.physical_block_exp;

    if (new_capacity > dev->config.capacity) {
        VHD_LOG_INFO("virtio-blk resize: %" PRIu64 " -> %" PRIu64,
                     dev->config.capacity, new_capacity);
    } else {
        VHD_LOG_WARN("virtio-blk resize not increasing: %"
                     PRIu64 " -> %" PRIu64,
                     dev->config.capacity, new_capacity);
    }

    dev->config.capacity = new_capacity;
    refresh_config_geometry(&dev->config);
}

void virtio_blk_init_dev(
    struct virtio_blk_dev *dev,
    const struct vhd_bdev_info *bdev)
{
    uint32_t phys_block_sectors = bdev->block_size >> VHD_SECTOR_SHIFT;
    uint8_t phys_block_exp = vhd_find_first_bit32(phys_block_sectors);

    dev->serial = vhd_strdup(bdev->serial);

    dev->features = VIRTIO_BLK_DEFAULT_FEATURES;
    if (vhd_blockdev_is_readonly(bdev)) {
        dev->features |= (1ull << VIRTIO_BLK_F_RO);
    }
    if (vhd_blockdev_has_discard(bdev)) {
        dev->features |= (1ull << VIRTIO_BLK_F_DISCARD);
    }
    if (vhd_blockdev_has_write_zeroes(bdev)) {
        dev->features |= (1ull << VIRTIO_BLK_F_WRITE_ZEROES);
    }

    /*
     * Both virtio and block backend use the same sector size of 512.  Don't
     * bother converting between the two, just assert they are the same.
     */
    VHD_STATIC_ASSERT(VHD_SECTOR_SIZE == VIRTIO_BLK_SECTOR_SIZE);

    dev->config.capacity = bdev->total_blocks << phys_block_exp;
    dev->config.blk_size = VHD_SECTOR_SIZE;
    dev->config.numqueues = bdev->num_queues;
    dev->config.topology.physical_block_exp = phys_block_exp;
    dev->config.topology.alignment_offset = 0;
    /* TODO: can get that from bdev info */
    dev->config.topology.min_io_size = 1;
    dev->config.topology.opt_io_size = bdev->optimal_io_size >> VHD_SECTOR_SHIFT;

    /*
     * Guarded by the assertion above:
     * VHD_SECTOR_SIZE == VIRTIO_BLK_SECTOR_SIZE
     */
    dev->config.discard_sector_alignment = 1;
    dev->config.max_discard_sectors = VIRTIO_BLK_MAX_DISCARD_SECTORS;
    dev->config.max_discard_seg = VIRTIO_BLK_MAX_DISCARD_SEGMENTS;

    dev->config.max_write_zeroes_sectors = VIRTIO_BLK_MAX_WRITE_ZEROES_SECTORS;
    dev->config.max_write_zeroes_seg = VIRTIO_BLK_MAX_WRITE_ZEROES_SEGMENTS;
    /*
     * Since we don't know anything about the user of the library beforehand
     * assume we _may_ unmap the sectors on write-zeroes.
     * TODO: maybe propagate this value from blockdev config at creation time?
     */
    dev->config.write_zeroes_may_unmap = 1;

    /*
     * Hardcode seg_max to 126. The same way like it's done for virtio-blk in
     * qemu 2.12 which is used by blockstor-plugin.
     * Although, this is an error prone approch which leads to the problems
     * when queue size != 128
     * (see https://www.mail-archive.com/qemu-devel@nongnu.org/msg668144.html)
     * we have to use it to provide migration compatibility between virtio-blk
     * and vhost-user-blk in both directions.
     */
    dev->config.seg_max = 128 - 2;

    refresh_config_geometry(&dev->config);
}

void virtio_blk_destroy_dev(struct virtio_blk_dev *dev)
{
    vhd_free(dev->serial);
    dev->serial = NULL;
}

struct vhd_bdev_io *vhd_get_bdev_io(struct vhd_io *io)
{
    struct virtio_blk_io *bio = containerof(io, struct virtio_blk_io, io);
    return &bio->bdev_io;
}
