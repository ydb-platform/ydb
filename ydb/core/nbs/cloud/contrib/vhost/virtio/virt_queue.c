#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <alloca.h>
#include <sys/eventfd.h>
#include <time.h>
#include <inttypes.h>

#include "catomic.h"
#include "virt_queue.h"
#include "logging.h"
#include "memmap.h"
#include "memlog.h"

/**
 * Holds private virtq data together with iovs we show users
 */
struct virtq_iov_private {
    /* Private virtq fields */
    uint16_t used_head;
    struct vhd_memory_map *mm;

    /* Iov we show to caller */
    struct virtio_iov iov;
};

static inline uint16_t virtq_get_used_event(struct virtio_virtq *vq)
{
    return vq->avail->ring[vq->qsz];
}

static inline void virtq_set_avail_event(struct virtio_virtq *vq,
                                         uint16_t avail_idx)
{
    *(le16 *)&vq->used->ring[vq->qsz] = avail_idx;
}

static int virtq_dequeue_one(struct virtio_virtq *vq, uint16_t head,
                             virtq_handle_buffers_cb handle_buffers_cb,
                             void *arg, bool resubmit);

static struct virtq_iov_private *clone_iov(struct virtio_virtq *vq)
{
    struct virtq_iov_private *priv;
    uint16_t niov = vq->niov_out + vq->niov_in;
    size_t iov_size = sizeof(struct vhd_buffer) * niov;

    priv = vhd_alloc(sizeof(*priv) + iov_size);
    memcpy(priv->iov.buffers, vq->buffers, iov_size);
    priv->iov.niov_out = vq->niov_out;
    priv->iov.iov_out = &priv->iov.buffers[0];
    priv->iov.niov_in = vq->niov_in;
    priv->iov.iov_in = &priv->iov.buffers[vq->niov_out];
    return priv;
}

void virtio_free_iov(struct virtio_iov *iov)
{
    struct virtq_iov_private *priv =
        containerof(iov, struct virtq_iov_private, iov);

    /* matched with ref in virtq_dequeue_one */
    vhd_memmap_unref(priv->mm);
    vhd_free(priv);
}

uint16_t virtio_iov_get_head(struct virtio_iov *iov)
{
    struct virtq_iov_private *priv =
        containerof(iov, struct virtq_iov_private, iov);
    return priv->used_head;
}

static int add_buffer(struct virtio_virtq *vq, void *addr, size_t len, bool in)
{
    uint16_t niov = vq->niov_out + vq->niov_in;

    if (niov >= vq->max_chain_len) {
        VHD_OBJ_ERROR(vq, "descriptor chain exceeds max length %u",
                      vq->max_chain_len);
        return -ENOBUFS;
    }

    if (in) {
        vq->niov_in++;
    } else {
        /*
         * 2.6.4.2 Driver Requirements: Message Framing The driver MUST place
         * any device-writable descriptor elements after any device-readable
         * descriptor elements.
         */
        if (vq->niov_in) {
            VHD_LOG_ERROR("Device-readable buffer after device-writable");
            return -EINVAL;
        }
        vq->niov_out++;
    }

    vq->buffers[niov] = (struct vhd_buffer) {
        .base = addr,
        .len = len,
        .write_only = in,
    };

    return 0;
}

static int map_buffer(struct virtio_virtq *vq, uint64_t gpa, size_t len,
                      bool write_only)
{
    void *addr = gpa_range_to_ptr(vq->mm, gpa, len);
    if (!addr) {
        VHD_OBJ_ERROR(vq, "Failed to map GPA 0x%" PRIx64 ", +0x%zx", gpa, len);
        return -EFAULT;
    }

    return add_buffer(vq, addr, len, write_only);
}

/* Modify inflight descriptor after dequeue request from the available ring. */
static void virtq_inflight_avail_update(struct virtio_virtq *vq, uint16_t head)
{
    if (!vq->inflight_region) {
        return;
    }

    if (vq->inflight_region->desc[head].inflight) {
        VHD_OBJ_WARN(vq, "inflight[%u]=%u (expected 0)", head,
                     vq->inflight_region->desc[head].inflight);
    }

    vq->inflight_region->desc[head].counter = vq->req_cnt;
    /*
     * Ensure the inflight region fields are updated in the expected order, so
     * that the next incarnation of the vhost backend can recover the state
     * regardless of where the current one dies.  There's no concurrent access
     * to the inflight region so only a compiler barrier is necessary.
     */
    barrier();
    vq->inflight_region->desc[head].inflight = 1;
    vq->req_cnt++;
}

/* Prepare the inflight descriptor for commit. */
static void virtq_inflight_used_update(struct virtio_virtq *vq, uint16_t head)
{
    if (!vq->inflight_region) {
        return;
    }

    vq->inflight_region->desc[head].next = vq->inflight_region->last_batch_head;
    /*
     * Ensure the inflight region fields are updated in the expected order, so
     * that the next incarnation of the vhost backend can recover the state
     * regardless of where the current one dies.  There's no concurrent access
     * to the inflight region so only a compiler barrier is necessary.
     */
    barrier();
    vq->inflight_region->last_batch_head = head;
}

/* Post commit inflight descriptor handling. */
static void virtq_inflight_used_commit(struct virtio_virtq *vq, uint16_t head)
{
    if (!vq->inflight_region) {
        return;
    }

    if (vq->inflight_region->desc[head].inflight != 1) {
        VHD_OBJ_WARN(vq, "inflight[%u]=%u (expected 1)", head,
                     vq->inflight_region->desc[head].inflight);
    }

    vq->inflight_region->desc[head].inflight = 0;
    /*
     * Make sure used_idx is stored after the desc content, so that the next
     * incarnation of the vhost backend sees consistent values regardless of
     * where the current one dies.  There's no concurrent access to the
     * inflight region so only a compiler barrier is necessary.
     */
    barrier();
    vq->inflight_region->used_idx = vq->used->idx;
}

/*
 * If the value of ``used_idx`` does not match the ``idx`` value of
 * used ring (means the inflight field of ``inflight_split_desc``
 * entries in last batch may be incorrect).
 */
static void virtq_inflight_reconnect_update(struct virtio_virtq *vq)
{
    uint16_t batch_size;
    uint16_t idx;

    vq->req_cnt = 0;
    if (!vq->inflight_region) {
        return;
    }

    /* Initialize the global req counter for the inflight descriptors. */
    for (idx = 0; idx < vq->inflight_region->desc_num; idx++) {
        if (vq->inflight_region->desc[idx].counter > vq->req_cnt) {
            vq->req_cnt = vq->inflight_region->desc[idx].counter;
        }
    }

    /* fresh inflight region (not a reconnect) */
    if (!vq->req_cnt) {
        goto out;
    }

    batch_size = vq->used->idx - vq->inflight_region->used_idx;
    if (!batch_size) {
        /* Last batch was sent successfully. Nothing to update. */
        goto out;
    }

    /* we don't do batching for now */
    VHD_ASSERT(batch_size == 1);

    idx = vq->inflight_region->last_batch_head;
    while (batch_size) {
        vq->inflight_region->desc[idx].inflight = 0;
        idx = vq->inflight_region->desc[idx].next;
        batch_size--;
    }

out:
    vq->req_cnt++;
    vq->inflight_region->used_idx = vq->used->idx;
}

static void virtio_virtq_reset_stat(struct virtio_virtq *vq)
{
    memset(&vq->stat, 0, sizeof(vq->stat));
}

/*
 * Windows drivers violate the spec and create descriptor chains up to this
 * long, regardless of the queue size.
 */
#define WINDOWS_CHAIN_LEN_MAX   (512 + 3)

void virtio_virtq_init(struct virtio_virtq *vq)
{
    VHD_ASSERT(!vq->buffers);

    vq->max_chain_len = MAX(vq->qsz, WINDOWS_CHAIN_LEN_MAX);

    vq->buffers = vhd_calloc(vq->max_chain_len, sizeof(vq->buffers[0]));

    /* Make check on the first virtq dequeue. */
    vq->inflight_check = true;
    virtq_inflight_reconnect_update(vq);

    virtio_virtq_reset_stat(vq);
}

void virtio_virtq_release(struct virtio_virtq *vq)
{
    VHD_ASSERT(vq->buffers);
    vhd_free(vq->buffers);
    *vq = (struct virtio_virtq) {};
}

struct inflight_resubmit {
    uint64_t counter;
    uint16_t head;
};

static int inflight_resubmit_compare(const void *first, const void *second)
{
    struct inflight_resubmit *left = (struct inflight_resubmit *)first;
    struct inflight_resubmit *right = (struct inflight_resubmit *)second;

    if (left->counter < right->counter) {
        return -1;
    }
    /* Can't return 0, since counter values are always different. */

    return 1;
}

/* Resubmit inflight requests on the virtqueue start. */
static int virtq_inflight_resubmit(struct virtio_virtq *vq,
                                   virtq_handle_buffers_cb handle_buffers_cb,
                                   void *arg)
{
    uint16_t desc_num;
    uint16_t cnt;
    struct inflight_resubmit *resubmit_array;
    uint16_t i;
    int res;

    if (!vq->inflight_region) {
        return 0;
    }

    desc_num = vq->inflight_region->desc_num;
    cnt = 0;
    resubmit_array = alloca(sizeof(*resubmit_array) * desc_num);
    for (i = 0; i < desc_num; i++) {
        if (vq->inflight_region->desc[i].inflight) {
            resubmit_array[cnt].counter = vq->inflight_region->desc[i].counter;
            resubmit_array[cnt].head = i;
            cnt++;
        }
    }
    qsort(resubmit_array, cnt, sizeof(*resubmit_array),
            inflight_resubmit_compare);

    res = 0;
    VHD_OBJ_DEBUG(vq, "cnt = %d inflight requests should be resubmitted", cnt);
    for (i = 0; i < cnt; i++) {
        uint16_t head = resubmit_array[i].head;
        if (head >= vq->qsz) {
            VHD_OBJ_ERROR(vq, "resubmit desc %u: head %u past queue size %u",
                          i, head, vq->qsz);
            return -ERANGE;
        }

        res = virtq_dequeue_one(vq, head, handle_buffers_cb, arg, true);
        if (res) {
            break;
        }
    }

    return res;
}

bool virtq_is_broken(struct virtio_virtq *vq)
{
    return vq->broken;
}

void mark_broken(struct virtio_virtq *vq)
{
    vq->broken = true;
}

#define DESCRIPTOR_ERROR(vq, idx, desc, fmt, ...)                       \
    VHD_OBJ_ERROR(vq, "[%u]{0x%" PRIx64 ", +0x%x, 0x%x, %u}: " fmt,     \
                  (idx), (desc)->addr, (desc)->len,                     \
                  (desc)->flags, (desc)->next, ##__VA_ARGS__)

static int walk_indirect_table(struct virtio_virtq *vq,
                               const struct virtq_desc *table_desc)
{
    int res;
    struct virtq_desc desc;
    struct virtq_desc *desc_table;
    uint16_t table_len = table_desc->len / sizeof(desc);
    uint16_t idx;

    if (table_desc->len == 0 || table_desc->len % sizeof(desc)) {
        VHD_OBJ_ERROR(vq, "Bad indirect descriptor table length %u",
                      table_desc->len);
        return -EINVAL;
    }

    desc_table = gpa_range_to_ptr(vq->mm, table_desc->addr, table_desc->len);
    if (!desc_table) {
        VHD_OBJ_ERROR(vq, "Failed to map indirect descriptor table "
                      "GPA 0x%" PRIx64 ", +0x%x",
                      table_desc->addr, table_desc->len);
        return -EFAULT;
    }

    for (idx = 0; ; idx = desc.next) {
        desc = desc_table[idx];

        if (desc.flags & VIRTQ_DESC_F_INDIRECT) {
            DESCRIPTOR_ERROR(vq, idx, &desc, "nested indirect descriptor");
            return -EMLINK;
        }

        res = map_buffer(vq, desc.addr, desc.len,
                         desc.flags & VIRTQ_DESC_F_WRITE);
        if (res != 0) {
            DESCRIPTOR_ERROR(vq, idx, &desc,
                             "failed to map descriptor in indirect table");
            return res;
        }

        if (!(desc.flags & VIRTQ_DESC_F_NEXT)) {
            break;
        }

        if (desc.next >= table_len) {
            DESCRIPTOR_ERROR(vq, idx, &desc,
                             "next points past indirect table size %u",
                             table_len);
            return -ERANGE;
        }
    }

    return 0;
}

/*
 * Traverse a descriptor chain starting at @head, mapping the descriptors found
 * and pushing them onto @vq->buffers.
 * Return the number of descriptors consumed, or -errno.
 */
static int walk_chain(struct virtio_virtq *vq, uint16_t head)
{
    uint16_t idx;
    uint16_t chain_len;
    struct virtq_desc desc;
    int res;

    vq->niov_out = vq->niov_in = 0;

    for (idx = head, chain_len = 1; ; idx = desc.next, chain_len++) {
        desc = vq->desc[idx];

        if (desc.flags & VIRTQ_DESC_F_INDIRECT) {
            if (desc.flags & VIRTQ_DESC_F_NEXT) {
                DESCRIPTOR_ERROR(vq, idx, &desc,
                                 "indirect descriptor must have no next");
                return -EINVAL;
            }

            res = walk_indirect_table(vq, &desc);
            if (res != 0) {
                DESCRIPTOR_ERROR(vq, idx, &desc,
                                 "failed to walk indirect descriptor table");
                return res;
            }

            break;
        }

        res = map_buffer(vq, desc.addr, desc.len,
                         desc.flags & VIRTQ_DESC_F_WRITE);
        if (res != 0) {
            DESCRIPTOR_ERROR(vq, idx, &desc, "failed to map");
            return res;
        }

        if (!(desc.flags & VIRTQ_DESC_F_NEXT)) {
            break;
        }

        if (desc.next >= vq->qsz) {
            DESCRIPTOR_ERROR(vq, idx, &desc,
                             "next points past queue size %u", vq->qsz);
            return -ERANGE;
        }
    }

    return chain_len;
}

int virtq_dequeue_many(struct virtio_virtq *vq,
                       virtq_handle_buffers_cb handle_buffers_cb,
                       void *arg)
{
    int res;
    uint16_t i;
    uint16_t num_avail;
    uint16_t avail, avail2;
    time_t now;

    if (virtq_is_broken(vq)) {
        VHD_OBJ_ERROR(vq, "virtqueue is broken, cannot process");
        return -ENODEV;
    }

    if (vq->inflight_check) {
        /* Check for the inflight requests once at the start. */
        VHD_OBJ_DEBUG(vq, "resubmit inflight requests, if any");
        res = virtq_inflight_resubmit(vq, handle_buffers_cb, arg);
        if (res) {
            goto queue_broken;
        }
        vq->inflight_check = false;
    }

    now = time(NULL);

    if (now - vq->stat.period_start_ts > 60) {
        vq->stat.period_start_ts = now;
        vq->stat.metrics.queue_len_max_60s = 0;
    }

    vq->stat.metrics.dispatch_total++;

    avail = vq->avail->idx;
    if (vq->has_event_idx) {
        smp_mb(); /* avail->idx read followed by avail_event write */
        while (true) {
            virtq_set_avail_event(vq, avail);
            smp_mb(); /* avail_event write followed by avail->idx read */
            avail2 = vq->avail->idx;
            if (avail2 == avail) {
                break;
            }
            smp_mb(); /* avail->idx read followed by avail_event write */
            avail = avail2;
        }
    }

    num_avail = avail - vq->last_avail;
    if (num_avail > vq->qsz) {
        VHD_OBJ_ERROR(vq, "num_avail %u (%u - %u) exceeds queue size %u",
                      num_avail, avail, vq->last_avail, vq->qsz);
        return -EOVERFLOW;
    }

    if (!num_avail) {
        vq->stat.metrics.dispatch_empty++;
        return 0;
    }

    vq->stat.metrics.queue_len_last = num_avail;
    if (vq->stat.metrics.queue_len_last > vq->stat.metrics.queue_len_max_60s) {
        vq->stat.metrics.queue_len_max_60s = vq->stat.metrics.queue_len_last;
    }

    /* Make sure that further desc reads do not pass avail->idx read. */
    smp_rmb();                  /* barrier pair [A] */

    /* TODO: disable extra notifies from this point */

    for (i = 0; i < num_avail; ++i) {
        /* Grab next descriptor head */
        uint16_t head = vq->avail->ring[vq->last_avail % vq->qsz];
        if (head >= vq->qsz) {
            VHD_OBJ_ERROR(vq, "avail %u: head %u past queue size %u",
                          vq->last_avail, head, vq->qsz);
            return -ERANGE;
        }

        res = virtq_dequeue_one(vq, head, handle_buffers_cb, arg, false);
        if (res) {
            goto queue_broken;
        }

        vq->stat.metrics.request_total++;
    }

    /* TODO: restore notifier mask here */
    return 0;

queue_broken:
    mark_broken(vq);
    return res;
}

static int virtq_dequeue_one(struct virtio_virtq *vq, uint16_t head,
                             virtq_handle_buffers_cb handle_buffers_cb,
                             void *arg, bool resubmit)
{
    int ret;

    ret = walk_chain(vq, head);
    if (ret < 0) {
        return ret;
    }

    /* Create iov copy from stored buffer for client handling */
    struct virtq_iov_private *priv = clone_iov(vq);
    priv->used_head = head;
    priv->mm = vq->mm;
    /* matched with unref in virtio_free_iov */
    vhd_memmap_ref(priv->mm);

    if (!resubmit) {
        virtq_inflight_avail_update(vq, head);
    }

    /* Send this over to handler */
    handle_buffers_cb(arg, vq, &priv->iov);

    vq->last_avail++;

    return 0;
}

static void vhd_log_buffers(struct vhd_memory_log *log,
                            struct vhd_memory_map *mm,
                            struct virtio_iov *viov)
{
    uint16_t i;
    for (i = 0; i < viov->niov_in; ++i) {
        struct vhd_buffer *iov = &viov->iov_in[i];
        vhd_mark_range_dirty(log, mm, iov->base, iov->len);
    }
}

/*
 * NOTE: this @mm is the one the request was started with, not the current one
 * on @vq
 */
static void vhd_log_modified(struct virtio_virtq *vq,
                             struct vhd_memory_map *mm,
                             struct virtio_iov *iov,
                             uint16_t used_idx)
{
    /* log modifications of buffers in descr */
    vhd_log_buffers(vq->log, mm, iov);
    if (vq->flags & VHOST_VRING_F_LOG) {
        /* log modification of used->idx */
        vhd_mark_gpa_range_dirty(vq->log,
                                 vq->used_gpa_base +
                                 offsetof(struct virtq_used, idx),
                                 sizeof(vq->used->idx));
        /* log modification of used->ring[idx] */
        vhd_mark_gpa_range_dirty(vq->log,
                                 vq->used_gpa_base +
                                 offsetof(struct virtq_used, ring[used_idx]),
                                 sizeof(vq->used->ring[0]));
    }
}

static void virtq_do_notify(struct virtio_virtq *vq)
{
    if (vq->notify_fd != -1) {
        eventfd_write(vq->notify_fd, 1);
    }
}

static bool virtq_need_notify(struct virtio_virtq *vq)
{
    if (!vq->has_event_idx) {
        /*
         * Virtio specification v1.0, 5.1.6.2.3:
         * Often a driver will suppress transmission interrupts using the
         * VIRTQ_AVAIL_F_NO_INTERRUPT flag (see 3.2.2 Receiving Used Buffers
         * From The Device) and check for used packets in the transmit path
         * of following packets.
         */
        return !(vq->avail->flags & VIRTQ_AVAIL_F_NO_INTERRUPT);
    }

    /*
     * Virtio specification v1.0, 2.4.7.2:
     * if the VIRTIO_F_EVENT_IDX feature bit is negotiated:
     * The device MUST ignore the lower bit of flags.
     * If the idx field in the used ring was
     * equal to used_event, the device MUST send an interrupt.
     * --------------------------------------------------------
     * Note: code below assumes that virtq_notify is always called
     * per one completion, and never per batch.
     */
    return virtq_get_used_event(vq) == (uint16_t)(vq->used->idx - 1);
}

static void virtq_notify(struct virtio_virtq *vq)
{
    /* expose used ring entries before checking used event */
    smp_mb();

    if (virtq_need_notify(vq)) {
        virtq_do_notify(vq);
    }
}

void virtq_push(struct virtio_virtq *vq, struct virtio_iov *iov, uint32_t len)
{
    /* Put buffer head index and len into used ring */
    struct virtq_iov_private *priv = containerof(iov, struct virtq_iov_private,
                                                 iov);
    uint16_t used_idx = vq->used->idx % vq->qsz;
    struct virtq_used_elem *used = &vq->used->ring[used_idx];
    used->id = priv->used_head;
    used->len = len;

    virtq_inflight_used_update(vq, used->id);

    smp_wmb();                  /* barrier pair [A] */
    vq->used->idx++;

    virtq_inflight_used_commit(vq, used->id);
    VHD_OBJ_DEBUG(vq, "head = %d", priv->used_head);

    /* use memmap the request was started with rather than the current one */
    if (vq->log) {
        vhd_log_modified(vq, priv->mm, &priv->iov, used_idx);
    }

    virtq_notify(vq);
    vq->stat.metrics.request_completed++;
}

void virtq_set_notify_fd(struct virtio_virtq *vq, int fd)
{
    vq->notify_fd = fd;

    /*
     * Always notify new fd because on initial setup QEMU sets up kick_fd
     * before call_fd, so before call_fd becomes configured there can be
     * already processed descriptors that guest wasn't notified about.
     * And on reconnect connection may have been lost before the server has
     * had a chance to signal guest.
     */
    virtq_do_notify(vq);
}

void virtio_virtq_get_stat(struct virtio_virtq *vq,
                           struct vhd_vq_metrics *metrics)
{
    *metrics = vq->stat.metrics;
}

__attribute__((weak))
void abort_request(struct virtio_virtq *vq, struct virtio_iov *iov)
{
    /*
     * FIXME: this is called when the message framing is messed up.  This
     * appears severe enough to just stop processing the virtq and mark it
     * broken
     */
    VHD_LOG_ERROR("no valid virtio request found, queue %p should be aborted", vq);
    virtq_push(vq, iov, 0);
    virtio_free_iov(iov);
}
