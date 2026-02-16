#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <pthread.h>
#include <inttypes.h>

#include "vdev.h"
#include "server_internal.h"
#include "logging.h"
#include "memmap.h"
#include "memlog.h"

#define VHOST_REQ(req) [VHOST_USER_ ## req] = #req
static const char *const vhost_req_names[] = {
    VHOST_REQ(GET_FEATURES),
    VHOST_REQ(SET_FEATURES),
    VHOST_REQ(SET_OWNER),
    VHOST_REQ(RESET_OWNER),
    VHOST_REQ(SET_MEM_TABLE),
    VHOST_REQ(SET_LOG_BASE),
    VHOST_REQ(SET_LOG_FD),
    VHOST_REQ(SET_VRING_NUM),
    VHOST_REQ(SET_VRING_ADDR),
    VHOST_REQ(SET_VRING_BASE),
    VHOST_REQ(GET_VRING_BASE),
    VHOST_REQ(SET_VRING_KICK),
    VHOST_REQ(SET_VRING_CALL),
    VHOST_REQ(SET_VRING_ERR),
    VHOST_REQ(GET_PROTOCOL_FEATURES),
    VHOST_REQ(SET_PROTOCOL_FEATURES),
    VHOST_REQ(GET_QUEUE_NUM),
    VHOST_REQ(SET_VRING_ENABLE),
    VHOST_REQ(SEND_RARP),
    VHOST_REQ(NET_SET_MTU),
    VHOST_REQ(SET_SLAVE_REQ_FD),
    VHOST_REQ(IOTLB_MSG),
    VHOST_REQ(SET_VRING_ENDIAN),
    VHOST_REQ(GET_CONFIG),
    VHOST_REQ(SET_CONFIG),
    VHOST_REQ(CREATE_CRYPTO_SESSION),
    VHOST_REQ(CLOSE_CRYPTO_SESSION),
    VHOST_REQ(POSTCOPY_ADVISE),
    VHOST_REQ(POSTCOPY_LISTEN),
    VHOST_REQ(POSTCOPY_END),
    VHOST_REQ(GET_INFLIGHT_FD),
    VHOST_REQ(SET_INFLIGHT_FD),
    VHOST_REQ(GET_MAX_MEM_SLOTS),
    VHOST_REQ(ADD_MEM_REG),
    VHOST_REQ(REM_MEM_REG),
};
#undef VHOST_REQ

// Internal synthetic requests
#define VHOST_INTERNAL_REQUEST_BASE 0xFFFF0000
#define VHOST_INTERNAL_MEMMAP_FLUSH_AFTER_THRESHOLD VHOST_INTERNAL_REQUEST_BASE

#define VHOST_REQ(req) \
    [VHOST_INTERNAL_ ## req - VHOST_INTERNAL_REQUEST_BASE] = #req

static const char *const internal_vhost_req_names[] = {
    VHOST_REQ(MEMMAP_FLUSH_AFTER_THRESHOLD),
};
#undef VHOST_REQ

static const char *vhost_req_name(uint32_t req)
{
    if (req >= VHOST_INTERNAL_REQUEST_BASE) {
        req -= VHOST_INTERNAL_REQUEST_BASE;

        if (req >= VHD_ARRAY_SIZE(internal_vhost_req_names) ||
            !internal_vhost_req_names[req]) {
            return "**UNKNOWN**";
        }
        return internal_vhost_req_names[req];
    }

    if (req >= VHD_ARRAY_SIZE(vhost_req_names) || !vhost_req_names[req]) {
        return "**UNKNOWN**";
    }
    return vhost_req_names[req];
}

static LIST_HEAD(, vhd_vdev) g_vdevs = LIST_HEAD_INITIALIZER(g_vdevs);

static uint16_t vring_idx(struct vhd_vring *vring)
{
    return vring - vring->vdev->vrings;
}

struct vhd_request_queue *vhd_get_rq_for_vring(struct vhd_vring *vring)
{
    struct vhd_vdev *vdev = vring->vdev;
    return vdev->rqs[vring_idx(vring) % vdev->num_rqs];
}

static void replace_fd(int *fd, int newfd)
{
    if (*fd >= 0) {
        close(*fd);
    }
    *fd = newfd;
}

/* Return size of per queue inflight buffer. */
static size_t vring_inflight_buf_size(uint16_t num)
{
    return sizeof(struct inflight_split_region) +
        num * sizeof(struct inflight_split_desc);
}

static int vring_kick(void *opaque)
{
    int ret;
    struct vhd_vring *vring = opaque;
    struct vhd_vdev *vdev = vring->vdev;

    /*
     * Clear vring event now, before processing virtq.
     * Otherwise we might lose events if guest has managed to
     * signal eventfd again while we were processing
     */
    vhd_clear_eventfd(vring->kickfd);

    if (!vring->vq.enabled) {
        return 0;
    }

    ret = vdev->type->dispatch_requests(vdev, vring);
    if (ret < 0) {
        /*
         * seems like full-fledged vring stop may surprize the client, so just
         * disable notifications and effectively suspend the vring
         */
        VHD_OBJ_ERROR(vring, "dispatch_requests: %s, suspending vring",
                      strerror(-ret));
        vhd_detach_io_handler(vring->kick_handler);
    }

    return 0;
}

/*
 * Resolve (and thus validate) the addresses used by the virtq, and record them
 * in the shadow structure, in the control event loop, to be later propagated
 * into the actual virtq in the dataplane.
 */
static int vring_update_shadow_vq_addrs(struct vhd_vring *vring,
                                        struct vhd_memory_map *mm)
{
    void *desc = uva_to_ptr(mm, vring->shadow_vq.desc_addr);
    void *used = uva_to_ptr(mm, vring->shadow_vq.used_addr);
    void *avail = uva_to_ptr(mm, vring->shadow_vq.avail_addr);

    if (!desc || !used || !avail) {
        VHD_OBJ_ERROR(vring, "failed to resolve vring addresses "
                      "(0x%" PRIx64 ", 0x%" PRIx64 ", 0x%" PRIx64 ")",
                      vring->shadow_vq.desc_addr, vring->shadow_vq.used_addr,
                      vring->shadow_vq.avail_addr);
        return -EINVAL;
    }

    vring->shadow_vq.desc = desc;
    vring->shadow_vq.used = used;
    vring->shadow_vq.avail = avail;
    vring->shadow_vq.mm = mm;

    return 0;
}

static void vring_sync_to_virtq(struct vhd_vring *vring)
{
    bool should_kick;

    vring->vq.flags = vring->shadow_vq.flags;
    vring->vq.desc = vring->shadow_vq.desc;
    vring->vq.used = vring->shadow_vq.used;
    vring->vq.avail = vring->shadow_vq.avail;
    vring->vq.used_gpa_base = vring->shadow_vq.used_gpa_base;
    vring->vq.mm = vring->shadow_vq.mm;
    vring->vq.log = vring->shadow_vq.log;

    /*
     * Since QEMU sets kickfd before enabling the vring, it gets kicked while
     * still in the disabled state. Kick manually after enabling here just
     * in case.
     */
    should_kick = !vring->vq.enabled && vring->shadow_vq.enabled;
    vring->vq.enabled = vring->shadow_vq.enabled;

    virtq_set_notify_fd(&vring->vq, vring->callfd);

    if (should_kick) {
        vhd_set_eventfd(vring->kickfd);
    }
}

/*
 * There are several counters of vrings in particular state:
 *
 * ->num_vrings_handling_msg
 *    counts vrings that are performing some state transitions in response to a
 *    client message; once it drops to zero, the handling of this message is
 *    finished, the reply is sent if necessary, and the device resumes
 *    accepting further messages
 *
 * ->num_vrings_started
 *    counts vrings that have been started and haven't acknowledged being
 *    stopped yet; once it drops to zero, the device is safe to assume no more
 *    requests will be submitted to the backend, and therefore release the
 *    semaphore and let vhd_vdev_stop_server return
 *
 * ->num_vrings_in_flight
 *    counts vrings that have any potential to have requests in flight: it's
 *    incremented when a vring is started and decremented when a stopped vring
 *    reports there are no requests remaining in flight
 */
static void vring_handle_msg(struct vhd_vring *vring,
                             void (*handler_bh)(void *))
{
    struct vhd_vdev *vdev = vring->vdev;

    if (!vring->started_in_ctl) {
        return;
    }

    vdev->num_vrings_handling_msg++;

    vhd_run_in_rq(vhd_get_rq_for_vring(vring), handler_bh, vring);
}

static void vdev_disconnect(struct vhd_vdev *vdev);

static void vring_mark_msg_handled(struct vhd_vring *vring)
{
    struct vhd_vdev *vdev = vring->vdev;

    VHD_ASSERT(vdev->num_vrings_handling_msg);
    vdev->num_vrings_handling_msg--;

    if (!vdev->num_vrings_handling_msg) {
        int ret = vdev->handle_complete(vdev);
        vdev->handle_complete = NULL;
        if (ret < 0) {
            vdev_disconnect(vdev);
        }
    }
}

static void vring_mark_msg_handled_bh(void *opaque)
{
    vring_mark_msg_handled(opaque);
}

static void vdev_vrings_stopped(struct vhd_vdev *vdev);
static void vdev_drained(struct vhd_vdev *vdev);

static bool vdev_in_use(struct vhd_vdev *vdev)
{
    return vdev->num_vrings_in_flight || vdev->num_vrings_handling_msg ||
           vdev->num_vrings_started || vdev->conn_handler;
}

static void vdev_maybe_finished(struct vhd_vdev *vdev)
{
    if (!vdev->num_vrings_started) {
        vdev_vrings_stopped(vdev);
    }

    if (!vdev_in_use(vdev)) {
        vdev_drained(vdev);
    }
}

static void vring_mark_stopped(struct vhd_vring *vring)
{
    struct vhd_vdev *vdev = vring->vdev;

    VHD_OBJ_INFO(vring, "stopped vring with %u in-flight requests",
                 vring->num_in_flight_at_stop);

    replace_fd(&vring->kickfd, -1);

    VHD_ASSERT(vdev->num_vrings_started);
    vdev->num_vrings_started--;

    vdev_maybe_finished(vdev);
}

static void vring_mark_stopped_bh(void *opaque)
{
    vring_mark_stopped(opaque);
}

static void vring_reset(struct vhd_vring *vring)
{
    replace_fd(&vring->callfd, -1);
    replace_fd(&vring->kickfd, -1);
    replace_fd(&vring->errfd, -1);

    memset(&vring->shadow_vq, 0, sizeof(vring->shadow_vq));

    vring->num_in_flight_at_stop = 0;

    vring->disconnecting = false;
}

static void vring_mark_drained(struct vhd_vring *vring)
{
    struct vhd_vdev *vdev = vring->vdev;

    VHD_ASSERT(vring->started_in_ctl);
    vring->started_in_ctl = false;

    if (vring->on_drain_cb) {
        int ret = vring->on_drain_cb(vring);
        vring->on_drain_cb = NULL;
        if (ret < 0) {
            vdev_disconnect(vdev);
        }
    }

    virtio_virtq_release(&vring->vq);

    VHD_ASSERT(vdev->num_vrings_in_flight);
    vdev->num_vrings_in_flight--;

    vdev_maybe_finished(vdev);
}

static void vring_mark_drained_bh(void *opaque)
{
    vring_mark_drained(opaque);
}

static void flush_vdev_ptes(struct vhd_vdev *vdev);

static void flush_vdev_ptes_bh(void *opaque)
{
    struct vhd_vdev *vdev = opaque;
    flush_vdev_ptes(vdev);
}

void vhd_vring_inc_in_flight(struct vhd_vring *vring)
{
    vring->num_in_flight++;
}

void vhd_vring_dec_in_flight(struct vhd_vring *vring)
{
    vring->num_in_flight--;

    if (vring->started_in_rq) {
        struct vhd_vdev *vdev = vring->vdev;

        if (vdev->pte_flush_byte_threshold && !vring->disconnecting) {
            int64_t bytes_left;

            if (catomic_load_acquire(&vdev->pte_flush_pending)) {
                return;
            }

            bytes_left = catomic_load_acquire(&vdev->bytes_left_before_pte_flush);
            if (bytes_left <= 0) {
                catomic_store_release(&vdev->pte_flush_pending, true);
                vhd_run_in_ctl(flush_vdev_ptes_bh, vdev);
            }
        }

        return;
    }

    if (!vring->num_in_flight) {
        vhd_run_in_ctl(vring_mark_drained_bh, vring);
    }
}

static void vring_stop_bh(void *opaque)
{
    struct vhd_vring *vring = opaque;

    if (!vring->started_in_rq) {
        return;
    }

    vhd_del_io_handler(vring->kick_handler);
    vring->kick_handler = NULL;

    /*
     * FIXME: if the vring is stopped on request from the client via
     * GET_VRING_BASE message (as opposed to a disconnect), the requests have
     * to run through the backend because inflight requests aren't migrated by
     * QEMU yet.  Once this is fixed (with the help of the inflight region
     * migration), the requests should be canceled here unconditionally,
     * speeding up vm migration and shutdown.
     */
    if (vring->disconnecting) {
        vhd_cancel_queued_requests(vhd_get_rq_for_vring(vring), vring);
    }

    vring->started_in_rq = false;

    vring->num_in_flight_at_stop = vring->num_in_flight;
    vhd_run_in_ctl(vring_mark_stopped_bh, vring);
    if (!vring->num_in_flight) {
        vhd_run_in_ctl(vring_mark_drained_bh, vring);
    }
}

static void vring_disconnect(struct vhd_vring *vring)
{
    if (vring->started_in_ctl) {
        /*
         * If vring_start_bh gets reordered with vring_stop_bh, make sure it
         * doesn't actually start vring.
         */
        vring->disconnecting = true;

        vhd_run_in_rq(vhd_get_rq_for_vring(vring), vring_stop_bh, vring);
    }
}

/*
 * Receive and store the message from the socket. Fill in the file
 * descriptor array. Return number of bytes received or
 * negative error code in case of error.
 */
static ssize_t net_recv_msg(int fd, struct vhost_user_msg_hdr *hdr,
                            void *payload, size_t len,
                            int *fds, size_t *num_fds)
{
    ssize_t ret;
    ssize_t rlen;
    struct cmsghdr *cmsg;
    int fds_rcvd[VHOST_USER_MAX_FDS];
    size_t num_fds_rcvd = 0;
    size_t i;
    union {
        char buf[CMSG_SPACE(sizeof(fds_rcvd))];
        struct cmsghdr cmsg_align;
    } control;
    struct iovec iov = {
        .iov_base = hdr,
        .iov_len = sizeof(*hdr),
    };
    struct msghdr msgh = {
        .msg_iov = &iov,
        .msg_iovlen = 1,
        .msg_control = &control,
        .msg_controllen = sizeof(control),
    };

    do {
        ret = recvmsg(fd, &msgh, MSG_CMSG_CLOEXEC);
    } while (ret < 0 && errno == EINTR);

    if (ret == 0) {
        goto out;
    }
    if (ret < 0) {
        ret = -errno;
        VHD_LOG_ERROR("recvmsg: %s", strerror(-ret));
        goto out;
    }

    for (cmsg = CMSG_FIRSTHDR(&msgh); cmsg; cmsg = CMSG_NXTHDR(&msgh, cmsg)) {
        if ((cmsg->cmsg_level == SOL_SOCKET) &&
            (cmsg->cmsg_type == SCM_RIGHTS)) {
            num_fds_rcvd = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
            memcpy(fds_rcvd, CMSG_DATA(cmsg), num_fds_rcvd * sizeof(int));
            break;
        }
    }

    if (ret != sizeof(*hdr)) {
        VHD_LOG_ERROR("recvmsg() read %zd expected %lu", ret, sizeof(*hdr));
        ret = -EIO;
        goto out;
    }
    if (hdr->size > len) {
        VHD_LOG_ERROR("payload size %d exceeds buffer size %zu", hdr->size,
                      len);
        ret = -EMSGSIZE;
        goto out;
    }

    do {
        rlen = read(fd, payload, hdr->size);
    } while (rlen < 0 && errno == EINTR);

    if (rlen < 0) {
        ret = -errno;
        VHD_LOG_ERROR("payload read failed: %s", strerror(-ret));
        goto out;
    }
    if ((size_t)rlen != hdr->size) {
        VHD_LOG_ERROR("payload read %zd, expected %d", rlen, hdr->size);
        ret = -EIO;
        goto out;
    }
    ret += rlen;

out:
    if (ret <= 0) {
        *num_fds = 0;
    }
    if (*num_fds > num_fds_rcvd) {
        *num_fds = num_fds_rcvd;
    }
    memcpy(fds, fds_rcvd, *num_fds * sizeof(int));

    for (i = *num_fds; i < num_fds_rcvd; i++) {
        close(fds_rcvd[i]);
    }
    return ret;
}

/*
 * Send message to master. Return number of bytes sent or negative
 * error code in case of error.
 */
static int net_send_msg(int fd, const struct vhost_user_msg_hdr *hdr,
                        const void *payload, int *fds, size_t num_fds)
{
    int ret;
    struct iovec iov[] = {
        {
            .iov_base = (void *)hdr,
            .iov_len = sizeof(*hdr),
        }, {
            .iov_base = (void *)payload,
            .iov_len = hdr->size,
        }
    };
    struct msghdr msgh = {
        .msg_iov = iov,
        .msg_iovlen = 2,
    };
    union {
        char buf[CMSG_SPACE(sizeof(int) * VHOST_USER_MAX_FDS)];
        struct cmsghdr cmsg_align;
    } control;
    struct cmsghdr *cmsgh;

    if (num_fds > VHOST_USER_MAX_FDS) {
        VHD_LOG_ERROR("too many fds: %zu", num_fds);
        return -EMSGSIZE;
    }

    if (num_fds) {
        size_t fdsize = sizeof(*fds) * num_fds;
        msgh.msg_control = &control;
        msgh.msg_controllen = CMSG_SPACE(fdsize);
        cmsgh = CMSG_FIRSTHDR(&msgh);
        cmsgh->cmsg_len = CMSG_LEN(fdsize);
        cmsgh->cmsg_level = SOL_SOCKET;
        cmsgh->cmsg_type = SCM_RIGHTS;
        memcpy(CMSG_DATA(cmsgh), fds, fdsize);
    }

    do {
        ret = sendmsg(fd, &msgh, MSG_NOSIGNAL);
    } while (ret < 0 && errno == EINTR);

    if (ret < 0) {
        ret = -errno;
        VHD_LOG_ERROR("sendmsg: %s", strerror(-ret));
        return ret;
    }
    if ((unsigned)ret != sizeof(*hdr) + hdr->size) {
        VHD_LOG_ERROR("sent %d wanted %zu", ret, sizeof(*hdr) + hdr->size);
        return -EIO;
    }

    return ret;
}

/*
 * Vhost protocol handling
 */

static const uint64_t g_default_features =
    (1UL << VHOST_USER_F_PROTOCOL_FEATURES) |
    (1UL << VHOST_F_LOG_ALL);

static const uint64_t g_default_protocol_features =
    (1UL << VHOST_USER_PROTOCOL_F_MQ) |
    (1UL << VHOST_USER_PROTOCOL_F_LOG_SHMFD) |
    (1UL << VHOST_USER_PROTOCOL_F_REPLY_ACK) |
    (1UL << VHOST_USER_PROTOCOL_F_CONFIG) |
    (1UL << VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD) |
    (1UL << VHOST_USER_PROTOCOL_F_CONFIGURE_MEM_SLOTS);

static inline bool has_feature(uint64_t features_qword, size_t feature_bit)
{
    return features_qword & (1ull << feature_bit);
}

#define NSEC_PER_SEC 1000000000
#define NSEC_PER_MSEC 1000000

static void elapsed_time(struct vhd_vdev *vdev, struct timespec *et)
{
    clock_gettime(CLOCK_MONOTONIC, et);

    if (et->tv_nsec < vdev->msg_handling_started.tv_nsec) {
        et->tv_sec -= 1;
        et->tv_nsec += NSEC_PER_SEC;
    }
    et->tv_sec -= vdev->msg_handling_started.tv_sec;
    et->tv_nsec -= vdev->msg_handling_started.tv_nsec;
}

static void arm_msg_handling_timer(struct vhd_vdev *vdev, int secs)
{
    struct itimerspec itimer = {
        .it_interval.tv_sec = secs,
        .it_value.tv_sec = secs,
    };

    timerfd_settime(vdev->timerfd, 0, &itimer, NULL);
}

/* Every so many seconds report if the message is still being handled */
#define MSG_HANDLING_LOG_INTERVAL 30

#ifdef VHD_DEBUG
#define MSG_ELAPSED_NSEC_LOG_THRESHOLD 0
#else
/*
 * This number must be under 1 sec as that's handled by a different
 * field inside the timespec struct.
 */
#define MSG_ELAPSED_NSEC_LOG_THRESHOLD (500 * NSEC_PER_MSEC)
#endif

static void vdev_handle_start(struct vhd_vdev *vdev, uint32_t req,
                              bool ack_pending)
{
    /* detach timer_handler attached right after accept, no-op after the first request */
    vhd_detach_io_handler(vdev->timer_handler);

    /* do not accept further messages until this one is fully handled */
    vhd_detach_io_handler(vdev->conn_handler);

    vdev->req = req;
    vdev->ack_pending = ack_pending;
    clock_gettime(CLOCK_MONOTONIC, &vdev->msg_handling_started);

    VHD_OBJ_DEBUG(vdev, "%s (%u)", vhost_req_name(req), req);

    arm_msg_handling_timer(vdev, MSG_HANDLING_LOG_INTERVAL);
    vhd_attach_io_handler(vdev->timer_handler);
}

static void vdev_handle_finish(struct vhd_vdev *vdev)
{
    struct timespec elapsed;

    if (vdev->req == VHOST_USER_NONE) {
        return;
    }

    vhd_detach_io_handler(vdev->timer_handler);
    arm_msg_handling_timer(vdev, 0);

    elapsed_time(vdev, &elapsed);

    if (elapsed.tv_sec || elapsed.tv_nsec > MSG_ELAPSED_NSEC_LOG_THRESHOLD) {
        VHD_OBJ_INFO(vdev, "%s (%u): elapsed %jd.%03lds",
                     vhost_req_name(vdev->req), vdev->req,
                     (intmax_t) elapsed.tv_sec,
                     elapsed.tv_nsec / NSEC_PER_MSEC);
    }

    vdev->ack_pending = false;
    vdev->req = VHOST_USER_NONE;

    /* resume accepting further messages if still connected */
    if (vdev->conn_handler) {
        vhd_attach_io_handler(vdev->conn_handler);
    }
}

static int vhost_send_fds(struct vhd_vdev *vdev,
                          const struct vhost_user_msg_hdr *hdr,
                          const void *payload,
                          int *fds, size_t num_fds)
{
    int len;

    len = net_send_msg(vdev->connfd, hdr, payload, fds, num_fds);
    vdev_handle_finish(vdev);
    return len < 0 ? len : 0;
}

static int vhost_reply_fds(struct vhd_vdev *vdev,
                           const void *payload, uint32_t len,
                           int *fds, size_t num_fds)
{
    struct vhost_user_msg_hdr hdr = {
        .req = vdev->req,
        .size = len,
        .flags = VHOST_USER_MSG_FLAGS_REPLY,
    };

    return vhost_send_fds(vdev, &hdr, payload, fds, num_fds);
}

static int vhost_reply(struct vhd_vdev *vdev,
                       const void *payload, uint32_t len)
{
    return vhost_reply_fds(vdev, payload, len, NULL, 0);
}

static int vhost_reply_u64(struct vhd_vdev *vdev, uint64_t u64)
{
    return vhost_reply(vdev, &u64, sizeof(u64));
}

static int vhost_send_vring_base(struct vhd_vring *vring)
{
    struct vhost_user_vring_state vrstate = {
        .index = vring_idx(vring),
        .num = vring->vq.last_avail,
    };

    return vhost_reply(vring->vdev, &vrstate, sizeof(vrstate));
}

static bool msg_ack_needed(struct vhd_vdev *vdev, uint32_t flags)
{
    return has_feature(vdev->negotiated_protocol_features,
                       VHOST_USER_PROTOCOL_F_REPLY_ACK) &&
        (flags & VHOST_USER_MSG_FLAGS_REPLY_ACK);
}

static int vhost_ack(struct vhd_vdev *vdev, int ret)
{
    if (!vdev->ack_pending) {
        vdev_handle_finish(vdev);
        return 0;
    }

    return vhost_reply_u64(vdev, ret);
}

static int vhost_get_protocol_features(struct vhd_vdev *vdev,
                                       const void *payload, size_t size,
                                       const int *fds, size_t num_fds)
{
    if (num_fds) {
        VHD_OBJ_ERROR(vdev, "malformed message num_fds=%zu", num_fds);
        return -EINVAL;
    }

    return vhost_reply_u64(vdev, vdev->supported_protocol_features);
}

static int vhost_set_protocol_features(struct vhd_vdev *vdev,
                                       const void *payload, size_t size,
                                       const int *fds, size_t num_fds)
{
    const uint64_t *features = payload;

    if (num_fds || size < sizeof(*features)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    if (vdev->num_vrings_in_flight) {
        VHD_OBJ_ERROR(vdev, "not allowed once vrings are started");
        return -EISCONN;
    }

    if (*features & ~vdev->supported_protocol_features) {
        VHD_OBJ_ERROR(vdev, "requested unsupported features 0x%" PRIx64,
                      *features & ~vdev->supported_protocol_features);
        return -ENOTSUP;
    }

    vdev->negotiated_protocol_features = *features;

    return vhost_ack(vdev, 0);
}

static int vhost_get_features(struct vhd_vdev *vdev, const void *payload,
                              size_t size, const int *fds, size_t num_fds)
{
    if (num_fds) {
        VHD_OBJ_ERROR(vdev, "malformed message num_fds=%zu", num_fds);
        return -EINVAL;
    }

    vdev->supported_features = g_default_features |
                               vdev->type->get_features(vdev);

    VHD_OBJ_INFO(vdev, "GET_FEATURES: reply with supported_features 0x%" PRIx64,
                 vdev->supported_features);

    return vhost_reply_u64(vdev, vdev->supported_features);
}

static void update_shadow_vq_memlog(struct vhd_vdev *vdev)
{
    uint16_t i;
    struct vhd_memory_log *log =
        has_feature(vdev->negotiated_features, VHOST_F_LOG_ALL) ?
        vdev->memlog : NULL;

    for (i = 0; i < vdev->num_queues; i++) {
        vdev->vrings[i].shadow_vq.log = log;
    }
}

static void vring_sync_to_virtq_bh(void *opaque)
{
    struct vhd_vring *vring = opaque;
    vring_sync_to_virtq(vring);
    vhd_run_in_ctl(vring_mark_msg_handled_bh, vring);
}

static int set_features_complete(struct vhd_vdev *vdev)
{
    return vhost_ack(vdev, 0);
}

static int vhost_set_features(struct vhd_vdev *vdev, const void *payload,
                              size_t size, const int *fds, size_t num_fds)
{
    uint16_t i;
    const uint64_t *features = payload;
    bool has_event_idx = has_feature(*features, VIRTIO_F_RING_EVENT_IDX);
    bool has_vring_enable = has_feature(*features, VHOST_USER_F_PROTOCOL_FEATURES);

    uint64_t supported_features = vdev->supported_features;
    uint64_t changed_features;

    VHD_OBJ_INFO(vdev, "SET_FEATURES: features 0x%" PRIx64, *features);

    if (num_fds || size < sizeof(*features)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    if (*features & ~supported_features) {
        VHD_OBJ_ERROR(vdev, "requested unsupported features 0x%" PRIx64,
                      *features & ~supported_features);
        return -ENOTSUP;
    }

    if (!vdev->num_vrings_in_flight) {
        vdev->negotiated_features = *features;
        for (i = 0; i < vdev->num_queues; i++) {
            vdev->vrings[i].vq.has_event_idx = has_event_idx;
            vdev->vrings[i].shadow_vq.enabled = !has_vring_enable;
            vdev->vrings[i].vq.enabled = vdev->vrings[i].shadow_vq.enabled;
        }
        return set_features_complete(vdev);
    }

    /* only logging may be toggled in a started device */
    changed_features =
        (vdev->negotiated_features ^ *features) & ~(1ull << VHOST_F_LOG_ALL);
    if (changed_features) {
        VHD_OBJ_ERROR(vdev, "changing features 0x%" PRIx64
                      " not allowed once vrings are started",
                      changed_features);
        return -EISCONN;
    }

    vdev->negotiated_features = *features;

    update_shadow_vq_memlog(vdev);

    vdev->handle_complete = set_features_complete;
    for (i = 0; i < vdev->num_queues; i++) {
        vring_handle_msg(&vdev->vrings[i], vring_sync_to_virtq_bh);
    }

    return 0;
}

static int vhost_set_owner(struct vhd_vdev *vdev, const void *payload,
                           size_t size, const int *fds, size_t num_fds)
{
    if (num_fds) {
        VHD_OBJ_ERROR(vdev, "malformed message num_fds=%zu", num_fds);
        return -EINVAL;
    }

    return vhost_ack(vdev, 0);
}

static int set_mem_table_complete(struct vhd_vdev *vdev)
{
    if (vdev->old_memmap) {
        vhd_memmap_unref(vdev->old_memmap);
        vdev->old_memmap = NULL;
    }

    return vhost_ack(vdev, 0);
}

static int vhost_set_mem_table(struct vhd_vdev *vdev, const void *payload,
                               size_t size, const int *fds, size_t num_fds)
{
    int ret;
    const struct vhost_user_mem_desc *desc = payload;
    size_t exp_size = offsetof(struct vhost_user_mem_desc, regions) +
        sizeof(desc->regions[0]) * num_fds;
    struct vhd_memory_map *mm;
    uint16_t i;

    if (size < exp_size) {
        VHD_OBJ_ERROR(vdev, "malformed message: size %zu expected %zu", size,
                      exp_size);
        return -EMSGSIZE;
    }
    if (desc->nregions > VHOST_USER_MEM_REGIONS_MAX) {
        VHD_OBJ_ERROR(vdev, "invalid #memory regions %u", desc->nregions);
        return -EINVAL;
    }
    if (desc->nregions != num_fds) {
        VHD_OBJ_ERROR(vdev, "#memory regions != #fds: %u != %zu",
                      desc->nregions, num_fds);
        return -EINVAL;
    }

    mm = vhd_memmap_new(vdev->map_cb, vdev->unmap_cb);

    for (i = 0; i < desc->nregions; i++) {
        const struct vhost_user_mem_region *region = &desc->regions[i];
        ret = vhd_memmap_add_slot(mm, region->guest_addr, region->user_addr,
                                  region->size, fds[i], region->mmap_offset,
                                  vdev->pte_flush_byte_threshold != 0);
        if (ret < 0) {
            goto fail;
        }
    }

    for (i = 0; i < vdev->num_queues; i++) {
        if (!vdev->vrings[i].started_in_ctl) {
            continue;
        }
        ret = vring_update_shadow_vq_addrs(&vdev->vrings[i], mm);
        if (ret < 0) {
            goto fail;
        }
    }

    vdev->old_memmap = vdev->memmap;
    vdev->memmap = mm;

    if (!vdev->num_vrings_in_flight) {
        return set_mem_table_complete(vdev);
    }

    vdev->handle_complete = set_mem_table_complete;
    for (i = 0; i < vdev->num_queues; i++) {
        vring_handle_msg(&vdev->vrings[i], vring_sync_to_virtq_bh);
    }
    return 0;

fail:
    vhd_memmap_unref(mm);
    return ret;
}

static int vhost_add_mem_reg(struct vhd_vdev *vdev, const void *payload,
                             size_t size, const int *fds, size_t num_fds)
{
    int ret;
    const struct vhost_user_mem_single_mem_desc *desc = payload;
    const struct vhost_user_mem_region *region = &desc->region;
    struct vhd_memory_map *mm = vdev->memmap;
    bool can_add_inplace = false;
    uint16_t i;

    if (size < sizeof(*desc)) {
        VHD_OBJ_ERROR(vdev, "malformed message: size %zu expected %zu",
                      size, sizeof(*desc));
        return -EMSGSIZE;
    }
    if (num_fds != 1) {
        VHD_OBJ_ERROR(vdev, "expected a region file descriptor");
        return -EINVAL;
    }

    if (mm == NULL) {
        mm = vhd_memmap_new(vdev->map_cb, vdev->unmap_cb);
    } else {
        can_add_inplace = vdev->num_vrings_in_flight == 0;

        /*
         * Slow path:
         * The rings are already live, therefore we cannot touch their memory
         * map here. All we can do is create a copy, modify it how we want,
         * and then tell the rings to use it via a message to their event loop.
         */
        if (!can_add_inplace) {
            mm = vhd_memmap_dup(mm);
        }
    }

    ret = vhd_memmap_add_slot(mm, region->guest_addr, region->user_addr,
                              region->size, fds[0], region->mmap_offset,
                              vdev->pte_flush_byte_threshold != 0);
    if (ret < 0) {
        goto fail;
    }

    for (i = 0; i < vdev->num_queues; i++) {
        if (!vdev->vrings[i].started_in_ctl) {
            continue;
        }
        ret = vring_update_shadow_vq_addrs(&vdev->vrings[i], mm);
        if (ret < 0) {
            goto fail;
        }
    }

    /*
     * Fast path:
     * The rings are not yet started, but a valid memory map
     * structure already exists. In this case we can just modify
     * it in-place and return right away without creating a copy
     * of it.
     */
    if (can_add_inplace) {
        return vhost_ack(vdev, 0);
    }

    vdev->old_memmap = vdev->memmap;
    vdev->memmap = mm;

    if (!vdev->num_vrings_in_flight) {
        return set_mem_table_complete(vdev);
    }

    vdev->handle_complete = set_mem_table_complete;
    for (i = 0; i < vdev->num_queues; i++) {
        vring_handle_msg(&vdev->vrings[i], vring_sync_to_virtq_bh);
    }
    return 0;

fail:
    if (!can_add_inplace) {
        vhd_memmap_unref(mm);
    }
    return ret;
}

static int vhost_rem_mem_reg(struct vhd_vdev *vdev, const void *payload,
                             size_t size, const int *fds, size_t num_fds)
{
    int ret;
    const struct vhost_user_mem_single_mem_desc *desc = payload;
    const struct vhost_user_mem_region *region = &desc->region;
    struct vhd_memory_map *new_mm, *mm = vdev->memmap;
    uint16_t i;

    if (size < sizeof(*desc)) {
        VHD_OBJ_ERROR(vdev, "malformed message: size %zu expected %zu",
                      size, sizeof(*desc));
        return -EMSGSIZE;
    }

    /*
     * vhost-user spec:
     * No file descriptors SHOULD be passed in the ancillary data.
     * For compatibility with existing incorrect implementations, the back-end
     * MAY accept messages with one file descriptor. If a file descriptor is
     * passed, the back-end MUST close it without using it otherwise.
     */
    if (num_fds > 1) {
        VHD_OBJ_ERROR(vdev, "malformed message num_fds=%zu", num_fds);
        return -EINVAL;
    }

    if (mm == NULL) {
        VHD_OBJ_ERROR(
            vdev,
            "cannot remove memory region, device doesn't have a memmap"
        );
        return -EINVAL;
    }

    new_mm = vhd_memmap_dup(mm);
    ret = vhd_memmap_del_slot(new_mm, region->guest_addr, region->user_addr,
                              region->size);
    if (ret < 0) {
        goto fail;
    }

    for (i = 0; i < vdev->num_queues; i++) {
        if (!vdev->vrings[i].started_in_ctl) {
            continue;
        }
        ret = vring_update_shadow_vq_addrs(&vdev->vrings[i], new_mm);
        if (ret < 0) {
            goto fail;
        }
    }

    vdev->old_memmap = mm;
    vdev->memmap = new_mm;

    if (!vdev->num_vrings_in_flight) {
        return set_mem_table_complete(vdev);
    }

    vdev->handle_complete = set_mem_table_complete;
    for (i = 0; i < vdev->num_queues; i++) {
        vring_handle_msg(&vdev->vrings[i], vring_sync_to_virtq_bh);
    }
    return 0;

fail:
    vhd_memmap_unref(new_mm);
    return ret;
}

static int pte_flush_complete(struct vhd_vdev *vdev)
{
    if (vdev->old_memmap) {
        vhd_memmap_unref(vdev->old_memmap);
        vdev->old_memmap = NULL;
    }

    catomic_store_release(&vdev->pte_flush_pending, false);
    return vhost_ack(vdev, 0);
}

static void pte_flush_refresh_threshold(struct vhd_vdev *vdev)
{
    catomic_store_release(&vdev->bytes_left_before_pte_flush,
                          vdev->pte_flush_byte_threshold);
}

static void vdev_handle_start(struct vhd_vdev *vdev, uint32_t req,
                              bool ack_pending);

static void flush_vdev_ptes(struct vhd_vdev *vdev)
{
    int64_t bytes_left;
    struct vhd_memory_map *new_memmap;
    size_t i;
    bool any_started = false;

    bytes_left = catomic_load_acquire(&vdev->bytes_left_before_pte_flush);
    if (bytes_left > 0) {
        /*
         * Multiple vrings might have noticed that it's time to flush the PTEs
         * at the same time, this is fine, just skip the duplicated call.
         */
        VHD_OBJ_DEBUG(vdev, "skipping duplicated PTE flush");
        return;
    }

    if (vdev->req != VHOST_USER_NONE) {
        /*
         * Racing vrings might make it here if pte_flush_byte_threshold is
         * set to such a tiny number that they manage to process more bytes
         * than the next threshold after having scheduled the flush call.
         * This is not a problem, just skip silently.
         */
        if (vdev->req == VHOST_INTERNAL_MEMMAP_FLUSH_AFTER_THRESHOLD) {
            // Don't cancel here, control plane will do it itself
            VHD_OBJ_DEBUG(vdev, "a PTE flush is already in-progress, skipping");
            return;
        }

        VHD_OBJ_INFO(vdev, "control plane is busy servicing '%s', "
                     "skipping PTE flush", vhost_req_name(vdev->req));
        goto out_cancel;
    }

    new_memmap = vhd_memmap_dup_remap(vdev->memmap);
    if (new_memmap == NULL) {
        VHD_OBJ_ERROR(vdev, "failed to create a new memory map for PTE flush");
        goto out_cancel;
    }

    for (i = 0; i < vdev->num_queues; i++) {
        if (!vdev->vrings[i].started_in_ctl) {
            continue;
        }

        any_started = true;
        if (vring_update_shadow_vq_addrs(&vdev->vrings[i], new_memmap) < 0) {
            /*
             * We weren't able to resolve the mappings in the new memmap, undo
             * what we just did and skip this flush.
             */
            while (i-- > 0) {
                vring_update_shadow_vq_addrs(&vdev->vrings[i], vdev->memmap);
            }

            vhd_memmap_unref(new_memmap);
            goto out_cancel;
        }
    }

    if (!any_started) {
        VHD_OBJ_INFO(vdev, "no vrings started, skipping PTE flush");
        vhd_memmap_unref(new_memmap);
        goto out_cancel;
    }

    // Block other VHOST requests while we perform the flush
    vdev_handle_start(vdev, VHOST_INTERNAL_MEMMAP_FLUSH_AFTER_THRESHOLD, 0);

    pte_flush_refresh_threshold(vdev);

    VHD_OBJ_INFO(vdev, "performing a PTE flush after processing %zu bytes",
                 vdev->pte_flush_byte_threshold);

    vdev->old_memmap = vdev->memmap;
    vdev->memmap = new_memmap;

    vdev->handle_complete = pte_flush_complete;
    for (i = 0; i < vdev->num_queues; i++) {
        vring_handle_msg(&vdev->vrings[i], vring_sync_to_virtq_bh);
    }

    return;

out_cancel:
    pte_flush_refresh_threshold(vdev);
    catomic_store_release(&vdev->pte_flush_pending, false);
}

static int vhost_get_config(struct vhd_vdev *vdev, const void *payload,
                            size_t size, const int *fds, size_t num_fds)
{
    const struct vhost_user_config_space *config = payload;
    struct vhost_user_config_space reply = {};

    if (num_fds || size < VHOST_CONFIG_HDR_SIZE || size > sizeof(*config)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    if (config->size > size - VHOST_CONFIG_HDR_SIZE) {
        VHD_OBJ_WARN(vdev, "Message size is not enough for requested data");
        reply.size = size - VHOST_CONFIG_HDR_SIZE;
    } else {
        reply.size = config->size;
    }
    reply.offset = config->offset;

    reply.size = vdev->type->get_config(vdev, &reply.payload, reply.size,
                                        reply.offset);

    return vhost_reply(vdev, &reply, size);
}

static int vhost_get_queue_num(struct vhd_vdev *vdev, const void *payload,
                               size_t size, const int *fds, size_t num_fds)
{
    if (num_fds) {
        VHD_OBJ_ERROR(vdev, "malformed message num_fds=%zu", num_fds);
        return -EINVAL;
    }

    return vhost_reply_u64(vdev, vdev->num_queues);
}

static struct vhd_vring *get_vring(struct vhd_vdev *vdev, uint16_t index)
{
    if (index >= vdev->num_queues) {
        VHD_OBJ_ERROR(vdev, "vring %u doesn't exist (max %u)", index,
                      vdev->num_queues - 1);
        return NULL;
    }

    return &vdev->vrings[index];
}

static struct vhd_vring *msg_u64_get_vring(struct vhd_vdev *vdev,
                                           const void *payload,
                                           size_t size, size_t num_fds)
{
    const uint64_t *u64 = payload;
    uint8_t vring_idx;
    bool has_fd;

    if (size < sizeof(*u64)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu", size);
        return NULL;
    }

    has_fd = !(*u64 & VHOST_VRING_INVALID_FD);
    vring_idx = *u64 & VHOST_VRING_IDX_MASK;

    if (num_fds != has_fd) {
        VHD_OBJ_ERROR(vdev, "unexpected #fds: %zu (expected %u)", num_fds,
                      has_fd);
        return NULL;
    }

    return get_vring(vdev, vring_idx);
}

static int set_vring_call_complete(struct vhd_vdev *vdev)
{
    replace_fd(&vdev->keep_fd, -1);

    return vhost_ack(vdev, 0);
}

static int vhost_set_vring_call(struct vhd_vdev *vdev, const void *payload,
                                size_t size, const int *fds, size_t num_fds)
{
    struct vhd_vring *vring = msg_u64_get_vring(vdev, payload, size, num_fds);
    int callfd = -1;

    if (!vring) {
        return -EINVAL;
    }

    if (num_fds > 0) {
        callfd = fcntl(fds[0], F_DUPFD_CLOEXEC, 0);
        if (callfd < 0) {
            int ret = -errno;
            VHD_OBJ_ERROR(vring, "fcntl(F_DUPFD_CLOEXEC): %s", strerror(-ret));
            return ret;
        }
    }

    /*
     * The current callfd may still be in use in the dataplane so we have to
     * keep it open until it's synced to the dataplane copy.
     */
    VHD_ASSERT(vdev->keep_fd == -1);
    vdev->keep_fd = vring->callfd;
    vring->callfd = callfd;

    if (!vring->started_in_ctl) {
        int ret = set_vring_call_complete(vdev);
        if (ret < 0) {
            replace_fd(&vring->callfd, -1);
        }
        return ret;
    }

    vdev->handle_complete = set_vring_call_complete;
    vring_handle_msg(vring, vring_sync_to_virtq_bh);
    return 0;
}

static int set_vring_kick_complete(struct vhd_vdev *vdev)
{
    return vhost_ack(vdev, 0);
}

static int set_vring_kick_fail_complete(struct vhd_vdev *vdev)
{
    return vhost_ack(vdev, -EIO);
}

static void vring_start_failed_bh(void *opaque)
{
    struct vhd_vring *vring = opaque;
    struct vhd_vdev *vdev = vring->vdev;

    vdev->handle_complete = set_vring_kick_fail_complete;

    vring_mark_msg_handled(vring);
    vring_mark_stopped(vring);
    vring_mark_drained(vring);
}

static void vring_start_bh(void *opaque)
{
    struct vhd_vring *vring = opaque;
    VHD_ASSERT(!vring->started_in_rq);

    /*
     * If vring_stop_bh from vdev_disconnect gets reordered with
     * vring_start_bh, do not start the vring as the device is going down.
     */
    if (vring->disconnecting) {
        goto fail;
    }

    vring->kick_handler = vhd_add_rq_io_handler(vhd_get_rq_for_vring(vring),
                                                vring->kickfd,
                                                vring_kick, vring);
    if (!vring->kick_handler) {
        VHD_OBJ_ERROR(vring, "Could not attach kick handler");
        goto fail;
    }

    vring_sync_to_virtq(vring);
    vring->started_in_rq = true;
    vhd_run_in_ctl(vring_mark_msg_handled_bh, vring);
    return;

fail:
    vhd_run_in_ctl(vring_start_failed_bh, vring);
}

static int vhost_set_vring_kick(struct vhd_vdev *vdev, const void *payload,
                                size_t size, const int *fds, size_t num_fds)
{
    struct vhd_vring *vring = msg_u64_get_vring(vdev, payload, size, num_fds);
    int ret;
    int kickfd;

    if (!vring) {
        return -EINVAL;
    }
    if (num_fds == 0) {
        VHD_OBJ_ERROR(vring, "vring polling mode is not supported");
        return -ENOTSUP;
    }
    if (vring->started_in_ctl) {
        VHD_OBJ_ERROR(vring, "vring is already started");
        return -EISCONN;
    }

    ret = vring_update_shadow_vq_addrs(vring, vdev->memmap);
    if (ret < 0) {
        return ret;
    }

    kickfd = fcntl(fds[0], F_DUPFD_CLOEXEC, 0);
    if (kickfd < 0) {
        ret = -errno;
        VHD_OBJ_ERROR(vring, "fcntl(F_DUPFD_CLOEXEC): %s", strerror(-ret));
        return ret;
    }

    VHD_ASSERT(vring->kickfd < 0);
    vring->kickfd = kickfd;

    vring_sync_to_virtq(vring);
    vring->vq.log_tag = vring->log_tag;
    virtio_virtq_init(&vring->vq);

    vring->started_in_ctl = true;
    vdev->num_vrings_started++;
    vdev->num_vrings_in_flight++;

    vdev->handle_complete = set_vring_kick_complete;
    vring_handle_msg(vring, vring_start_bh);
    return 0;
}

static int set_vring_err_complete(struct vhd_vdev *vdev)
{
    replace_fd(&vdev->keep_fd, -1);

    return vhost_ack(vdev, 0);
}

static int vhost_set_vring_err(struct vhd_vdev *vdev, const void *payload,
                               size_t size, const int *fds, size_t num_fds)
{
    struct vhd_vring *vring = msg_u64_get_vring(vdev, payload, size, num_fds);
    int errfd = -1;

    if (!vring) {
        return -EINVAL;
    }

    if (num_fds > 0) {
        errfd = fcntl(fds[0], F_DUPFD_CLOEXEC, 0);
        if (errfd < 0) {
            int ret = -errno;
            VHD_OBJ_ERROR(vring, "fcntl(F_DUPFD_CLOEXEC): %s", strerror(-ret));
            return ret;
        }
    }

    /*
     * The current errfd may still be in use in the dataplane so we have to
     * keep it open until it's synced to the dataplane copy.
     */
    VHD_ASSERT(vdev->keep_fd == -1);
    vdev->keep_fd = vring->errfd;
    vring->errfd = errfd;

    if (!vring->started_in_ctl) {
        int ret = set_vring_err_complete(vdev);
        if (ret < 0) {
            replace_fd(&vring->errfd, -1);
        }
        return ret;
    }

    vdev->handle_complete = set_vring_err_complete;
    vring_handle_msg(vring, vring_sync_to_virtq_bh);
    return 0;
}

static int vhost_set_vring_num(struct vhd_vdev *vdev, const void *payload,
                               size_t size, const int *fds, size_t num_fds)
{
    const struct vhost_user_vring_state *vrstate = payload;
    struct vhd_vring *vring;

    if (num_fds || size < sizeof(*vrstate)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    vring = get_vring(vdev, vrstate->index);
    if (!vring) {
        return -EINVAL;
    }

    if (vring->started_in_ctl) {
        VHD_OBJ_ERROR(vring, "vring is already started");
        return -EISCONN;
    }

    vring->vq.qsz = vrstate->num;
    return vhost_ack(vdev, 0);
}

static int vhost_set_vring_base(struct vhd_vdev *vdev, const void *payload,
                                size_t size, const int *fds, size_t num_fds)
{
    const struct vhost_user_vring_state *vrstate = payload;
    struct vhd_vring *vring;

    if (num_fds || size < sizeof(*vrstate)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    vring = get_vring(vdev, vrstate->index);
    if (!vring) {
        return -EINVAL;
    }

    if (vring->started_in_ctl) {
        VHD_OBJ_ERROR(vring, "vring is already started");
        return -EISCONN;
    }

    vring->vq.last_avail = vrstate->num;
    return vhost_ack(vdev, 0);
}

static int vhost_get_vring_base(struct vhd_vdev *vdev, const void *payload,
                                size_t size, const int *fds, size_t num_fds)
{
    const struct vhost_user_vring_state *vrstate = payload;
    struct vhd_vring *vring;

    if (num_fds || size < sizeof(*vrstate)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    vring = get_vring(vdev, vrstate->index);
    if (!vring) {
        return -EINVAL;
    }

    if (!vring->started_in_ctl) {
        return vhost_send_vring_base(vring);
    }

    /*
     * This command is special as it needs to wait for drain, not just until
     * the message is handled in rq.  Mark this in the vring and submit
     * vring_stop_bh() instead of going through vring_handle_msg().
     */
    vring->on_drain_cb = vhost_send_vring_base;
    vhd_run_in_rq(vhd_get_rq_for_vring(vring), vring_stop_bh, vring);
    return 0;
}

static int set_vring_addr_complete(struct vhd_vdev *vdev)
{
    return vhost_ack(vdev, 0);
}

static int vhost_set_vring_addr(struct vhd_vdev *vdev, const void *payload,
                                size_t size, const int *fds, size_t num_fds)
{
    const struct vhost_user_vring_addr *vraddr = payload;
    struct vhd_vring *vring;

    if (num_fds || size < sizeof(*vraddr)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    vring = get_vring(vdev, vraddr->index);
    if (!vring) {
        return -EINVAL;
    }

    if (!vring->started_in_ctl) {
        vring->shadow_vq.desc_addr = vraddr->desc_addr;
        vring->shadow_vq.used_addr = vraddr->used_addr;
        vring->shadow_vq.avail_addr = vraddr->avail_addr;
        vring->shadow_vq.flags = vraddr->flags;
        vring->shadow_vq.used_gpa_base = vraddr->used_gpa_base;

        return set_vring_addr_complete(vdev);
    }

    if (vring->shadow_vq.desc_addr != vraddr->desc_addr ||
        vring->shadow_vq.used_addr != vraddr->used_addr ||
        vring->shadow_vq.avail_addr != vraddr->avail_addr ||
        vring->shadow_vq.used_gpa_base != vraddr->used_gpa_base) {
        VHD_OBJ_ERROR(vring, "changing started vring addresses not allowed");
        return -EISCONN;
    }

    vring->shadow_vq.flags = vraddr->flags;

    vdev->handle_complete = set_vring_addr_complete;
    vring_handle_msg(vring, vring_sync_to_virtq_bh);
    return 0;
}

static int set_log_base_complete(struct vhd_vdev *vdev)
{
    if (vdev->old_memlog) {
        vhd_free(vdev->old_memlog);
        vdev->old_memlog = NULL;
    }

    return vhost_reply_u64(vdev, 0);
}

static int vhost_set_log_base(struct vhd_vdev *vdev, const void *payload,
                              size_t size, const int *fds, size_t num_fds)
{
    uint16_t i;
    struct vhd_memory_log *memlog;
    const struct vhost_user_log *log = payload;

    if (num_fds != 1 || size < sizeof(*log)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    memlog = vhd_memlog_new(log->size, fds[0], log->offset);
    if (!memlog) {
        return -EFAULT;
    }

    vdev->old_memlog = vdev->memlog;
    vdev->memlog = memlog;

    update_shadow_vq_memlog(vdev);

    if (!vdev->num_vrings_in_flight) {
        return set_log_base_complete(vdev);
    }

    vdev->handle_complete = set_log_base_complete;
    for (i = 0; i < vdev->num_queues; i++) {
        vring_handle_msg(&vdev->vrings[i], vring_sync_to_virtq_bh);
    }

    return 0;
}

static void inflight_mem_init(void *buf, size_t queue_region_size,
                              uint16_t num_queues, uint16_t queue_size)
{
    uint16_t i;

    memset(buf, 0,  num_queues * queue_region_size);
    for (i = 0; i < num_queues; i++) {
        struct inflight_split_region *region = buf + i * queue_region_size;
        region->version = 1;
        region->desc_num = queue_size;
    }
}

static void inflight_mem_cleanup(struct vhd_vdev *vdev)
{
    if (!vdev->inflight_mem) {
        return;
    }

    munmap(vdev->inflight_mem, vdev->inflight_size);
    vdev->inflight_mem = NULL;
}

static int inflight_mmap_region(struct vhd_vdev *vdev, int fd,
                                size_t queue_region_size, uint16_t num_queues)
{
    size_t mmap_size = queue_region_size * num_queues;
    int ret;
    void *buf;
    uint16_t i;

    inflight_mem_cleanup(vdev);

    buf = mmap(NULL, mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (buf == MAP_FAILED) {
        ret = -errno;
        VHD_OBJ_ERROR(vdev, "mmap(%d, %zu): %s", fd, mmap_size,
                      strerror(-ret));
        return ret;
    }

    if (vdev->num_queues < num_queues) {
        num_queues = vdev->num_queues;
    }

    for (i = 0; i < num_queues; i++) {
        vdev->vrings[i].vq.inflight_region = buf + i * queue_region_size;
    }

    vdev->inflight_mem = buf;
    vdev->inflight_size = mmap_size;

    return 0;
}

/* memfd_create is only present since glibc-2.27 */
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC                0x0001U

static int memfd_create(const char *name, unsigned int flags)
{
    return syscall(__NR_memfd_create, name, flags);
}
#endif

static int vhost_get_inflight_fd(struct vhd_vdev *vdev, const void *payload,
                                 size_t size, const int *fds, size_t num_fds)
{
    const struct vhost_user_inflight_desc *idesc = payload;
    struct vhost_user_inflight_desc reply = {};
    size_t queue_region_size = vring_inflight_buf_size(idesc->queue_size);
    size_t mmap_size = queue_region_size * idesc->num_queues;
    int fd;
    int ret;

    if (num_fds || size < sizeof(*idesc)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    if (vdev->num_vrings_in_flight) {
        VHD_OBJ_ERROR(vdev, "not allowed once vrings are started");
        return -EISCONN;
    }

    fd = memfd_create("vhost_get_inflight_fd", MFD_CLOEXEC);
    if (fd == -1) {
        ret = -errno;
        VHD_OBJ_ERROR(vdev, "memfd_create: %s", strerror(-ret));
        return ret;
    }
    ret = ftruncate(fd, mmap_size);
    if (ret == -1) {
        ret = -errno;
        VHD_OBJ_ERROR(vdev, "ftruncate(memfd, %zu): %s", mmap_size,
                      strerror(-ret));
        goto out;
    }
    ret = inflight_mmap_region(vdev, fd, queue_region_size, idesc->num_queues);
    if (ret) {
        goto out;
    }

    inflight_mem_init(vdev->inflight_mem, queue_region_size, idesc->num_queues,
                      idesc->queue_size);

    /* Prepare reply to the master side. */
    reply.mmap_size = vdev->inflight_size;

    ret = vhost_reply_fds(vdev, &reply, sizeof(reply), &fd, 1);
out:
    close(fd);
    return ret;
}

static int vhost_set_inflight_fd(struct vhd_vdev *vdev, const void *payload,
                                 size_t size, const int *fds, size_t num_fds)
{
    const struct vhost_user_inflight_desc *idesc = payload;
    size_t queue_region_size = vring_inflight_buf_size(idesc->queue_size);
    int ret;

    if (num_fds != 1 || size < sizeof(*idesc)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    if (vdev->num_vrings_in_flight) {
        VHD_OBJ_ERROR(vdev, "not allowed once vrings are started");
        return -EISCONN;
    }

    /* we never create inflight with non-zero mmap_offset */
    if (idesc->mmap_offset) {
        VHD_OBJ_ERROR(vdev, "non-zero mmap offset: %lx", idesc->mmap_offset);
        return -EINVAL;
    }

    if (idesc->mmap_size != queue_region_size * idesc->num_queues) {
        VHD_OBJ_ERROR(vdev,
                      "invalid inflight region dimensions: %zu != %zu * %u",
                      idesc->mmap_size, queue_region_size, idesc->num_queues);
        return -EINVAL;
    }

    ret = inflight_mmap_region(vdev, fds[0], queue_region_size,
                               idesc->num_queues);
    if (ret < 0) {
        return ret;
    }

    return vhost_ack(vdev, 0);
}

static int vhost_get_max_mem_slots(struct vhd_vdev *vdev, const void *payload,
                                   size_t size, const int *fds, size_t num_fds)
{
    if (num_fds) {
        VHD_OBJ_ERROR(vdev, "malformed message num_fds=%zu", num_fds);
        return -EINVAL;
    }

    return vhost_reply_u64(vdev, vhd_memmap_max_memslots());
}

static int vring_enable_complete(struct vhd_vdev *vdev)
{
    return vhost_ack(vdev, 0);
}

static int vhost_vring_enable(struct vhd_vdev *vdev, const void *payload,
                              size_t size, const int *fds, size_t num_fds)
{
    const struct vhost_user_vring_state *vrstate = payload;
    struct vhd_vring *vring;

    if (num_fds || size < sizeof(*vrstate)) {
        VHD_OBJ_ERROR(vdev, "malformed message size=%zu #fds=%zu", size,
                      num_fds);
        return -EINVAL;
    }

    vring = get_vring(vdev, vrstate->index);
    if (!vring) {
        return -EINVAL;
    }

    if (!has_feature(vdev->negotiated_features,
                     VHOST_USER_F_PROTOCOL_FEATURES)) {
        VHD_OBJ_ERROR(vring, "tried to SET_VRING_ENABLE without negotiating "
                             "VHOST_USER_F_PROTOCOL_FEATURES");
        return -EINVAL;
    }

    vring->shadow_vq.enabled = !vring->shadow_vq.enabled;
    VHD_OBJ_INFO(vdev, "changing vring %" PRIu32 " state to %s", vrstate->index,
                 vring->shadow_vq.enabled ? "enabled" : "disabled");

    if (!vring->started_in_ctl) {
        return vring_enable_complete(vdev);
    }

    vdev->handle_complete = vring_enable_complete;
    vring_handle_msg(vring, vring_sync_to_virtq_bh);
    return 0;
}

static int (*vhost_msg_handlers[])(struct vhd_vdev *vdev,
                                   const void *payload, size_t size,
                                   const int *fds, size_t num_fds) = {
    [VHOST_USER_GET_FEATURES]           = vhost_get_features,
    [VHOST_USER_SET_FEATURES]           = vhost_set_features,
    [VHOST_USER_SET_OWNER]              = vhost_set_owner,
    [VHOST_USER_GET_PROTOCOL_FEATURES]  = vhost_get_protocol_features,
    [VHOST_USER_SET_PROTOCOL_FEATURES]  = vhost_set_protocol_features,
    [VHOST_USER_GET_CONFIG]             = vhost_get_config,
    [VHOST_USER_SET_MEM_TABLE]          = vhost_set_mem_table,
    [VHOST_USER_GET_QUEUE_NUM]          = vhost_get_queue_num,
    [VHOST_USER_SET_LOG_BASE]           = vhost_set_log_base,
    [VHOST_USER_SET_VRING_CALL]         = vhost_set_vring_call,
    [VHOST_USER_SET_VRING_KICK]         = vhost_set_vring_kick,
    [VHOST_USER_SET_VRING_ERR]          = vhost_set_vring_err,
    [VHOST_USER_SET_VRING_NUM]          = vhost_set_vring_num,
    [VHOST_USER_SET_VRING_BASE]         = vhost_set_vring_base,
    [VHOST_USER_GET_VRING_BASE]         = vhost_get_vring_base,
    [VHOST_USER_SET_VRING_ADDR]         = vhost_set_vring_addr,
    [VHOST_USER_GET_INFLIGHT_FD]        = vhost_get_inflight_fd,
    [VHOST_USER_SET_INFLIGHT_FD]        = vhost_set_inflight_fd,
    [VHOST_USER_GET_MAX_MEM_SLOTS]      = vhost_get_max_mem_slots,
    [VHOST_USER_ADD_MEM_REG]            = vhost_add_mem_reg,
    [VHOST_USER_REM_MEM_REG]            = vhost_rem_mem_reg,
    [VHOST_USER_SET_VRING_ENABLE]       = vhost_vring_enable,
};

static inline void log_vhost_request_failed(struct vhd_vdev *vdev, uint32_t req,
                                            const void *payload, size_t size,
                                            int ret)
{
#define CHARS_PER_BYTE 3 // hex number + space
#define MAX_BYTES_TO_DUMP 100
    char payload_hex[CHARS_PER_BYTE * MAX_BYTES_TO_DUMP + 1];
    size_t payload_bytes_to_dump;

    payload_bytes_to_dump = MIN(MAX_BYTES_TO_DUMP, size);

    for (size_t i = 0; i < payload_bytes_to_dump; ++i) {
        sprintf(payload_hex + CHARS_PER_BYTE * i, "%02X ", ((char*)payload)[i]);
    }
    payload_hex[CHARS_PER_BYTE * payload_bytes_to_dump] = '\0';
#undef MAX_BYTES_TO_DUMP
#undef CHARS_PER_BYTE

    VHD_OBJ_ERROR(vdev, "%s (%u) request failed: %s, request payload (%zu bytes): %s",
                  vhost_req_name(req), req, strerror(-ret), size, payload_hex);
}

static int vhost_handle_msg(struct vhd_vdev *vdev, uint32_t req,
                            const void *payload, size_t size,
                            const int *fds, size_t num_fds)
{
    int ret;

    if (req >= sizeof(vhost_msg_handlers) / sizeof(vhost_msg_handlers[0]) ||
        !vhost_msg_handlers[req]) {
        VHD_OBJ_WARN(vdev, "%s (%u) not supported", vhost_req_name(req), req);
        return -ENOTSUP;
    }

    ret = vhost_msg_handlers[req](vdev, payload, size, fds, num_fds);
    if (ret < 0) {
        log_vhost_request_failed(vdev, req, payload, size, ret);
    }

    return ret;
}

struct vdev_work {
    struct vhd_vdev *vdev;
    void (*func)(struct vhd_vdev *, void *);
    void *opaque;
};

static void vdev_complete_work(struct vhd_vdev *vdev, int ret)
{
    if (vdev->work) {
        vhd_complete_work(vdev->work, ret);
        vdev->work = NULL;
    }
}

static void vdev_work_fn(struct vhd_work *work, void *opaque)
{
    struct vdev_work *vd_work = opaque;
    struct vhd_vdev *vdev = vd_work->vdev;

    /* allow no concurrent work */
    if (vdev->work != NULL) {
        vhd_complete_work(work, -EBUSY);
        return;
    }

    vdev->work = work;
    vd_work->func(vdev, vd_work->opaque);
}

static int vdev_submit_work_and_wait(struct vhd_vdev *vdev,
                                     void (*func)(struct vhd_vdev *, void *),
                                     void *opaque)
{
    struct vdev_work vd_work = {
        .vdev = vdev,
        .func = func,
        .opaque = opaque,
    };

    return vhd_submit_ctl_work_and_wait(vdev_work_fn, &vd_work);
}

static void vdev_cleanup(struct vhd_vdev *vdev)
{
    uint16_t i;

    VHD_ASSERT(!vdev->num_vrings_handling_msg);
    VHD_ASSERT(vdev->req == VHOST_USER_NONE);
    VHD_ASSERT(!vdev->old_memmap);
    VHD_ASSERT(!vdev->old_memlog);
    VHD_ASSERT(vdev->keep_fd == -1);

    for (i = 0; i < vdev->num_queues; i++) {
        vring_reset(&vdev->vrings[i]);
    }

    inflight_mem_cleanup(vdev);

    if (vdev->memmap) {
        vhd_memmap_unref(vdev->memmap);
        vdev->memmap = NULL;
    }

    if (vdev->memlog) {
        vhd_memlog_free(vdev->memlog);
        vdev->memlog = NULL;
    }

    if (vdev->timer_handler) {
        vhd_del_io_handler(vdev->timer_handler);
        vdev->timer_handler = NULL;
        replace_fd(&vdev->timerfd, -1);
    }

    /*
     * Closing the connection should go last, so that the client doesn't see
     * the need to reconnect until the server detaches from the client's
     * mappings.
     */
     replace_fd(&vdev->connfd, -1);
}

static void vhd_vdev_release(struct vhd_vdev *vdev)
{
    uint16_t i;

    LIST_REMOVE(vdev, vdev_list);

    for (i = 0; i < vdev->num_queues; i++) {
        vhd_free(vdev->vrings[i].log_tag);
    }
    vhd_free(vdev->vrings);
    vhd_free(vdev->rqs);

    if (vdev->release_cb) {
        vdev->release_cb(vdev->release_arg);
    }

    vhd_free(vdev->log_tag);
    vdev->type->free(vdev);
}

static void vdev_disconnect(struct vhd_vdev *vdev)
{
    uint16_t i;

    /* prevent double disconnect on error paths */
    if (!vdev->conn_handler) {
        return;
    }

    VHD_OBJ_INFO(vdev, "Close connection with client, sock = %d", vdev->connfd);

    /*
     * Stop processing further requests from the client but postpone closing
     * the socket until drained.  The client doesn't expect us to touch its
     * memory once the control connection is closed.
     */
    vhd_del_io_handler(vdev->conn_handler);
    vdev->conn_handler = NULL;

    for (i = 0; i < vdev->num_queues; i++) {
        vring_disconnect(&vdev->vrings[i]);
    }

    vdev_maybe_finished(vdev);
}

/*
 * Read a vhost-user message and begin handling it.  Suspend reading further
 * messages until the current one is finished processing and the reply is sent
 * back, if necessary.
 */
static int conn_read(void *opaque)
{
    struct vhd_vdev *vdev = opaque;
    struct vhost_user_msg_hdr hdr;
    union vhost_user_msg_payload payload;
    int fds[VHOST_USER_MAX_FDS];
    size_t num_fds = VHOST_USER_MAX_FDS;
    int ret;

    if (net_recv_msg(vdev->connfd, &hdr, &payload, sizeof(payload),
                     fds, &num_fds) <= 0) {
        goto recv_fail;
    }

    vdev_handle_start(vdev, hdr.req, msg_ack_needed(vdev, hdr.flags));

    ret = vhost_handle_msg(vdev, hdr.req, &payload, hdr.size, fds, num_fds);

    while (num_fds--) {
        close(fds[num_fds]);
    }

    if (ret < 0) {
        goto handle_fail;
    }

    return 0;
handle_fail:
    vdev_handle_finish(vdev);
recv_fail:
    vdev_disconnect(vdev);
    return 0;
}

/*
 * Timer handler to log excessively long message handling.
 */
static int timer_read(void *opaque)
{
    struct vhd_vdev *vdev = opaque;
    struct timespec elapsed;
    uint64_t count;

    /* Read the count to rearm the periodic timer, but ignore the result */
    (void)read(vdev->timerfd, &count, sizeof(count));

    elapsed_time(vdev, &elapsed);

    if (likely(vdev->req != VHOST_USER_NONE)) {
        VHD_OBJ_WARN(vdev, "long processing %s (%u): elapsed %jd.%03lds",
                     vhost_req_name(vdev->req), vdev->req,
                     (intmax_t)elapsed.tv_sec, elapsed.tv_nsec / NSEC_PER_MSEC);
    } else {
        VHD_OBJ_WARN(vdev, "Still waiting for a vhost-user request...");
    }

    return 0;
}

/*
 * Accept a client connection and suspend accepting further connections until
 * the current client is disconnected.
 */
static int server_read(void *opaque)
{
    struct vhd_vdev *vdev = opaque;
    int connfd, timerfd;
    struct vhd_io_handler *conn_handler, *timer_handler;

    VHD_ASSERT(vdev->connfd < 0);

    connfd = accept4(vdev->listenfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (connfd == -1) {
        VHD_OBJ_ERROR(vdev, "accept: %s", strerror(errno));
        return 0;
    }

    conn_handler = vhd_add_vhost_io_handler(connfd, conn_read, vdev);
    if (!conn_handler) {
        goto close_client;
    }

    timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timerfd == -1) {
        VHD_OBJ_ERROR(vdev, "timerfd_create: %s", strerror(errno));
        goto del_conn_handler;
    }

    timer_handler = vhd_add_vhost_io_handler(timerfd, timer_read, vdev);
    if (!timer_handler) {
        goto close_timer;
    }

    vhd_detach_io_handler(vdev->listen_handler);

    vdev->connfd = connfd;
    vdev->conn_handler = conn_handler;
    vdev->timerfd = timerfd;
    vdev->timer_handler = timer_handler;
    vdev->negotiated_features = 0;
    vdev->negotiated_protocol_features = 0;
    VHD_OBJ_INFO(vdev, "Connection established, sock = %d", connfd);

    arm_msg_handling_timer(vdev, MSG_HANDLING_LOG_INTERVAL);

    return 0;

close_timer:
    close(timerfd);
del_conn_handler:
    vhd_del_io_handler(conn_handler);
close_client:
    close(connfd);
    return 0;
}

static int vdev_start_listening(struct vhd_vdev *vdev)
{
    vdev->listen_handler = vhd_add_vhost_io_handler(vdev->listenfd,
                                                    server_read, vdev);
    if (!vdev->listen_handler) {
        return -EIO;
    }

    return 0;
}

static void vdev_stop_listening(struct vhd_vdev *vdev)
{
    vhd_del_io_handler(vdev->listen_handler);
    vdev->listen_handler = NULL;
    replace_fd(&vdev->listenfd, -1);
}

/*
 * Action to perform when all vrings in the device acknowledged disconnection.
 */
static void vdev_vrings_stopped(struct vhd_vdev *vdev)
{
    /* vdev is being shut down */
    if (!vdev->listen_handler) {
        /* there must be a work pending completion */
        vdev_complete_work(vdev, 0);
    }
}

/*
 * Action to perform when the device is fully drained, i.e. when it's finished
 * handling all dataplane requests and all control messages and no longer
 * connected to a client.
 */
static void vdev_drained(struct vhd_vdev *vdev)
{
    vdev_cleanup(vdev);

    /* vdev is being shut down */
    if (!vdev->listen_handler) {
        vhd_vdev_release(vdev);
    } else {
        /* resume listening */
        if (vhd_attach_io_handler(vdev->listen_handler) < 0) {
            /* no useful action beside putting an error message */
            VHD_OBJ_ERROR(vdev, "failed to resume listening");
        }
    }
}

/* TODO: properly destroy server on close */
static int sock_create_server(const char *path)
{
    int fd;
    int ret;
    struct sockaddr_un sockaddr = {
        .sun_family = AF_UNIX,
    };

    if (strlen(path) >= sizeof(sockaddr.sun_path)) {
        VHD_LOG_ERROR("%s exceeds max size %zu", path,
                      sizeof(sockaddr.sun_path));
        return -EINVAL;
    }
    strcpy(sockaddr.sun_path, path);

    if (unlink(path) < 0 && errno != ENOENT) {
        ret = -errno;
        VHD_LOG_ERROR("unlink(%s): %s", path, strerror(-ret));
        return ret;
    }

    fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        ret = -errno;
        VHD_LOG_ERROR("socket: %s", strerror(-ret));
        return ret;
    }

    if (bind(fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) < 0) {
        ret = -errno;
        VHD_LOG_ERROR("bind(%s): %s", path, strerror(-ret));
        goto close_fd;
    }

    if (listen(fd, 1) < 0) {
        ret = -errno;
        VHD_LOG_ERROR("listen(%s): %s", path, strerror(-ret));
        goto close_fd;
    }

    return fd;

close_fd:
    close(fd);
    return ret;
}

static void vdev_start(struct vhd_vdev *vdev, void *opaque)
{
    int ret;

    ret = vdev_start_listening(vdev);

    vdev_complete_work(vdev, ret);
}

int vhd_vdev_init_server(
    struct vhd_vdev *vdev,
    const char *socket_path,
    const struct vhd_vdev_type *type,
    int max_queues,
    struct vhd_request_queue **rqs,
    int num_rqs,
    void *priv,
    int (*map_cb)(void *addr, size_t len),
    int (*unmap_cb)(void *addr, size_t len),
    size_t pte_flush_byte_threshold)
{
    int ret;
    int listenfd;
    uint16_t i;
    struct vhd_request_queue **vhd_rqs;

    /*
     * The spec is unclear about the maximum number of queues allowed, using
     * different types for the vring index in different messages.  The most
     * limiting appear to VHOST_USER_SET_VRING_{CALL,ERR,KICK}, which allow
     * only 8 bits for the vring index.
     */
    if (max_queues > VHOST_VRING_IDX_MASK + 1) {
        VHD_LOG_ERROR("%s: %d queues is too many", socket_path, max_queues);
        return -1;
    }

    if (num_rqs < 1 || num_rqs > VHD_MAX_REQUEST_QUEUES ||
        num_rqs > max_queues) {
        VHD_LOG_ERROR("%s: invalid number of requests queues: %d",
                      socket_path, num_rqs);
        return -1;
    }
    if ((max_queues % num_rqs) != 0) {
        VHD_LOG_WARN("%s: max_queues %d is not aligned to num_rqs %d, "
                     "expect uneven request distribution", socket_path,
                     max_queues, num_rqs);
    }
    VHD_ASSERT(rqs);

    listenfd = sock_create_server(socket_path);
    if (listenfd < 0) {
        return -1;
    }

    vhd_rqs = vhd_alloc(num_rqs * sizeof(*rqs));
    memcpy(vhd_rqs, rqs, num_rqs * sizeof(*rqs));

    *vdev = (struct vhd_vdev) {
        .priv = priv,
        .type = type,
        .listenfd = listenfd,
        .connfd = -1,
        .req = VHOST_USER_NONE,
        .rqs = vhd_rqs,
        .num_rqs = num_rqs,
        .map_cb = map_cb,
        .unmap_cb = unmap_cb,
        .pte_flush_byte_threshold = pte_flush_byte_threshold,
        .bytes_left_before_pte_flush = pte_flush_byte_threshold,
        .supported_protocol_features = g_default_protocol_features,
        .num_queues = max_queues,
        .keep_fd = -1,
    };

    vdev->log_tag = vhd_strdup(socket_path);

    vdev->vrings = vhd_calloc(vdev->num_queues, sizeof(vdev->vrings[0]));
    for (i = 0; i < vdev->num_queues; i++) {
        vdev->vrings[i] = (struct vhd_vring) {
            .vdev = vdev,
            .log_tag = vhd_strdup_printf("%s[%u]", socket_path, i),
            .callfd = -1,
            .kickfd = -1,
            .errfd = -1,
        };
    }

    LIST_INSERT_HEAD(&g_vdevs, vdev, vdev_list);

    ret = vdev_submit_work_and_wait(vdev, vdev_start, NULL);
    if (ret != 0) {
        vhd_vdev_release(vdev);
    }

    return ret;
}

struct vdev_stop_work {
    void (*release_cb)(void *);
    void *release_arg;
};

static void vdev_stop(struct vhd_vdev *vdev, void *opaque)
{
   struct vdev_stop_work *work = opaque;

   vdev->release_cb = work->release_cb;
   vdev->release_arg = work->release_arg;

   vdev_stop_listening(vdev);

    /*
     * If a client was connected initiate full-fledged disconnect with stopping
     * vrings.  The stop work completion will be signaled asynchronously once
     * all vrings accept the stop signal.  The release callback will be called
     * even later once all vrings are finished draining.
     */
    if (vdev->conn_handler) {
        vdev_disconnect(vdev);
        return;
    }

    vdev_maybe_finished(vdev);
}

int vhd_vdev_stop_server(struct vhd_vdev *vdev,
                         void (*release_cb)(void *), void *release_arg)
{
    int ret;
    struct vdev_stop_work work = {
        .release_cb = release_cb,
        .release_arg = release_arg
    };

    ret = vdev_submit_work_and_wait(vdev, vdev_stop, &work);
    if (ret < 0) {
        VHD_OBJ_ERROR(vdev, "%s", strerror(-ret));
    }
    return ret;
}

void *vhd_vdev_get_priv(struct vhd_vdev *vdev)
{
    return vdev->priv;
}

/**
 * metrics - output parameter.
 * Returns 0 on success, -errno on failure
 */
int vhd_vdev_get_queue_stat(struct vhd_vdev *vdev, uint32_t queue_num,
                            struct vhd_vq_metrics *metrics)
{
    if (queue_num >= vdev->num_queues) {
        return -EINVAL;
    }

    virtio_virtq_get_stat(&vdev->vrings[queue_num].vq, metrics);

    return 0;
}
