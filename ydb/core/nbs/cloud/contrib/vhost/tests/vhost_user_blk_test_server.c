#define _GNU_SOURCE 1

#include <libaio.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>
#include <inttypes.h>
#include <signal.h>

#include "catomic.h"
#include "vhost/server.h"
#include "vhost/blockdev.h"
#include "test_utils.h"
#include "platform.h"
#include "vdev.h"

#define DIE(fmt, ...)                              \
do {                                               \
    vhd_log_stderr(LOG_ERROR, fmt, ##__VA_ARGS__); \
    exit(EXIT_FAILURE);                            \
} while (0)

#define PERROR(fun, err)                                     \
do {                                                         \
    vhd_log_stderr(LOG_ERROR, "%s: %s", fun, strerror(err)); \
} while (0)

#define VALUE_TO_STR_HELPER(val) #val
#define VALUE_TO_STR(val) VALUE_TO_STR_HELPER(val)

struct request_stats {
    uint64_t dequeued;
    uint64_t submitted;
    uint64_t sub_failed;
    uint64_t completed;
    uint64_t comp_failed;
    uint64_t discards;
    uint64_t ns;
};

/*
 * Configuration used for disk initialization.
 */
struct disk_config {
    const char *socket_path;
    const char *serial;
    const char *blk_file;
    unsigned long delay;
    bool readonly;
    bool support_discard;
    bool support_write_zeroes;
    unsigned long batch_size;
    unsigned long pte_flush_threshold;
    unsigned long num_rqs;
};

/*
 * Structure per disk.
 */
struct disk {
    struct disk_config conf;

    struct vhd_vdev *handler;
    struct vhd_bdev_info info;
    int fd;

    pthread_t *completion_threads;
    pthread_t *submission_threads;

    struct queue *qdevs;
};

#define MAX_NUM_DISKS 8

struct disks_context {
    struct disk disks[MAX_NUM_DISKS];
    size_t num_disks;
};

/*
 * Structure per vhost eventloop, contains link to disk.
 */
struct queue {
    struct vhd_request_queue *rq;
    struct request_stats prev_stats, cur_stats;
    unsigned long delay;
    io_context_t io_ctx;
    unsigned batch_size;
};

/*
 * Single IO request. Also map libvhost's vhd_buffer to iovec.
 */
struct request {
    struct vhd_io *io;
    struct iocb ios;
    bool bounce_buf;
    struct iovec iov[]; /* bio->sglist.nbuffers */
};

/*
 * Zero page used for write-zeroes requests
 */
static uint8_t *zero_page;

static const char *bio_type_to_str(enum vhd_bdev_io_type type)
{
    switch (type) {
    case VHD_BDEV_READ:
        return "Read";
    case VHD_BDEV_WRITE:
        return "Write";
    case VHD_BDEV_DISCARD:
        return "Discard";
    default:
        return "<invalid>";
    }
}

static void trace_io_op(struct vhd_bdev_io *bio)
{
    vhd_log_stderr(LOG_DEBUG,
        "%s request, %u parts: start block %" PRIu64 ", blocks count %" PRIu64,
        bio_type_to_str(bio->type),
        bio->sglist.nbuffers,
        bio->first_sector,
        bio->total_sectors);
}

/*
 * Allocate and prepare IO request (fill iovecs and AIO control block).
 */
static struct iocb *prepare_io_operation(struct vhd_request *lib_req)
{
    struct disk *bdev = vhd_vdev_get_priv(lib_req->vdev);
    struct vhd_bdev_io *bio = vhd_get_bdev_io(lib_req->io);

    uint64_t offset = bio->first_sector * VHD_SECTOR_SIZE;
    struct vhd_buffer *buffers = bio->sglist.buffers;
    unsigned nbufs;
    uint32_t i;
    struct request *req;
    bool is_write_zeroes = bio->type == VHD_BDEV_WRITE_ZEROES;

    if (is_write_zeroes) {
        uint64_t bytes = bio->total_sectors << VHD_SECTOR_SHIFT;
        nbufs = VHD_ALIGN_UP(bytes, platform_page_size) / platform_page_size;
    } else {
        nbufs = bio->sglist.nbuffers;
    }
    trace_io_op(bio);

    req = calloc(1, sizeof(struct request) + sizeof(struct iovec) * nbufs);
    req->io = lib_req->io;

    /*
     * Just write the same one zero-page for WRITE_ZEROES multiple times
     * until we satisfy the requested bio->total_sectors.
     */
    if (is_write_zeroes) {
        uint64_t bytes_left = bio->total_sectors << VHD_SECTOR_SHIFT;

        for (i = 0; i < nbufs; ++i) {
            struct iovec *iov = &req->iov[i];
            iov->iov_base = zero_page;
            iov->iov_len = MIN(platform_page_size, bytes_left);
            bytes_left -= iov->iov_len;
        }
    } else {
        for (i = 0; i < nbufs; i++) {
            /*
             * Windows allows i/o with buffers not aligned to i/o block size,
             * but Linux doesn't, so use bounce buffer in this case.
             * Note: the required alignment is the logical block size of the
             * underlying storage; assume it to equal the sector size as BIOS
             * requires sector-granular i/o anyway.
             */
            if (!VHD_IS_ALIGNED((uintptr_t) buffers[i].base, VHD_SECTOR_SIZE) ||
                !VHD_IS_ALIGNED(buffers[i].len, VHD_SECTOR_SIZE)) {
                req->bounce_buf = true;
                req->iov[0].iov_len = bio->total_sectors * VHD_SECTOR_SIZE;
                req->iov[0].iov_base = aligned_alloc(VHD_SECTOR_SIZE,
                                                     req->iov[0].iov_len);

                if (bio->type == VHD_BDEV_WRITE) {
                    void *ptr = req->iov[0].iov_base;
                    for (i = 0; i < nbufs; i++) {
                        memcpy(ptr, buffers[i].base, buffers[i].len);
                        ptr += buffers[i].len;
                    }
                }

                nbufs = 1;

                break;
            }

            req->iov[i].iov_base = buffers[i].base;
            req->iov[i].iov_len = buffers[i].len;
        }
    }

    if (bio->type == VHD_BDEV_READ) {
        io_prep_preadv(&req->ios, bdev->fd, req->iov, nbufs, offset);
    } else {
        io_prep_pwritev(&req->ios, bdev->fd, req->iov, nbufs, offset);
    }

    /* set AFTER io_prep_* because it fill all fields with zeros */
    req->ios.data = req;
    vhd_log_stderr(LOG_DEBUG, "Prepared IO request with addr: %p", req);

    return &req->ios;
}

static void complete_request(struct request *req,
                             enum vhd_bdev_io_result status)
{
    struct vhd_bdev_io *bio = vhd_get_bdev_io(req->io);

    if (req->bounce_buf) {
        if (bio->type == VHD_BDEV_READ && status == VHD_BDEV_SUCCESS) {
            struct vhd_buffer *buffers = bio->sglist.buffers;
            unsigned nbufs = bio->sglist.nbuffers, i;
            void *ptr = req->iov[0].iov_base;
            for (i = 0; i < nbufs; i++) {
                memcpy(buffers[i].base, ptr, buffers[i].len);
                ptr += buffers[i].len;
            }
        }
        free(req->iov[0].iov_base);
    }
    vhd_complete_bio(req->io, status);
    free(req);
}

static int prepare_batch(struct vhd_request_queue *rq,
                         struct iocb **ios, int batch_size,
                         uint64_t *nr_discards)
{
    int nr = 0;
    struct vhd_request req;
    struct vhd_bdev_io *bio;

    while (nr < batch_size && vhd_dequeue_request(rq, &req)) {
        bio = vhd_get_bdev_io(req.io);

        /*
         * Pretend we discarded the sectors, and skip the request
         * for the batch as we don't need it there.
         */
        if (bio->type == VHD_BDEV_DISCARD) {
            trace_io_op(bio);
            vhd_complete_bio(req.io, VHD_BDEV_SUCCESS);
            (*nr_discards)++;
            continue;
        }

        ios[nr++] = prepare_io_operation(&req);
    }

    return nr;
}

/*
 * IO requests submission thread, that serve all requests in one vhost
 * eventloop.
 */
static void *io_submission(void *opaque)
{
    struct queue *qdev = opaque;
    struct iocb **ios = calloc(qdev->batch_size, sizeof(*ios));
    int nr = 0;
    struct request_stats *stats = &qdev->cur_stats;
    uint64_t dequeued = 0, submitted = 0, sub_failed = 0, discards = 0;

    while (true) {
        int ret;

        ret = vhd_run_queue(qdev->rq);
        if (ret != -EAGAIN) {
            if (ret < 0) {
                vhd_log_stderr(LOG_ERROR, "vhd_run_queue error: %d", ret);
            }
            break;
        }

        while (true) {
            /* append new requests to the tail of the batch */
            ret = prepare_batch(qdev->rq, ios + nr, qdev->batch_size - nr,
                                &discards);
            nr += ret;
            dequeued += ret;
            if (nr == 0) {
                break;
            }

            do {
                ret = io_submit(qdev->io_ctx, nr, ios);
            } while (ret == -EINTR);

            /*
             * kernel queue full, punt the re-submission to later event
             * loop iterations, woken up by completions
             */
            if (ret == -EAGAIN) {
                break;
            }

            /*
             * submission failed for other reasons, fail the first request but
             * keep the rest of the batch
             */
            if (ret < 0) {
                PERROR("io_submit", -ret);
                complete_request((*ios)->data, VHD_BDEV_IOERR);
                sub_failed++;
                ret = 1;
            }

            nr -= ret;
            submitted += ret;
            /* move the rest of the batch to the front of the array */
            memmove(ios, ios + ret, nr * sizeof(ios[0]));
        }

        catomic_set(&stats->dequeued, dequeued);
        catomic_set(&stats->submitted, submitted);
        catomic_set(&stats->discards, discards);
        catomic_set(&stats->sub_failed, sub_failed);
    }

    free(ios);
    return NULL;
}

static sig_atomic_t stop_completion_thread;

static void thread_exit()
{
    stop_completion_thread = 1;
}

/*
 * IO requests completion handler thread, that serve one disk.
 */
static void *io_completion(void *thread_data)
{
    struct queue *qdev = thread_data;
    struct io_event *events = calloc(qdev->batch_size, sizeof(*events));
    struct request_stats *stats = &qdev->cur_stats;
    uint64_t completed = 0, comp_failed = 0;

    /* setup a signal handler for thread stopping */
    struct sigaction sigusr1_action = {
        .sa_handler = thread_exit,
    };
    sigaction(SIGUSR1, &sigusr1_action, NULL);

    while (true) {
        int ret;

        ret = io_getevents(qdev->io_ctx, 1, qdev->batch_size, events, NULL);
        if (ret < 0 && ret != -EINTR) {
            DIE("io_getevents: %s", strerror(-ret));
        }

        if (stop_completion_thread) {
            break;
        }

        for (int i = 0; i < ret; i++) {
            struct request *req = events[i].data;
            struct vhd_bdev_io *bio = vhd_get_bdev_io(req->io);

            vhd_log_stderr(LOG_DEBUG,
                           "IO result event for request with addr: %p", req);

            if ((events[i].res2 != 0) ||
                (events[i].res != bio->total_sectors * VHD_SECTOR_SIZE)) {
                complete_request(req, VHD_BDEV_IOERR);
                comp_failed++;
                PERROR("IO request", -events[i].res);
            } else {
                if (qdev->delay) {
                    usleep(qdev->delay);
                }
                complete_request(req, VHD_BDEV_SUCCESS);
                vhd_log_stderr(LOG_DEBUG, "IO request completed successfully");
            }
            completed++;
        }
        catomic_set(&stats->completed, completed);
        catomic_set(&stats->comp_failed, comp_failed);
    }

    free(events);
    return NULL;
}

/*
 * Prepare disk before server starts.
 */
static int init_disk(struct disk *d)
{
    struct disk_config *conf = &d->conf;
    off64_t file_len;
    int ret = 0;
    int flags = (conf->readonly ? O_RDONLY : O_RDWR) | O_DIRECT;

    d->fd = open(conf->blk_file, flags);
    if (d->fd < 0) {
        ret = errno;
        PERROR("open", ret);
        return -ret;
    }

    file_len = lseek(d->fd, 0, SEEK_END);
    if (file_len % VHD_SECTOR_SIZE != 0) {
        vhd_log_stderr(LOG_WARNING,
                       "File size is not a multiple of the block size");
        vhd_log_stderr(LOG_WARNING,
                       "Last %llu bytes will not be accessible",
                       file_len % VHD_SECTOR_SIZE);
    }

    d->info.socket_path = conf->socket_path;
    d->info.serial = conf->serial;
    d->info.block_size = VHD_SECTOR_SIZE;
    d->info.num_queues = 256; /* Max count of virtio queues */
    d->info.total_blocks = file_len / VHD_SECTOR_SIZE;
    d->info.map_cb = NULL;
    d->info.unmap_cb = NULL;
    d->info.pte_flush_byte_threshold = conf->pte_flush_threshold;

    if (conf->readonly) {
        d->info.features |= VHD_BDEV_F_READONLY;
    }

    if (conf->support_discard) {
        d->info.features |= VHD_BDEV_F_DISCARD;
    }
    if (conf->support_write_zeroes) {
        d->info.features |= VHD_BDEV_F_WRITE_ZEROES;
    }

    return 0;
}

/*
 * Show usage message.
 */
static void usage(const char *cmd)
{
    printf("Usage: %s -d socket-path=PATH,serial=STRING,blk-file=PATH\n", cmd);
    printf("Start vhost daemon.\n");
    printf("\n");
    printf("Mandatory arguments to long options "
           "are mandatory for short options too.\n");
    printf("  -d, --disk=DISK where DISK consists of the following "
           "arguments:\n");
    printf("      ,socket-path=PATH  vhost-user Unix domain socket path\n");
    printf("      ,serial=STRING     disk serial\n");
    printf("      ,blk-file=PATH     block device or file path\n");
    printf("      ,readonly=on|off   readonly block device\n");
    printf("      ,discard=on|off    declare discard request support "
           "to guest\n");
    printf("      ,write-zeroes=on|off declare write-zeroes request support "
           "to guest\n");
    printf("      ,delay=USECS       delay each completion by USECS\n");
    printf("      ,num-rqs=NUM       NUM of rqs to spawn\n");
    printf("      ,batch-size=NUM    submit/complete i/o in batches "
           "of up to NUM\n");
    printf("      ,pte-flush-threshold=NUM Bytes to process before flushing "
           "the PTEs (0 disables this)\n");
    printf("  -m, --monitor=PATH      Unix socket for interactive command line "
           "to operate with sever. Or 'stdio' keyword to operate through stdin "
           "and stdout\n");
}

static bool set_string(const char *val, void *dst)
{
    const char **real_dst = dst;
    *real_dst = strdup(val);
    return true;
}

static bool set_bool(const char *val, void *dst)
{
    bool *real_dst = dst;
    int setting = -1;

    if (strcmp(val, "on") == 0) {
        setting = 1;
    } else if (strcmp(val, "off") == 0) {
        setting = 0;
    }

    if (setting == -1) {
        return false;
    }

    *real_dst = setting;
    return true;
}

static bool set_ul(const char *val, void *dst)
{
    unsigned long conv_val, *real_dst = dst;
    char *end;

    conv_val = strtoul(val, &end, 0);
    if (end != (val + strlen(val))) {
        return false;
    }

    *real_dst = conv_val;
    return true;
}

enum disk_arg {
    DISK_ARG_SOCKET_PATH = 0,
    DISK_ARG_SERIAL,
    DISK_ARG_BLK_FILE,
    DISK_ARG_READONLY,
    DISK_ARG_DISCARD,
    DISK_ARG_WRITE_ZEROES,
    DISK_ARG_DELAY,
    DISK_ARG_NUM_RQS,
    DISK_ARG_BATCH_SIZE,
    DISK_ARG_PTE_FLUSH_THRESHOLD,
};

static char *const disk_arg_tokens[] = {
    [DISK_ARG_SOCKET_PATH] = "socket-path",
    [DISK_ARG_SERIAL] = "serial",
    [DISK_ARG_BLK_FILE] = "blk-file",
    [DISK_ARG_READONLY] = "readonly",
    [DISK_ARG_DISCARD] = "discard",
    [DISK_ARG_WRITE_ZEROES] = "write-zeroes",
    [DISK_ARG_DELAY] = "delay",
    [DISK_ARG_NUM_RQS] = "num-rqs",
    [DISK_ARG_BATCH_SIZE] = "batch-size",
    [DISK_ARG_PTE_FLUSH_THRESHOLD] = "pte-flush-threshold",
    NULL
};

struct arg_setter {
    bool (*set)(const char *val, void *dst);
    size_t field_offset;
};

static bool set_disk_arg(struct arg_setter *arg, const char *val,
                         struct disk_config *conf)
{
    return arg->set(val, ((char *)conf) + arg->field_offset);
}

#define CONF_FIELD(field) offsetof(struct disk_config, field)
static struct arg_setter disk_arg_setters[] = {
    [DISK_ARG_SOCKET_PATH] = { set_string, CONF_FIELD(socket_path) },
    [DISK_ARG_SERIAL] = { set_string, CONF_FIELD(serial) },
    [DISK_ARG_BLK_FILE] = { set_string, CONF_FIELD(blk_file) },
    [DISK_ARG_READONLY] = { set_bool, CONF_FIELD(readonly) },
    [DISK_ARG_DISCARD] = { set_bool, CONF_FIELD(support_discard) },
    [DISK_ARG_WRITE_ZEROES] = { set_bool, CONF_FIELD(support_write_zeroes) },
    [DISK_ARG_DELAY] = { set_ul, CONF_FIELD(delay) },
    [DISK_ARG_NUM_RQS] = { set_ul, CONF_FIELD(num_rqs) },
    [DISK_ARG_BATCH_SIZE] = { set_ul, CONF_FIELD(batch_size) },
    [DISK_ARG_PTE_FLUSH_THRESHOLD] = { set_ul, CONF_FIELD(pte_flush_threshold) },
};

static bool parse_disk_args(const char *args, struct disk_config *conf)
{
    char *subopts = optarg;
    char *value;

    while (*subopts != '\0') {
        int ret;

        ret = getsubopt(&subopts, disk_arg_tokens, &value);
        if (ret < 0 || value == NULL) {
            return false;
        }

        if (!set_disk_arg(&disk_arg_setters[ret], value, conf)) {
            return false;
        }
    }

    return true;
}

/*
 * Parse command line options.
 */
static void parse_opts(int argc, char **argv, struct disks_context *ctx,
                       const char **monitor)
{
    int opt;
    do {
        static struct option long_options[] = {
            {"disk",     1, NULL, 'd'},
            {"monitor",  1, NULL, 'm'},
            {0, 0, 0, 0}
        };

        opt = getopt_long(argc, argv, "d:m:", long_options, NULL);

        switch (opt) {
        case -1:
            break;
        case 'd':
            if (ctx->num_disks == MAX_NUM_DISKS) {
                DIE("too many disks specified, max is %d\n",
                    MAX_NUM_DISKS);
            }

            if (!parse_disk_args(optarg, &ctx->disks[ctx->num_disks++].conf)) {
                goto out_bad_arg;
            }
            break;
        case 'm':
            *monitor = optarg;
            break;
        default:
            goto out_bad_arg;
        }
    } while (opt != -1);

    return;

out_bad_arg:
    usage(argv[0]);
    exit(2);
}

static void notify_event(void *opaque)
{
    int *fd = (int *) opaque;

    while (eventfd_write(*fd, 1) && errno == EINTR) {
        ;
    }
}

static void wait_event(int fd)
{
    eventfd_t unused;

    while (eventfd_read(fd, &unused) && errno == EINTR) {
        ;
    }
}

static uint64_t clock_get_ns()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void do_dump_stats(struct request_stats *new,
                          struct request_stats *old,
                          bool print_totals) {
    uint64_t dequeued = catomic_read(&new->dequeued);
    uint64_t submitted = catomic_read(&new->submitted);
    uint64_t sub_failed = catomic_read(&new->sub_failed);
    uint64_t completed = catomic_read(&new->completed);
    uint64_t comp_failed = catomic_read(&new->comp_failed);
    uint64_t discards = catomic_read(&new->discards);
    uint64_t ns = clock_get_ns();
    uint64_t elapsed_ms = (ns - old->ns) / 1000000;

    vhd_log_stderr(LOG_INFO,
                   "Stats: %"PRIu64".%03"PRIu64"s elapsed, "
                   "%"PRIu64" dequeued, "
                   "%"PRIu64" submitted (%"PRIu64" failed), "
                   "%"PRIu64" completed (%"PRIu64" failed), "
                   "%"PRIu64" discards",
                   elapsed_ms / 1000, elapsed_ms % 1000,
                   dequeued - old->dequeued,
                   submitted - old->submitted,
                   sub_failed - old->sub_failed,
                   completed - old->completed,
                   comp_failed - old->comp_failed,
                   discards - old->discards);

    if (print_totals) {
        vhd_log_stderr(LOG_INFO,
                       "Totals: "
                       "%"PRIu64" dequeued, "
                       "%"PRIu64" submitted (%"PRIu64" failed), "
                       "%"PRIu64" completed (%"PRIu64" failed), "
                       "%"PRIu64" discards",
                       dequeued,
                       submitted,
                       sub_failed,
                       completed,
                       comp_failed,
                       discards);
    }

    old->dequeued = dequeued;
    old->submitted = submitted;
    old->sub_failed = sub_failed;
    old->completed = completed;
    old->comp_failed = comp_failed;
    old->discards = discards;
    old->ns = ns;
}

static void dump_per_queue_stats(struct queue *qdevs, unsigned long num_rqs,
                                 bool print_totals)
{
    unsigned long i;

    for (i = 0; i < num_rqs; ++i) {
        vhd_log_stderr(LOG_INFO, "======> QUEUE %lu", i);
        do_dump_stats(&qdevs[i].cur_stats, &qdevs[i].prev_stats, print_totals);
    }
}

static struct queue *init_queues(struct disk *d)
{
    struct disk_config *conf = &d->conf;
    struct vhd_request_queue *vqs[VHD_MAX_REQUEST_QUEUES];
    struct queue *qdevs;
    unsigned long i;
    uint64_t ns;

    qdevs = calloc(conf->num_rqs, sizeof(struct queue));
    ns = clock_get_ns();

    for (i = 0; i < conf->num_rqs; ++i) {
        struct queue *qdev = &qdevs[i];

        qdev->delay = conf->delay;
        qdev->batch_size = conf->batch_size;

        if (io_setup(qdev->batch_size, &qdev->io_ctx) < 0) {
            DIE("io_setup");
        }

        vqs[i] = vhd_create_request_queue();
        qdev->rq = vqs[i];
        if (!qdev->rq) {
            DIE("vhd_create_request_queue failed");
        }

        qdev->prev_stats.ns = ns;
    }

    d->handler = vhd_register_blockdev(&d->info, vqs, conf->num_rqs, d);
    if (!d->handler) {
        vhd_log_stderr(LOG_ERROR,
                       "vhd_register_blockdev: Can't register device");
        DIE("init_queues failed");
    }

    return qdevs;
}

static void create_threads(struct disk *d)
{
    unsigned long i;
    unsigned long num_rqs = d->conf.num_rqs;

    d->completion_threads = malloc(sizeof(pthread_t) * num_rqs);
    d->submission_threads = malloc(sizeof(pthread_t) * num_rqs);

    for (i = 0; i < num_rqs; ++i) {
        /* start the worker thread(s) */
        pthread_create(&d->completion_threads[i], NULL, io_completion,
                       &d->qdevs[i]);

        /* start libvhost request queue runner thread */
        pthread_create(&d->submission_threads[i], NULL, io_submission,
                       &d->qdevs[i]);
    }
}

static void stop_and_release_threads(struct disk *d)
{
    unsigned long i;

    /* For each do: */
    for (i = 0; i < d->conf.num_rqs; ++i) {
        /* 1. Stop a request queue */
        vhd_stop_queue(d->qdevs[i].rq);

        /* 2 Wait for queue's thread to join */
        pthread_join(d->submission_threads[i], NULL);

        /* 3. Stop the worker thread(s) */
        pthread_kill(d->completion_threads[i], SIGUSR1);
        pthread_join(d->completion_threads[i], NULL);
    }

    free(d->completion_threads);
    free(d->submission_threads);
}

static void release_queues(struct queue *qdevs, unsigned long num_qdevs)
{
    unsigned long i;

    /* For each do */
    for (i = 0; i < num_qdevs; ++i) {
        struct queue *qdev = &qdevs[i];

        /* 1. Release the queue */
        vhd_release_request_queue(qdev->rq);

        /* 2. Release io ctx */
        io_destroy(qdev->io_ctx);
    }

    free(qdevs);
}

static int resize(struct disk *d, uint64_t new_size)
{
    int ret;
    uint32_t block_size = d->info.block_size;

    vhd_log_stderr(LOG_INFO, "Resize command");

    if (new_size % block_size != 0) {
        vhd_log_stderr(LOG_WARNING,
                       "New file size is not a multiple of the block size");
        vhd_log_stderr(LOG_WARNING,
                       "Last %lu bytes will not be accessible",
                       new_size % block_size);
    }

    ret = ftruncate(d->fd, new_size);
    if (ret < 0) {
        vhd_log_stderr(LOG_ERROR, "ftruncate failed %m");
        return ret;
    }

    vhd_blockdev_set_total_blocks(d->handler, new_size / block_size);

    vhd_log_stderr(LOG_INFO, "Resized succesfully to %" PRIu64, new_size);

    return 0;
}

static bool monitor_serve_fd(FILE *f_in, FILE *f_out,
                             struct disks_context *ctx)
{
    const char *const help = "Commands:\n"
                             "  help  -  print this message\n"
                             "  stop  -  stop the server and quit\n"
                             "  stat  -  print statistics\n"
                             "  resize <new_size>  -  resize the disk\n";

    bool interactive = (f_in == stdin && f_out == stdout);

    if (interactive) {
        puts(help);
    }

    vhd_log_stderr(LOG_INFO, "Start serve monitor fd");

    while (true) {
        char cmdline[100];
        uint64_t new_size, dev_idx;
        int ret;
        size_t len;
        char output_buf[100];
        int output_buf_size = sizeof(output_buf);
        const char *out = output_buf;

        ret = fputs("$> ", f_out);
        if (ret < 0) {
            vhd_log_stderr(LOG_INFO,
                           "Stop serve monitor fd: fputs($> ) failed");
            return false;
        }

        ret = fflush(f_out);
        if (ret < 0) {
            vhd_log_stderr(LOG_INFO, "Stop serve monitor fd: fflush failed");
            return false;
        }

        if (!fgets(cmdline, sizeof(cmdline), f_in)) {
            vhd_log_stderr(LOG_INFO, "Stop serve monitor fd: EOF or failure");
            return false;
        }

        len = strlen(cmdline);
        if (cmdline[len - 1] == '\n') {
            if (len == 1) {
                continue;
            }
            cmdline[len - 1] = '\0';
        }

        if (!interactive) {
            vhd_log_stderr(LOG_INFO, "Handle command: '%s'", cmdline);
        }

        if (!strcmp(cmdline, "stop")) {
            vhd_log_stderr(LOG_INFO, "Stop serve monitor fd: 'stop' command");
            /* After stop command we don't put anything to fd on server side */
            return true;
        } else if (!strcmp(cmdline, "help")) {
            out = help;
        } else if (sscanf(cmdline, "stat %" PRIu64 "\n", &dev_idx) == 1) {
            if (dev_idx < ctx->num_disks) {
                struct disk *d = &ctx->disks[dev_idx];
                dump_per_queue_stats(d->qdevs, d->conf.num_rqs, false);
                out = "Stats dumped\n";
            } else {
                snprintf(output_buf, output_buf_size,
                         "Invalid device index %" PRIu64 "\n", dev_idx);
            }
        } else if (sscanf(cmdline, "resize %" PRIu64 " %" PRIu64 "\n",
                          &dev_idx, &new_size) == 2) {
            if (dev_idx < ctx->num_disks) {
                ret = resize(&ctx->disks[dev_idx], new_size);
                if (ret == 0) {
                    ret = snprintf(output_buf, output_buf_size,
                                   "Successfully resized to %ld\n", new_size);
                } else {
                    ret = snprintf(output_buf, output_buf_size,
                                   "Resize failed: %s\n", strerror(-ret));
                }
                VHD_VERIFY(ret < output_buf_size); /* fit in buffer */
            } else {
                snprintf(output_buf, output_buf_size,
                         "Invalid device index %" PRIu64 "\n", dev_idx);
            }
        } else {
            out = "Unknown command\n";
        }

        ret = fputs(out, f_out);
        if (ret < 0) {
            vhd_log_stderr(LOG_INFO,
                           "Stop serve monitor fd: fputs(result) failed");
            return false;
        }

        if (!interactive) {
            vhd_log_stderr(LOG_INFO, "Result:\n<<<%s>>>", out);
        }
    }
}

/*
 * Accordingly to https://man7.org/linux/man-pages/man3/signal.3p.html:
 *
 *       the behavior is undefined if the signal handler refers to any
 *       object other than errno with static storage duration other than
 *       by assigning a value to an object declared as volatile
 *       sig_atomic_t, or if the signal handler calls any function defined
 *       in this standard other than one of the functions listed in
 *       Section 2.4, Signal Concepts.
 *
 * So, seems we can't access sockets monitor_client_sock and
 * monitor_listen_sock in that way. Still, the only alternative I see is making
 * a full-functional event-loop, which seems too much for this test solution.
 *
 * Don't use this signal-handling code for production.
 */
static volatile sig_atomic_t signalled;
/* monitor_client_sock is for communication with client */
static volatile int monitor_client_sock = -1;
/* monitor_listen_sock is for waiting for the new client */
static volatile int monitor_listen_sock = -1;
static void monitor_sock_sigint(int sig)
{
    shutdown(monitor_client_sock, SHUT_RDWR);
    shutdown(monitor_listen_sock, SHUT_RDWR);
    signalled = 1;
}

static void monitor_serve_unix_socket(const char *path,
                                      struct disks_context *ctx)
{
    struct sockaddr_un addr = {
        .sun_family = AF_UNIX,
    };

    struct sigaction sigint_action = {
        .sa_handler = monitor_sock_sigint,
    };
    sigaction(SIGINT, &sigint_action, NULL);

    unlink(path);

    monitor_listen_sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (monitor_listen_sock < 0) {
        DIE("Can't create socket: %m");
    }

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);

    int ret = bind(monitor_listen_sock, (const struct sockaddr *) &addr,
                   sizeof(addr));
    if (ret < 0) {
        DIE("Can't bind socket: %m");
    }

    ret = listen(monitor_listen_sock, 20);
    if (ret < 0) {
        DIE("Can't listen on socket: %m");
    }

    bool stop;
    do {
        monitor_client_sock = accept(monitor_listen_sock, NULL, NULL);
        if (monitor_client_sock < 0) {
            if (signalled) {
                break;
            }
            DIE("Can't accept connection on socket: %m");
        }
        FILE *fp = fdopen(monitor_client_sock, "r+b");

        stop = monitor_serve_fd(fp, fp, ctx);
        stop = stop || signalled;

        fclose(fp);
    } while (!stop);

    close(monitor_listen_sock);
}

static void interactive_sigint(int sig)
{
    const char *const msg = "\nUse 'stop' command or Ctrl+D to exit\n";
    write(2, msg, strlen(msg));
}

static void monitor_serve_stdio(struct disks_context *ctx)
{
    struct sigaction sigint_action = {
        .sa_handler = interactive_sigint,
    };
    sigaction(SIGINT, &sigint_action, NULL);
    monitor_serve_fd(stdin, stdout, ctx);
}

static void monitor_serve(const char *path, struct disks_context *ctx)
{
    if (!strcmp(path, "stdio")) {
        monitor_serve_stdio(ctx);
    } else {
        monitor_serve_unix_socket(path, ctx);
    }
}

static bool validate_disk_config(struct disk_config *conf, const char **err)
{
    if (!conf->socket_path) {
        *err = "no socket-path specified";
        return false;
    }

    if (!conf->blk_file) {
        *err = "no blk-file specified";
        return false;
    }

    if (!conf->serial) {
        *err = "no serial specified";
        return false;
    }

    if (conf->num_rqs < 1 || conf->num_rqs > VHD_MAX_REQUEST_QUEUES) {
        *err = "invalid num-rqs, expected a value between 1 and " \
                VALUE_TO_STR(VHD_MAX_REQUEST_QUEUES);
        return false;
    }

    return true;
}

static void disk_start(struct disk *d)
{
    if (init_disk(d) < 0) {
        DIE("init_disk failed");
    }

    d->qdevs = init_queues(d);

    create_threads(d);
}

static void disk_stop(struct disk *d)
{
    struct disk_config *conf = &d->conf;
    int unreg_done_fd;

    /* stopping sequence - should be in this order */

    /* 1. Unregister the blockdevs */
    /* 1.1 Prepare unregestring done event */
    unreg_done_fd = eventfd(0, 0);
    if (unreg_done_fd == -1) {
        DIE("eventfd creation failed");
    }
    /* 1.2. Call for unregister blockdev */
    vhd_unregister_blockdev(d->handler, notify_event, &unreg_done_fd);

    /* 1.3. Wait until the unregestering finishes */
    wait_event(unreg_done_fd);

    /* 2. Stop request queues. */
    stop_and_release_threads(d);

    /* dump total stats for this run */
    dump_per_queue_stats(d->qdevs, conf->num_rqs, true);

    /* 3. Release request queues */
    release_queues(d->qdevs, conf->num_rqs);

    /* 4. Close the image */
    close(d->fd);

    /* 5. Free config strings */
    vhd_free((void *)conf->socket_path);
    vhd_free((void *)conf->blk_file);
    vhd_free((void *)conf->serial);
}

static void init_zero_page(void)
{
    int res;

    res = init_platform_page_size();
    if (res) {
        DIE("failed to init platform page size: %d", res);
    }

    zero_page = mmap(NULL, platform_page_size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (zero_page == MAP_FAILED) {
        DIE("failed to map zero page: %d", errno);
    }
}

/*
 * Main execution thread. Used for initializing and common management.
 */
int main(int argc, char **argv)
{
    struct disks_context ctx = {};
    const char *monitor = NULL, *err = NULL;
    size_t i;

    for (i = 0; i < MAX_NUM_DISKS; ++i) {
        struct disk_config *conf = &ctx.disks[i].conf;
        conf->batch_size = 128;
        conf->num_rqs = 1;
    }

    parse_opts(argc, argv, &ctx, &monitor);

    for (i = 0; i < ctx.num_disks; ++i) {
        struct disk_config *conf = &ctx.disks[i].conf;

        if (!validate_disk_config(conf, &err)) {
            usage(argv[0]);
            DIE("Invalid command line options (disk %zu): %s\n", i, err);
        }
    }

    init_zero_page();
    if (vhd_start_vhost_server(vhd_log_stderr) < 0) {
        DIE("vhd_start_vhost_server failed");
    }

    for (i = 0; i < ctx.num_disks; ++i) {
        disk_start(&ctx.disks[i]);
    }

    vhd_log_stderr(LOG_INFO, "Test server started");

    if (monitor) {
        monitor_serve(monitor, &ctx);
    } else {
        sigset_t sigset;
        int sig;
        /* tune the signal to block on waiting for "stop server" command */
        sigemptyset(&sigset);
        sigaddset(&sigset, SIGINT);
        pthread_sigmask(SIG_BLOCK, &sigset, NULL);

        /* wait for signal to stop the server (Ctrl+C) */
        sigwait(&sigset, &sig);
    }

    vhd_log_stderr(LOG_INFO, "Stopping the server");

    for (i = 0; i < ctx.num_disks; ++i) {
        disk_stop(&ctx.disks[i]);
    }

    vhd_stop_vhost_server();

    vhd_log_stderr(LOG_INFO, "Server has been stopped.");

    return 0;
}
