LIBRARY(vhost-server)

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    blockdev.c
    event.c
    fs.c
    logging.c
    memlog.c
    memmap.c
    server.c
    vdev.c
    platform.c
    virtio/virt_queue.c
    virtio/virtio_blk.c
    virtio/virtio_fs.c
)

ADDINCL(
    GLOBAL ydb/core/nbs/cloud/contrib/vhost/include
    ydb/core/nbs/cloud/contrib/vhost
)

END()
