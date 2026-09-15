/**
 * vhost-user protocol definitions
 */

#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Define protocol structures and definitions based on the vhost user
 * protocol specification:
 * https://github.com/qemu/qemu/blob/master/docs/interop/vhost-user.txt
 */

/* Vhost user protocol flags. */
/* This is a vhost protocol version. */
#define VHOST_USER_VERSION_MASK        0x3
#define VHOST_USER_MSG_VERSION         0x1
#define VHOST_USER_MSG_FLAGS_REPLY     ((1 << 2) | VHOST_USER_MSG_VERSION)
#define VHOST_USER_MSG_FLAGS_REPLY_ACK (1 << 3)

/*
 * Vhost user protocol features (GET_PROTOCOL_FEATURES and
 * SET_PROTOCOL_FEATURES commands).
 */
#define VHOST_USER_PROTOCOL_F_MQ             0
#define VHOST_USER_PROTOCOL_F_LOG_SHMFD      1
#define VHOST_USER_PROTOCOL_F_RARP           2
#define VHOST_USER_PROTOCOL_F_REPLY_ACK      3
#define VHOST_USER_PROTOCOL_F_MTU            4
#define VHOST_USER_PROTOCOL_F_SLAVE_REQ      5
#define VHOST_USER_PROTOCOL_F_CROSS_ENDIAN   6
#define VHOST_USER_PROTOCOL_F_CRYPTO_SESSION 7
#define VHOST_USER_PROTOCOL_F_PAGEFAULT      8
#define VHOST_USER_PROTOCOL_F_CONFIG         9
#define VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD 12
#define VHOST_USER_PROTOCOL_F_CONFIGURE_MEM_SLOTS 15

/* Vhost user features (GET_FEATURES and SET_FEATURES commands). */
#define VHOST_F_LOG_ALL                     26
#define VHOST_USER_F_PROTOCOL_FEATURES      30
#define VIRTIO_F_RING_INDIRECT_DESC         28
#define VIRTIO_F_RING_EVENT_IDX             29
#define VIRTIO_F_VERSION_1                  32

/*
 * Invalid FD bit for the VHOST_USER_SET_VRING_KICK and
 * VHOST_USER_SET_VRING_CALL commands. If this bit is
 * set then the communication is forced to use polling
 * instead of using for a kick.
 */
#define VHOST_VRING_INVALID_FD  (1 << 8)
#define VHOST_VRING_IDX_MASK    0xff

/* Maximum size of the device PCI configuration space. */
#define VHOST_USER_CONFIG_SPACE_MAX 256

/*
 * According to the protocol specification this is the maximum number
 * of memory regions sent in one command. Also it is the maximum number
 * of file descriptors.
 */
#define VHOST_USER_MEM_REGIONS_MAX 8
#define VHOST_USER_MAX_FDS VHOST_USER_MEM_REGIONS_MAX

/* Define request types. */
enum {
    VHOST_USER_NONE = 0,
    VHOST_USER_GET_FEATURES = 1,
    VHOST_USER_SET_FEATURES = 2,
    VHOST_USER_SET_OWNER = 3,
    VHOST_USER_RESET_OWNER = 4,
    VHOST_USER_SET_MEM_TABLE = 5,
    VHOST_USER_SET_LOG_BASE = 6,
    VHOST_USER_SET_LOG_FD = 7,
    VHOST_USER_SET_VRING_NUM = 8,
    VHOST_USER_SET_VRING_ADDR = 9,
    VHOST_USER_SET_VRING_BASE = 10,
    VHOST_USER_GET_VRING_BASE = 11,
    VHOST_USER_SET_VRING_KICK = 12,
    VHOST_USER_SET_VRING_CALL = 13,
    VHOST_USER_SET_VRING_ERR = 14,
    VHOST_USER_GET_PROTOCOL_FEATURES = 15,
    VHOST_USER_SET_PROTOCOL_FEATURES = 16,
    VHOST_USER_GET_QUEUE_NUM = 17,
    VHOST_USER_SET_VRING_ENABLE = 18,
    VHOST_USER_SEND_RARP = 19,
    VHOST_USER_NET_SET_MTU = 20,
    VHOST_USER_SET_SLAVE_REQ_FD = 21,
    VHOST_USER_IOTLB_MSG = 22,
    VHOST_USER_SET_VRING_ENDIAN = 23,
    VHOST_USER_GET_CONFIG = 24,
    VHOST_USER_SET_CONFIG = 25,
    VHOST_USER_CREATE_CRYPTO_SESSION = 26,
    VHOST_USER_CLOSE_CRYPTO_SESSION = 27,
    VHOST_USER_POSTCOPY_ADVISE = 28,
    VHOST_USER_POSTCOPY_LISTEN = 29,
    VHOST_USER_POSTCOPY_END = 30,
    VHOST_USER_GET_INFLIGHT_FD = 31,
    VHOST_USER_SET_INFLIGHT_FD = 32,
    VHOST_USER_GET_MAX_MEM_SLOTS = 36,
    VHOST_USER_ADD_MEM_REG = 37,
    VHOST_USER_REM_MEM_REG = 38,
};

struct vhost_user_mem_region {
    uint64_t guest_addr;
    uint64_t size;
    uint64_t user_addr;
    uint64_t mmap_offset;
};

struct vhost_user_mem_single_mem_desc {
    uint64_t _padding;
    struct vhost_user_mem_region region;
};

struct vhost_user_mem_desc {
    uint32_t nregions;
    uint32_t _padding;
    struct vhost_user_mem_region regions[VHOST_USER_MEM_REGIONS_MAX];
};

struct vhost_user_vring_state {
    uint32_t index;
    uint32_t num;
};

struct vhost_user_vring_addr {
    uint32_t index;
#define VHOST_VRING_F_LOG (1 << 0)
    uint32_t flags;
    uint64_t desc_addr;
    uint64_t used_addr;
    uint64_t avail_addr;
    uint64_t used_gpa_base;
};

struct vhost_user_config_space {
    uint32_t offset;
    uint32_t size;
    uint32_t flags;
    uint8_t payload[VHOST_USER_CONFIG_SPACE_MAX];
};
#define VHOST_CONFIG_HDR_SIZE (offsetof(struct vhost_user_config_space, payload))

struct vhost_user_inflight_desc {
    uint64_t mmap_size;
    uint64_t mmap_offset;
    uint16_t num_queues;
    uint16_t queue_size;
};

struct inflight_split_desc {
    uint8_t inflight;
    uint8_t padding[5];
    uint16_t next;
    uint64_t counter;
};

struct inflight_split_region {
    uint64_t features;
    uint16_t version;
    uint16_t desc_num;
    uint16_t last_batch_head;
    uint16_t used_idx;
    struct inflight_split_desc desc[];
};

struct vhost_user_log {
    uint64_t size;
    uint64_t offset;
};

struct vhost_user_msg_hdr {
    uint32_t req;
    uint32_t flags;
    uint32_t size;
};

union vhost_user_msg_payload {
    /*
     * VHOST_USER_GET_QUEUE_NUM, VHOST_USER_GET_PROTOCOL_FEATURES,
     * VHOST_USER_GET_FEATURES,
     * VHOST_USER_SET_VRING_KICK, VHOST_USER_SET_VRING_CALL
     */
    uint64_t u64;
    /* VHOST_USER_GET_CONFIG, VHOST_USER_SET_CONFIG */
    struct vhost_user_config_space config;
    /* VHOST_USER_SET_MEM_TABLE */
    struct vhost_user_mem_desc mem_desc;
    /*
     * VHOST_USER_GET_VRING_BASE, VHOST_USER_SET_VRING_BASE,
     * VHOST_USER_SET_VRING_NUM
     */
    struct vhost_user_vring_state vring_state;
    /* VHOST_USER_SET_VRING_ADDR */
    struct vhost_user_vring_addr vring_addr;
    /* VHOST_USER_GET_INFLIGHT_FD, VHOST_USER_SET_INFLIGHT_FD */
    struct vhost_user_inflight_desc inflight_desc;
    /* VHOST_USER_SET_LOG_BASE */
    struct vhost_user_log log;
};

#ifdef __cplusplus
}
#endif
