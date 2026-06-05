// Auto-included rename layer for the in-tree
// (contrib/ydb/core/nbs/cloud/...) copy of libvhost-server.
//
// The canonical copy lives at $S/cloud/contrib/vhost and exports the very
// same set of vhd_, virtio_ and virtq_ symbols (same C source files). When
// both archives end up in the same link closure the static linker silently
// keeps only one set of objects and the resulting binary may pick up
// ABI-incompatible code (the in-tree fork has a different struct
// vhd_bdev_info layout, for example). To make the two libraries
// link-compatible we move every externally-visible symbol exported by this
// archive into a private namespace by prepending nbs2_ to its external
// (assembler) name.
//
// Mechanism: GCC/Clang #pragma redefine_extname (Solaris extension, fully
// supported by both compilers and exposed via the _Pragma() operator so it
// can live inside a regular function-like macro). The pragma only retargets
// the external linkage name of a declared function or variable; it does not
// rewrite source-level tokens, so struct/union/enum tags, local identifiers
// and string literals that happen to share the same name are completely
// unaffected.
//
// IMPORTANT: each pragma applies only to declarations of the named symbol
// that the compiler sees AFTER the pragma in the current translation unit.
// To guarantee that holds in every TU, this header must be #include'd from
// every internal header that declares an exported symbol BEFORE that
// declaration. We do that directly from each such internal header (see the
// `#include "vhost/nbs2_rename.h"` lines at the top of memmap.h, event.h,
// memlog.h, bio.h, virtio/virt_queue.h, virtio/virtio_blk.h, etc.) so that
// the ordering is not sensitive to the include order chosen by callers.
// The public headers (vhost/types.h pulled by vhost/blockdev.h, server.h,
// fs.h) and platform.h also include this header so that every public
// consumer and every internal TU likewise sees the renames in scope before
// any declaration.

#pragma once

#define NBS2_STR_INNER(x) #x
#define NBS2_STR(x) NBS2_STR_INNER(x)

// NBS2_RENAME(sym) -- give the external (assembler) name of `sym` the
// `nbs2_` prefix. Use exactly once per externally-visible symbol exported
// by this library.
#define NBS2_RENAME(sym) \
    _Pragma(NBS2_STR(redefine_extname sym nbs2_##sym))

// ---- vhd_* functions -------------------------------------------------
NBS2_RENAME(vhd_add_io_handler)
NBS2_RENAME(vhd_add_rq_io_handler)
NBS2_RENAME(vhd_add_vhost_io_handler)
NBS2_RENAME(vhd_attach_io_handler)
NBS2_RENAME(vhd_bh_cancel)
NBS2_RENAME(vhd_bh_delete)
NBS2_RENAME(vhd_bh_new)
NBS2_RENAME(vhd_bh_schedule)
NBS2_RENAME(vhd_bh_schedule_oneshot)
NBS2_RENAME(vhd_blockdev_set_total_blocks)
NBS2_RENAME(vhd_cancel_queued_requests)
NBS2_RENAME(vhd_clear_eventfd)
NBS2_RENAME(vhd_complete_bio)
NBS2_RENAME(vhd_complete_work)
NBS2_RENAME(vhd_create_event_loop)
NBS2_RENAME(vhd_create_request_queue)
NBS2_RENAME(vhd_del_io_handler)
NBS2_RENAME(vhd_dequeue_request)
NBS2_RENAME(vhd_detach_io_handler)
NBS2_RENAME(vhd_enqueue_request)
NBS2_RENAME(vhd_free_event_loop)
NBS2_RENAME(vhd_get_bdev_io)
NBS2_RENAME(vhd_get_fs_io)
NBS2_RENAME(vhd_get_rq_for_vring)
NBS2_RENAME(vhd_get_rq_stat)
NBS2_RENAME(vhd_mark_gpa_range_dirty)
NBS2_RENAME(vhd_mark_range_dirty)
NBS2_RENAME(vhd_memlog_free)
NBS2_RENAME(vhd_memlog_new)
NBS2_RENAME(vhd_memmap_add_slot)
NBS2_RENAME(vhd_memmap_del_slot)
NBS2_RENAME(vhd_memmap_dup)
NBS2_RENAME(vhd_memmap_dup_remap)
NBS2_RENAME(vhd_memmap_max_memslots)
NBS2_RENAME(vhd_memmap_new)
NBS2_RENAME(vhd_memmap_ref)
NBS2_RENAME(vhd_memmap_unref)
NBS2_RENAME(vhd_register_blockdev)
NBS2_RENAME(vhd_register_fs)
NBS2_RENAME(vhd_register_fs_mq)
NBS2_RENAME(vhd_release_request_queue)
NBS2_RENAME(vhd_run_event_loop)
NBS2_RENAME(vhd_run_in_ctl)
NBS2_RENAME(vhd_run_in_rq)
NBS2_RENAME(vhd_run_queue)
NBS2_RENAME(vhd_set_eventfd)
NBS2_RENAME(vhd_start_vhost_server)
NBS2_RENAME(vhd_stop_queue)
NBS2_RENAME(vhd_stop_vhost_server)
NBS2_RENAME(vhd_submit_ctl_work_and_wait)
NBS2_RENAME(vhd_submit_work_and_wait)
NBS2_RENAME(vhd_terminate_event_loop)
NBS2_RENAME(vhd_unregister_blockdev)
NBS2_RENAME(vhd_unregister_fs)
NBS2_RENAME(vhd_vdev_get_priv)
NBS2_RENAME(vhd_vdev_get_queue_stat)
NBS2_RENAME(vhd_vdev_init_server)
NBS2_RENAME(vhd_vdev_stop_server)
NBS2_RENAME(vhd_vring_dec_in_flight)
NBS2_RENAME(vhd_vring_inc_in_flight)

// ---- virtio_* functions ----------------------------------------------
NBS2_RENAME(virtio_blk_destroy_dev)
NBS2_RENAME(virtio_blk_dispatch_requests)
NBS2_RENAME(virtio_blk_get_config)
NBS2_RENAME(virtio_blk_get_features)
NBS2_RENAME(virtio_blk_get_total_blocks)
NBS2_RENAME(virtio_blk_handle_request)
NBS2_RENAME(virtio_blk_has_feature)
NBS2_RENAME(virtio_blk_init_dev)
NBS2_RENAME(virtio_blk_is_readonly)
NBS2_RENAME(virtio_blk_set_total_blocks)
NBS2_RENAME(virtio_free_iov)
NBS2_RENAME(virtio_fs_dispatch_requests)
NBS2_RENAME(virtio_fs_init_dev)
NBS2_RENAME(virtio_iov_get_head)
NBS2_RENAME(virtio_virtq_get_stat)
NBS2_RENAME(virtio_virtq_init)
NBS2_RENAME(virtio_virtq_release)

// ---- virtq_* functions -----------------------------------------------
NBS2_RENAME(virtq_dequeue_many)
NBS2_RENAME(virtq_is_broken)
NBS2_RENAME(virtq_push)
NBS2_RENAME(virtq_set_notify_fd)

// ---- externals without a vhd_/virtio_/virtq_ prefix ------------------
NBS2_RENAME(abort_request)
NBS2_RENAME(g_log_fn)
NBS2_RENAME(gpa_range_to_ptr)
NBS2_RENAME(init_platform_page_size)
NBS2_RENAME(mark_broken)
NBS2_RENAME(platform_page_size)
NBS2_RENAME(ptr_to_gpa)
NBS2_RENAME(uva_to_ptr)
