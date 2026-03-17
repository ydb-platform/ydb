from .sd_id128 cimport sd_id128_t
from libc.stdint cimport uint64_t


cdef extern from "sys/uio.h":
    cdef struct iovec:
        void *iov_base
        size_t iov_len

cdef extern from "<systemd/sd-journal.h>" nogil:
    int sd_journal_sendv(const iovec *iov, int n)

    ctypedef void* sd_journal

    ctypedef enum:
        SD_JOURNAL_LOCAL_ONLY,
        SD_JOURNAL_RUNTIME_ONLY,
        SD_JOURNAL_SYSTEM,
        SD_JOURNAL_CURRENT_USER,
        SD_JOURNAL_SYSTEM_ONLY

    ctypedef enum:
        SD_JOURNAL_NOP,
        SD_JOURNAL_APPEND,
        SD_JOURNAL_INVALIDATE

    int sd_journal_stream_fd(const char *identifier, int priority, int level_prefix)

    int sd_journal_open(sd_journal **ret, int flags)
    int sd_journal_open_directory(sd_journal **ret, const char *path, int flags)
    int sd_journal_open_files(sd_journal **ret, const char **paths, int flags)
    int sd_journal_open_container(sd_journal **ret, const char *machine, int flags)
    void sd_journal_close(sd_journal *j)

    int sd_journal_previous(sd_journal *j)
    int sd_journal_next(sd_journal *j)

    int sd_journal_previous_skip(sd_journal *j, uint64_t skip)
    int sd_journal_next_skip(sd_journal *j, uint64_t skip)

    int sd_journal_get_realtime_usec(sd_journal *j, uint64_t *ret)
    int sd_journal_get_monotonic_usec(sd_journal *j, uint64_t *ret, sd_id128_t *ret_boot_id)

    int sd_journal_set_data_threshold(sd_journal *j, size_t sz)
    int sd_journal_get_data_threshold(sd_journal *j, size_t *sz)

    int sd_journal_enumerate_data(sd_journal *j, const void **data, size_t *l)
    void sd_journal_restart_data(sd_journal *j)

    int sd_journal_add_match(sd_journal *j, const void *data, size_t size)
    int sd_journal_add_disjunction(sd_journal *j)
    int sd_journal_add_conjunction(sd_journal *j)
    void sd_journal_flush_matches(sd_journal *j)

    int sd_journal_seek_head(sd_journal *j)
    int sd_journal_seek_tail(sd_journal *j)
    int sd_journal_seek_monotonic_usec(sd_journal *j, sd_id128_t boot_id, uint64_t usec)
    int sd_journal_seek_realtime_usec(sd_journal *j, uint64_t usec)
    int sd_journal_seek_cursor(sd_journal *j, const char *cursor)

    int sd_journal_get_cursor(sd_journal *j, char **cursor)
    int sd_journal_test_cursor(sd_journal *j, const char *cursor)

    int sd_journal_get_cutoff_realtime_usec(sd_journal *j, uint64_t *frm, uint64_t *to)
    int sd_journal_get_cutoff_monotonic_usec(sd_journal *j, const sd_id128_t boot_id, uint64_t *frm, uint64_t *to)

    int sd_journal_get_usage(sd_journal *j, uint64_t *bytes)

    int sd_journal_query_unique(sd_journal *j, const char *field)

    int sd_journal_enumerate_unique(sd_journal *j, const void **data, size_t *l)

    void sd_journal_restart_unique(sd_journal *j)

    int sd_journal_get_fd(sd_journal *j)
    int sd_journal_get_events(sd_journal *j)
    int sd_journal_get_timeout(sd_journal *j, uint64_t *timeout_usec)
    int sd_journal_process(sd_journal *j)
    int sd_journal_wait(sd_journal *j, uint64_t timeout_usec)
    int sd_journal_reliable_fd(sd_journal *j)

    int sd_journal_get_catalog(sd_journal *j, char **text)
    int sd_journal_get_catalog_for_message_id(sd_id128_t id, char **text)


__all__ = (
    "SD_JOURNAL_APPEND",
    "SD_JOURNAL_CURRENT_USER",
    "SD_JOURNAL_INVALIDATE",
    "SD_JOURNAL_LOCAL_ONLY",
    "SD_JOURNAL_NOP",
    "SD_JOURNAL_RUNTIME_ONLY",
    "SD_JOURNAL_SYSTEM",
    "SD_JOURNAL_SYSTEM_ONLY",
    "sd_journal_add_conjunction",
    "sd_journal_add_disjunction",
    "sd_journal_add_match",
    "sd_journal_close",
    "sd_journal_enumerate_data",
    "sd_journal_enumerate_unique",
    "sd_journal_flush_matches",
    "sd_journal_get_catalog",
    "sd_journal_get_catalog_for_message_id",
    "sd_journal_get_cursor",
    "sd_journal_get_cutoff_monotonic_usec",
    "sd_journal_get_cutoff_realtime_usec",
    "sd_journal_get_data_threshold",
    "sd_journal_get_events",
    "sd_journal_get_fd",
    "sd_journal_get_monotonic_usec",
    "sd_journal_get_realtime_usec",
    "sd_journal_get_timeout",
    "sd_journal_get_usage",
    "sd_journal_next",
    "sd_journal_next_skip",
    "sd_journal_open",
    "sd_journal_open_container",
    "sd_journal_open_directory",
    "sd_journal_open_files",
    "sd_journal_previous",
    "sd_journal_previous_skip",
    "sd_journal_process",
    "sd_journal_query_unique",
    "sd_journal_reliable_fd",
    "sd_journal_restart_data",
    "sd_journal_restart_unique",
    "sd_journal_seek_cursor",
    "sd_journal_seek_head",
    "sd_journal_seek_monotonic_usec",
    "sd_journal_seek_realtime_usec",
    "sd_journal_seek_tail",
    "sd_journal_sendv",
    "sd_journal_set_data_threshold",
    "sd_journal_stream_fd",
    "sd_journal_test_cursor",
    "sd_journal_wait",
)
