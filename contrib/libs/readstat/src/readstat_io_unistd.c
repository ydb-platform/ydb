
#include <fcntl.h>
#include <stdlib.h>
#include <wchar.h>

#if defined _WIN32
#   include <windows.h>
#   include <io.h>
#endif

#if !defined(_MSC_VER)
#   include <unistd.h>
#else
#define open _open
#define read _read
#define close _close
#endif

#if defined _WIN32 || defined __CYGWIN__
#define UNISTD_OPEN_OPTIONS O_RDONLY | O_BINARY
#elif defined _AIX
#define UNISTD_OPEN_OPTIONS O_RDONLY | O_LARGEFILE
#else
#define UNISTD_OPEN_OPTIONS O_RDONLY
#endif

#if defined _WIN32
#define lseek _lseeki64
#elif defined _AIX
#define lseek lseek64
#endif

#include "readstat.h"
#include "readstat_io_unistd.h"

int open_with_unicode(const char *path, int options)
{
#if defined _WIN32
    const int buffer_size = MultiByteToWideChar(CP_UTF8, 0, path, -1, NULL, 0);

    if(buffer_size <= 0)
        return -1;

    wchar_t* wpath = malloc((buffer_size + 1) * sizeof(wchar_t));
    const int res = MultiByteToWideChar(CP_UTF8, 0, path, -1, wpath, buffer_size);
    wpath[buffer_size] = 0;

    if(res <= 0)
    {
        free(wpath);
        return -1;
    }

    int fd = _wopen(wpath, options);

    free(wpath);
    return fd;
#else
    return open(path, options);
#endif
}

int unistd_open_handler(const char *path, void *io_ctx) {
    int fd = open_with_unicode(path, UNISTD_OPEN_OPTIONS);
    ((unistd_io_ctx_t*) io_ctx)->fd = fd;
    return fd;
}

int unistd_close_handler(void *io_ctx) {
    int fd = ((unistd_io_ctx_t*) io_ctx)->fd;
    if (fd != -1)
        return close(fd);
    else
        return 0;
}

readstat_off_t unistd_seek_handler(readstat_off_t offset,
        readstat_io_flags_t whence, void *io_ctx) {
    int flag = 0;
    switch(whence) {
        case READSTAT_SEEK_SET:
            flag = SEEK_SET;
            break;
        case READSTAT_SEEK_CUR:
            flag = SEEK_CUR;
            break;
        case READSTAT_SEEK_END:
            flag = SEEK_END;
            break;
        default:
            return -1;
    }
    int fd = ((unistd_io_ctx_t*) io_ctx)->fd;
    return lseek(fd, offset, flag);
}

ssize_t unistd_read_handler(void *buf, size_t nbyte, void *io_ctx) {
    int fd = ((unistd_io_ctx_t*) io_ctx)->fd;
    ssize_t out = read(fd, buf, nbyte);
    return out;
}

readstat_error_t unistd_update_handler(long file_size, 
        readstat_progress_handler progress_handler, void *user_ctx,
        void *io_ctx) {
    if (!progress_handler)
        return READSTAT_OK;

    int fd = ((unistd_io_ctx_t*) io_ctx)->fd;
    readstat_off_t current_offset = lseek(fd, 0, SEEK_CUR);

    if (current_offset == -1)
        return READSTAT_ERROR_SEEK;

    if (progress_handler(1.0 * current_offset / file_size, user_ctx))
        return READSTAT_ERROR_USER_ABORT;

    return READSTAT_OK;
}

readstat_error_t unistd_io_init(readstat_parser_t *parser) {
    readstat_error_t retval = READSTAT_OK;
    unistd_io_ctx_t *io_ctx = NULL;

    if ((retval = readstat_set_open_handler(parser, unistd_open_handler)) != READSTAT_OK)
        return retval;

    if ((retval = readstat_set_close_handler(parser, unistd_close_handler)) != READSTAT_OK)
        return retval;

    if ((retval = readstat_set_seek_handler(parser, unistd_seek_handler)) != READSTAT_OK)
        return retval;

    if ((retval = readstat_set_read_handler(parser, unistd_read_handler)) != READSTAT_OK)
        return retval;

    if ((readstat_set_update_handler(parser, unistd_update_handler)) != READSTAT_OK)
        return retval;

    io_ctx = calloc(1, sizeof(unistd_io_ctx_t));
    io_ctx->fd = -1;

    retval = readstat_set_io_ctx(parser, (void*) io_ctx);
    parser->io->io_ctx_needs_free = 1;

    return retval;
}
