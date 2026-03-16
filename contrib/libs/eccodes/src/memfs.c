/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */


#include "eccodes_config.h"
#ifdef ECCODES_HAVE_FMEMOPEN
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "eccodes_windef.h"

struct entry {
    const char* path;
    const unsigned char* content;
    size_t length;
};

/* The code will be added below: */
/* Start generated code */
/* <MARKER> */
/* End generated code */

#if !defined(ECCODES_HAVE_FMEMOPEN)

#if defined(ECCODES_HAVE_FUNOPEN)
typedef struct mem_file {
    const char* buffer;
    size_t size;
    size_t pos;
} mem_file;

static int read_mem(void *data, char * buf, int len) {
    mem_file* f = (mem_file*)data;
    int n = len;

    if(f->pos + n > f->size) {
        n = f->size - f->pos;
    }

    memcpy(buf, f->buffer + f->pos, n);

    f->pos += n;
    return n;
}

static int write_mem(void* data, const char* buf, int len) {
    mem_file* f = (mem_file*)data;
    return -1;
}

static fpos_t seek_mem(void *data, fpos_t pos, int whence) {
    mem_file* f = (mem_file*)data;
    long newpos = 0;

    switch (whence) {
    case SEEK_SET:
        newpos = (long)pos;
        break;

    case SEEK_CUR:
        newpos = (long)f->pos + (long)pos;
        break;

    case SEEK_END:
        newpos = (long)f->size -  (long)pos;
        break;

    default:
        return -1;
        break;
  }

  if(newpos < 0) { newpos = 0; }
  if(newpos > f->size) { newpos = f->size; }

  f->pos = newpos;
  return newpos;
}

static int close_mem(void *data) {
    mem_file* f = (mem_file*)data;
    free(f);
    return 0;
}

static FILE* fmemopen(const char* buffer, size_t size, const char* mode){
    mem_file* f = (mem_file*)calloc(sizeof(mem_file), 1);
    if(!f) return NULL;

    f->buffer = buffer;
    f->size = size;

    return funopen(f, &read_mem, &write_mem, &seek_mem, &close_mem);
}

#else /* defined(ECCODES_HAVE_FUNOPEN) */

#if defined(ECCODES_ON_WINDOWS)

#include <io.h>
#include <fcntl.h>
#include <windows.h>

static FILE *fmemopen(void* buffer, size_t size, const char* mode) {
    char path[MAX_PATH - 13];
    if (!GetTempPath(sizeof(path), path))
        return NULL;

    char filename[MAX_PATH + 1];
    if (!GetTempFileName(path, "eccodes", 0, filename))
        return NULL;

    HANDLE h = CreateFile(filename,
                          GENERIC_READ | GENERIC_WRITE,
                          0,
                          NULL,
                          OPEN_ALWAYS,
                          FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE,
                          NULL);

    if (h == INVALID_HANDLE_VALUE)
        return NULL;

    int fd = _open_osfhandle((intptr_t)h, _O_RDWR);
    if (fd < 0) {
        CloseHandle(h);
        return NULL;
    }

    FILE* f = _fdopen(fd, "w+");
    if (!f) {
        _close(fd);
        return NULL;
    }

    fwrite(buffer, size, 1, f);
    rewind(f);
    return f;
}

#else /* defined(ECCODES_ON_WINDOWS) */
#include <stdio.h>
/* On old unix systems */

static FILE *fmemopen(void* buffer, size_t size, const char* mode) {

    FILE* f = tmpfile();
    if (!f) {
        return NULL;
    }

    fwrite(buffer, size, 1, f);
    rewind(f);
    return f;
}

#endif /* defined(ECCODES_ON_WINDOWS) */

#endif /* defined(ECCODES_HAVE_FUNOPEN) */
#endif /* !defined(ECCODES_HAVE_FMEMOPEN) */

static size_t entries_count = sizeof(entries)/sizeof(entries[0]);

static const unsigned char* find(const char* path, size_t* length) {
    size_t i;

    for(i = 0; i < entries_count; i++) {
        if(strcmp(path, entries[i].path) == 0) {
            /*printf("Found in MEMFS %s\\n", path);*/
            *length = entries[i].length;
            return entries[i].content;
        }
    }

    return NULL;
}

int codes_memfs_exists(const char* path) {
    size_t dummy;
    return find(path, &dummy) != NULL;
}

FILE* codes_memfs_open(const char* path) {
    size_t size;
    const unsigned char* mem = find(path, &size);
    if(!mem) {
        return NULL;
    }
    return fmemopen((void*)mem, size, "r");
}
