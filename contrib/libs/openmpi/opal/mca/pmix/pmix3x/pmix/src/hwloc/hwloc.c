/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2017      Inria.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>
#include <sys/mman.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#if HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "src/util/error.h"
#include "src/util/fd.h"
#include "src/util/path.h"
#include "src/mca/bfrops/bfrops_types.h"
#include "src/server/pmix_server_ops.h"
#include "hwloc-internal.h"

#if PMIX_HAVE_HWLOC

#if HWLOC_API_VERSION >= 0x20000
#include <hwloc/shmem.h>
#endif


PMIX_EXPORT hwloc_topology_t pmix_hwloc_topology = NULL;
static bool external_topology = false;

#if HWLOC_API_VERSION >= 0x20000
static size_t shmemsize = 0;
static size_t shmemaddr;
static char *shmemfile = NULL;
static int shmemfd = -1;

static int parse_map_line(const char *line,
                          unsigned long *beginp,
                          unsigned long *endp,
                          pmix_hwloc_vm_map_kind_t *kindp);
static int use_hole(unsigned long holebegin,
                    unsigned long holesize,
                    unsigned long *addrp,
                    unsigned long size);
static int find_hole(pmix_hwloc_vm_hole_kind_t hkind,
                     size_t *addrp,
                     size_t size);
static int enough_space(const char *filename,
                        size_t space_req,
                        uint64_t *space_avail,
                        bool *result);
#endif

static int set_flags(hwloc_topology_t topo, unsigned int flags)
{
    #if HWLOC_API_VERSION < 0x20000
            flags = HWLOC_TOPOLOGY_FLAG_IO_DEVICES;
    #else
            int ret = hwloc_topology_set_io_types_filter(topo, HWLOC_TYPE_FILTER_KEEP_IMPORTANT);
            if (0 != ret) return ret;
    #endif
    if (0 != hwloc_topology_set_flags(topo, flags)) {
        return PMIX_ERR_INIT;
    }
    return PMIX_SUCCESS;
}
#endif // have_hwloc

pmix_status_t pmix_hwloc_get_topology(pmix_info_t *info, size_t ninfo)
{
#if PMIX_HAVE_HWLOC
    size_t n;
    bool save_xml_v1 = false;
    bool save_xml_v2 = false;
#if HWLOC_API_VERSION < 0x20000
    bool save_xml_v2_reqd = false;
#endif
    bool share_topo = false;
    bool share_reqd = false;
    pmix_kval_t *kp2;
    char *xml;
    int sz;
    pmix_status_t rc;
#if HWLOC_API_VERSION >= 0x20000
    pmix_hwloc_vm_hole_kind_t hole = VM_HOLE_BIGGEST;
#endif

    if (NULL == info || 0 == ninfo) {
        if (0 != hwloc_topology_init(&pmix_hwloc_topology)) {
            return PMIX_ERR_INIT;
        }

        if (0 != set_flags(pmix_hwloc_topology, 0)) {
            hwloc_topology_destroy(pmix_hwloc_topology);
            return PMIX_ERR_INIT;
        }

        if (0 != hwloc_topology_load(pmix_hwloc_topology)) {
            PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
            hwloc_topology_destroy(pmix_hwloc_topology);
            return PMIX_ERR_NOT_SUPPORTED;
        }
        return PMIX_SUCCESS;
    }

    /* check for directives */
    for (n=0; n < ninfo; n++) {
        if (0 == strncmp(info[n].key, PMIX_TOPOLOGY, PMIX_MAX_KEYLEN)) {
            /* if the pointer is NULL, then they want us to
             * get the topology - it not NULL, then they
             * are giving us the topology */
            if (NULL != pmix_hwloc_topology) {
                /* cannot have two topologies */
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                return PMIX_ERR_BAD_PARAM;
            }
            if (NULL != info[n].value.data.ptr) {
                pmix_hwloc_topology = (hwloc_topology_t)info[n].value.data.ptr;
                external_topology = true;
            } else {
                if (0 != hwloc_topology_init(&pmix_hwloc_topology)) {
                    return PMIX_ERR_INIT;
                }

                if (0 != set_flags(pmix_hwloc_topology, 0)) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERR_INIT;
                }

                if (0 != hwloc_topology_load(pmix_hwloc_topology)) {
                    PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERR_NOT_SUPPORTED;
                }
            }
        } else if (0 == strncmp(info[n].key, PMIX_HWLOC_XML_V1, PMIX_MAX_KEYLEN)) {
            /* if the string pointer is NULL or empty, then they
             * want us to create it and store it for later sharing.
             * if non-NULL, then this is the topology we are to use */
            if (NULL == info[n].value.data.string) {
                save_xml_v1 = true;
            } else if (NULL != pmix_hwloc_topology) {
                /* cannot have two topologies */
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                return PMIX_ERR_BAD_PARAM;
            } else {
                /* load the topology */
                if (0 != hwloc_topology_init(&pmix_hwloc_topology)) {
                    return PMIX_ERROR;
                }
                if (0 != hwloc_topology_set_xmlbuffer(pmix_hwloc_topology,
                                                      info[n].value.data.string,
                                                      strlen(info[n].value.data.string))) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERROR;
                }
                /* since we are loading this from an external source, we have to
                 * explicitly set a flag so hwloc sets things up correctly
                 */
                if (0 != set_flags(pmix_hwloc_topology, HWLOC_TOPOLOGY_FLAG_IS_THISSYSTEM)) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERROR;
                }
                /* now load the topology */
                if (0 != hwloc_topology_load(pmix_hwloc_topology)) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERROR;
                }
                /* store the string */
                kp2 = PMIX_NEW(pmix_kval_t);
                if (NULL == kp2) {
                    return PMIX_ERR_NOMEM;
                }
                kp2->key = strdup(info[n].key);
                PMIX_VALUE_XFER(rc, kp2->value, &info[n].value);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp2);
                    return rc;
                }
                pmix_list_append(&pmix_server_globals.gdata, &kp2->super);
            }
        } else if (0 == strncmp(info[n].key, PMIX_HWLOC_XML_V2, PMIX_MAX_KEYLEN)) {
            /* if the string pointer is NULL or empty, then they
             * want us to create it and store it for later sharing.
             * if non-NULL, then this is the topology we are to use */
            if (NULL == info[n].value.data.string) {
                save_xml_v2 = true;
#if HWLOC_API_VERSION < 0x20000
                save_xml_v2_reqd = PMIX_INFO_REQUIRED(&info[n]);
#endif
            } else if (NULL != pmix_hwloc_topology) {
                /* cannot have two topologies */
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                return PMIX_ERR_BAD_PARAM;
            } else {
                /* load the topology */
                if (0 != hwloc_topology_init(&pmix_hwloc_topology)) {
                    return PMIX_ERROR;
                }
                if (0 != hwloc_topology_set_xmlbuffer(pmix_hwloc_topology,
                                                      info[n].value.data.string,
                                                      strlen(info[n].value.data.string))) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERROR;
                }
                /* since we are loading this from an external source, we have to
                 * explicitly set a flag so hwloc sets things up correctly
                 */
                if (0 != set_flags(pmix_hwloc_topology, HWLOC_TOPOLOGY_FLAG_IS_THISSYSTEM)) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERROR;
                }
                /* now load the topology */
                if (0 != hwloc_topology_load(pmix_hwloc_topology)) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERROR;
                }
                /* store the string */
                kp2 = PMIX_NEW(pmix_kval_t);
                if (NULL == kp2) {
                    return PMIX_ERR_NOMEM;
                }
                kp2->key = strdup(info[n].key);
                PMIX_VALUE_XFER(rc, kp2->value, &info[n].value);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp2);
                    return rc;
                }
                pmix_list_append(&pmix_server_globals.gdata, &kp2->super);
            }
        } else if (0 == strncmp(info[n].key, PMIX_TOPOLOGY_FILE, PMIX_MAX_KEYLEN)) {
            if (NULL == info[n].value.data.string) {
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                return PMIX_ERR_BAD_PARAM;
            } else if (NULL != pmix_hwloc_topology) {
                /* cannot have two topologies */
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                return PMIX_ERR_BAD_PARAM;
            } else {
                if (0 != hwloc_topology_init(&pmix_hwloc_topology)) {
                    return PMIX_ERR_NOT_SUPPORTED;
                }
                if (0 != hwloc_topology_set_xml(pmix_hwloc_topology, info[n].value.data.string)) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERR_NOT_SUPPORTED;
                }
                /* since we are loading this from an external source, we have to
                 * explicitly set a flag so hwloc sets things up correctly
                 */
                if (0 != set_flags(pmix_hwloc_topology, HWLOC_TOPOLOGY_FLAG_IS_THISSYSTEM)) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                    return PMIX_ERR_NOT_SUPPORTED;
                }
                if (0 != hwloc_topology_load(pmix_hwloc_topology)) {
                    hwloc_topology_destroy(pmix_hwloc_topology);
                     return PMIX_ERR_NOT_SUPPORTED;
                }
                /* store the filename */
                kp2 = PMIX_NEW(pmix_kval_t);
                if (NULL == kp2) {
                    return PMIX_ERR_NOMEM;
                }
                kp2->key = strdup(info[n].key);
                PMIX_VALUE_XFER(rc, kp2->value, &info[n].value);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp2);
                    return rc;
                }
                pmix_list_append(&pmix_server_globals.gdata, &kp2->super);
            }
        } else if (0 == strncmp(info[n].key, PMIX_HWLOC_SHARE_TOPO, PMIX_MAX_KEYLEN)) {
            share_topo = PMIX_INFO_TRUE(&info[n]);
            share_reqd = PMIX_INFO_IS_REQUIRED(&info[n]);
        } else if (0 == strncmp(info[n].key, PMIX_HWLOC_HOLE_KIND, PMIX_MAX_KEYLEN)) {
#if HWLOC_API_VERSION >= 0x20000
            if (0 == strcasecmp(info[n].value.data.string, "none")) {
                hole = VM_HOLE_NONE;
            } else if (0 == strcasecmp(info[n].value.data.string, "begin")) {
                hole = VM_HOLE_BEGIN;
            } else if (0 == strcasecmp(info[n].value.data.string, "biggest")) {
                hole = VM_HOLE_BIGGEST;
            } else if (0 == strcasecmp(info[n].value.data.string, "libs")) {
                hole = VM_HOLE_IN_LIBS;
            } else if (0 == strcasecmp(info[n].value.data.string, "heap")) {
                hole = VM_HOLE_AFTER_HEAP;
            } else if (0 == strcasecmp(info[n].value.data.string, "stack")) {
                hole = VM_HOLE_BEFORE_STACK;
            } else {
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                return PMIX_ERR_BAD_PARAM;
            }
#endif
        }
    }

    if (save_xml_v1) {
        /* create the XML string */
#if HWLOC_API_VERSION >= 0x20000
        if (0 != hwloc_topology_export_xmlbuffer(pmix_hwloc_topology, &xml, &sz, HWLOC_TOPOLOGY_EXPORT_XML_FLAG_V1)) {
            PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
            return PMIX_ERR_NOT_SUPPORTED;
        }
#else
        if (0 != hwloc_topology_export_xmlbuffer(pmix_hwloc_topology, &xml, &sz)) {
            PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
            return PMIX_ERR_NOT_SUPPORTED;
        }
#endif
        /* store it */
        kp2 = PMIX_NEW(pmix_kval_t);
        if (NULL == kp2) {
            return PMIX_ERR_NOMEM;
        }
        kp2->key = strdup(PMIX_HWLOC_XML_V1);
        PMIX_VALUE_LOAD(kp2->value, xml, PMIX_STRING);
        hwloc_free_xmlbuffer(pmix_hwloc_topology, xml);
        pmix_list_append(&pmix_server_globals.gdata, &kp2->super);
    }
    if (save_xml_v2) {
        /* create the XML string */
#if HWLOC_API_VERSION >= 0x20000
        if (0 != hwloc_topology_export_xmlbuffer(pmix_hwloc_topology, &xml, &sz, 0)) {
            PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
            return PMIX_ERR_NOT_SUPPORTED;
        }
        /* store it */
        kp2 = PMIX_NEW(pmix_kval_t);
        if (NULL == kp2) {
            return PMIX_ERR_NOMEM;
        }
        kp2->key = strdup(PMIX_HWLOC_XML_V1);
        PMIX_VALUE_LOAD(kp2->value, xml, PMIX_STRING);
        hwloc_free_xmlbuffer(pmix_hwloc_topology, xml);
        pmix_list_append(&pmix_server_globals.gdata, &kp2->super);
#else
        if (save_xml_v2_reqd) {
            PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
            return PMIX_ERR_NOT_SUPPORTED;
        }
#endif
    }

    if (share_topo) {
#if HWLOC_API_VERSION < 0x20000
        if (share_reqd) {
            return PMIX_ERR_NOT_SUPPORTED;
        }
#else
        pmix_status_t rc;
        bool space_available = false;
        uint64_t amount_space_avail = 0;

        if (VM_HOLE_NONE == hole) {
            return PMIX_SUCCESS;
        }

        /* get the size of the topology shared memory segment */
        if (0 != hwloc_shmem_topology_get_length(pmix_hwloc_topology, &shmemsize, 0)) {
            if (share_reqd) {
                PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
                return PMIX_ERR_NOT_SUPPORTED;
            }
            return PMIX_SUCCESS;
        }

        if (PMIX_SUCCESS != (rc = find_hole(hole, &shmemaddr, shmemsize))) {
            /* we couldn't find a hole, so don't use the shmem support */
            if (share_reqd) {
                PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
                return PMIX_ERR_NOT_SUPPORTED;
            }
            return PMIX_SUCCESS;
        }
        /* create the shmem file in our session dir so it
         * will automatically get cleaned up */
        asprintf(&shmemfile, "%s/hwloc.sm", pmix_server_globals.tmpdir);
        /* let's make sure we have enough space for the backing file */
        if (PMIX_SUCCESS != (rc = enough_space(shmemfile, shmemsize,
                                               &amount_space_avail,
                                               &space_available))) {
            free(shmemfile);
            shmemfile = NULL;
            if (share_reqd) {
                PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
                return PMIX_ERR_NOT_SUPPORTED;
            } else {
                return PMIX_SUCCESS;
            }
        }
        if (!space_available) {
            free(shmemfile);
            shmemfile = NULL;
            if (share_reqd) {
                PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
                return PMIX_ERR_NOT_SUPPORTED;
            } else {
                return PMIX_SUCCESS;
            }
        }
        /* enough space is available, so create the segment */
        if (-1 == (shmemfd = open(shmemfile, O_CREAT | O_RDWR, 0600))) {
            free(shmemfile);
            shmemfile = NULL;
            if (share_reqd) {
                PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
                return PMIX_ERR_NOT_SUPPORTED;
            } else {
                return PMIX_SUCCESS;
            }
        }
        /* ensure nobody inherits this fd */
        pmix_fd_set_cloexec(shmemfd);
        /* populate the shmem segment with the topology */
        if (0 != (rc = hwloc_shmem_topology_write(pmix_hwloc_topology, shmemfd, 0,
                                                  (void*)shmemaddr, shmemsize, 0))) {
            unlink(shmemfile);
            free(shmemfile);
            shmemfile = NULL;
            close(shmemfd);
            shmemfd = -1;
            if (share_reqd) {
                PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
                return PMIX_ERR_NOT_SUPPORTED;
            } else {
                return PMIX_SUCCESS;
            }
        }
        /* store the rendezvous info */
        kp2 = PMIX_NEW(pmix_kval_t);
        if (NULL == kp2) {
            return PMIX_ERR_NOMEM;
        }
        kp2->key = strdup(PMIX_HWLOC_SHMEM_FILE);
        PMIX_VALUE_CREATE(kp2->value, 1);
        PMIX_VALUE_LOAD(kp2->value, shmemfile, PMIX_STRING);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(kp2);
            return rc;
        }
        pmix_list_append(&pmix_server_globals.gdata, &kp2->super);
        kp2 = PMIX_NEW(pmix_kval_t);
        if (NULL == kp2) {
            return PMIX_ERR_NOMEM;
        }
        kp2->key = strdup(PMIX_HWLOC_SHMEM_ADDR);
        PMIX_VALUE_CREATE(kp2->value, 1);
        PMIX_VALUE_LOAD(kp2->value, &shmemaddr, PMIX_SIZE);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(kp2);
            return rc;
        }
        pmix_list_append(&pmix_server_globals.gdata, &kp2->super);
        kp2 = PMIX_NEW(pmix_kval_t);
        if (NULL == kp2) {
            return PMIX_ERR_NOMEM;
        }
        kp2->key = strdup(PMIX_HWLOC_SHMEM_SIZE);
        PMIX_VALUE_CREATE(kp2->value, 1);
        PMIX_VALUE_LOAD(kp2->value, &shmemsize, PMIX_SIZE);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(kp2);
            return rc;
        }
        pmix_list_append(&pmix_server_globals.gdata, &kp2->super);


#endif
    }

    return PMIX_SUCCESS;
#else  // PMIX_HAVE_HWLOC
    return PMIX_SUCCESS;
#endif
}

void pmix_hwloc_cleanup(void)
{
#if PMIX_HAVE_HWLOC
#if HWLOC_API_VERSION >= 0x20000
    if (NULL != shmemfile) {
        unlink(shmemfile);
        free(shmemfile);
    }
    if (0 <= shmemfd) {
        close(shmemfd);
    }
#endif
    if (NULL != pmix_hwloc_topology && !external_topology) {
        hwloc_topology_destroy(pmix_hwloc_topology);
    }
#endif
    return;
}

#if PMIX_HAVE_HWLOC
#if HWLOC_API_VERSION >= 0x20000

static int parse_map_line(const char *line,
                          unsigned long *beginp,
                          unsigned long *endp,
                          pmix_hwloc_vm_map_kind_t *kindp)
{
    const char *tmp = line, *next;
    unsigned long value;

    /* "beginaddr-endaddr " */
    value = strtoull(tmp, (char **) &next, 16);
    if (next == tmp) {
        return PMIX_ERROR;
    }

    *beginp = (unsigned long) value;

    if (*next != '-') {
        return PMIX_ERROR;
    }

     tmp = next + 1;

    value = strtoull(tmp, (char **) &next, 16);
    if (next == tmp) {
        return PMIX_ERROR;
    }
    *endp = (unsigned long) value;
    tmp = next;

    if (*next != ' ') {
        return PMIX_ERROR;
    }
    tmp = next + 1;

    /* look for ending absolute path */
    next = strchr(tmp, '/');
    if (next) {
        *kindp = VM_MAP_FILE;
    } else {
        /* look for ending special tag [foo] */
        next = strchr(tmp, '[');
        if (next) {
            if (!strncmp(next, "[heap]", 6)) {
                *kindp = VM_MAP_HEAP;
            } else if (!strncmp(next, "[stack]", 7)) {
                *kindp = VM_MAP_STACK;
            } else {
                char *end;
                if ((end = strchr(next, '\n')) != NULL) {
                    *end = '\0';
                }
                *kindp = VM_MAP_OTHER;
            }
        } else {
            *kindp = VM_MAP_ANONYMOUS;
        }
    }

    return PMIX_SUCCESS;
}

#define ALIGN2MB (2*1024*1024UL)

static int use_hole(unsigned long holebegin,
                    unsigned long holesize,
                    unsigned long *addrp,
                    unsigned long size)
{
    unsigned long aligned;
    unsigned long middle = holebegin+holesize/2;

    if (holesize < size) {
        return PMIX_ERROR;
    }

    /* try to align the middle of the hole on 64MB for POWER's 64k-page PMD */
    #define ALIGN64MB (64*1024*1024UL)
    aligned = (middle + ALIGN64MB) & ~(ALIGN64MB-1);
    if (aligned + size <= holebegin + holesize) {
        *addrp = aligned;
        return PMIX_SUCCESS;
    }

    /* try to align the middle of the hole on 2MB for x86 PMD */
    aligned = (middle + ALIGN2MB) & ~(ALIGN2MB-1);
    if (aligned + size <= holebegin + holesize) {
        *addrp = aligned;
        return PMIX_SUCCESS;
    }

    /* just use the end of the hole */
    *addrp = holebegin + holesize - size;
    return PMIX_SUCCESS;
}

static int find_hole(pmix_hwloc_vm_hole_kind_t hkind,
                     size_t *addrp, size_t size)
{
    unsigned long biggestbegin = 0;
    unsigned long biggestsize = 0;
    unsigned long prevend = 0;
    pmix_hwloc_vm_map_kind_t prevmkind = VM_MAP_OTHER;
    int in_libs = 0;
    FILE *file;
    char line[96];

    file = fopen("/proc/self/maps", "r");
    if (!file) {
        return PMIX_ERROR;
    }

    while (fgets(line, sizeof(line), file) != NULL) {
        unsigned long begin=0, end=0;
        pmix_hwloc_vm_map_kind_t mkind=VM_MAP_OTHER;

        if (!parse_map_line(line, &begin, &end, &mkind)) {
            switch (hkind) {
                case VM_HOLE_BEGIN:
                    fclose(file);
                    return use_hole(0, begin, addrp, size);

                case VM_HOLE_AFTER_HEAP:
                    if (prevmkind == VM_MAP_HEAP && mkind != VM_MAP_HEAP) {
                        /* only use HEAP when there's no other HEAP after it
                         * (there can be several of them consecutively).
                         */
                        fclose(file);
                        return use_hole(prevend, begin-prevend, addrp, size);
                    }
                    break;

                case VM_HOLE_BEFORE_STACK:
                    if (mkind == VM_MAP_STACK) {
                        fclose(file);
                        return use_hole(prevend, begin-prevend, addrp, size);
                    }
                    break;

                case VM_HOLE_IN_LIBS:
                    /* see if we are between heap and stack */
                    if (prevmkind == VM_MAP_HEAP) {
                        in_libs = 1;
                    }
                    if (mkind == VM_MAP_STACK) {
                        in_libs = 0;
                    }
                    if (!in_libs) {
                        /* we're not in libs, ignore this entry */
                        break;
                    }
                    /* we're in libs, consider this entry for searching the biggest hole below */
                    /* fallthrough */

                case VM_HOLE_BIGGEST:
                    if (begin-prevend > biggestsize) {
                        biggestbegin = prevend;
                        biggestsize = begin-prevend;
                    }
                    break;

                    default:
                        assert(0);
            }
        }

        while (!strchr(line, '\n')) {
            if (!fgets(line, sizeof(line), file)) {
                goto done;
            }
        }

        if (mkind == VM_MAP_STACK) {
          /* Don't go beyond the stack. Other VMAs are special (vsyscall, vvar, vdso, etc),
           * There's no spare room there. And vsyscall is even above the userspace limit.
           */
          break;
        }

        prevend = end;
        prevmkind = mkind;

    }

  done:
    fclose(file);
    if (hkind == VM_HOLE_IN_LIBS || hkind == VM_HOLE_BIGGEST) {
        return use_hole(biggestbegin, biggestsize, addrp, size);
    }

    return PMIX_ERROR;
}

static int enough_space(const char *filename,
                        size_t space_req,
                        uint64_t *space_avail,
                        bool *result)
{
    uint64_t avail = 0;
    size_t fluff = (size_t)(.05 * space_req);
    bool enough = false;
    char *last_sep = NULL;
    /* the target file name is passed here, but we need to check the parent
     * directory. store it so we can extract that info later. */
    char *target_dir = strdup(filename);
    int rc;

    if (NULL == target_dir) {
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        goto out;
    }
    /* get the parent directory */
    last_sep = strrchr(target_dir, PMIX_PATH_SEP[0]);
    *last_sep = '\0';
    /* now check space availability */
    if (PMIX_SUCCESS != (rc = pmix_path_df(target_dir, &avail))) {
        goto out;
    }
    /* do we have enough space? */
    if (avail >= space_req + fluff) {
        enough = true;
    }

out:
    if (NULL != target_dir) {
        free(target_dir);
    }
    *result = enough;
    *space_avail = avail;
    return rc;
}
#endif

#endif  // PMIX_HAVE_HWLOC
