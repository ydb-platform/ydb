/*
 * SPDX-License-Identifier: BSD-3-Clause
 * Copyright © 2009 CNRS
 * Copyright © 2009-2023 Inria.  All rights reserved.
 * Copyright © 2009-2013 Université Bordeaux
 * Copyright © 2009-2011 Cisco Systems, Inc.  All rights reserved.
 * See COPYING in top-level directory.
 */

/* Detect topology change: registering for power management changes and check
 * if for example hw.activecpu changed */

/* Apparently, Darwin people do not _want_ to provide binding functions.  */

#include "private/autogen/config.h"

#include <sys/types.h>
#include <sys/sysctl.h>
#include <stdlib.h>
#include <inttypes.h>

#include "hwloc.h"
#include "private/private.h"
#include "private/debug.h"

#define MAX_KINDS 8 /* only 2 needed as of 2022/01 */
struct hwloc_darwin_cpukinds {
  unsigned nr;
  struct hwloc_darwin_cpukind {
    char cluster_type;
    hwloc_bitmap_t cpuset;
#define HWLOC_DARWIN_COMPATIBLE_MAX 128
    char *compatible;
    int perflevel; /* -1 if unknown */
  } kinds[MAX_KINDS]; /* sorted by computing performance first, as sysctl hw.perflevels */
};

static void
hwloc__darwin_cpukinds_clear(struct hwloc_darwin_cpukinds *kinds)
{
  unsigned i;
  for(i=0; i<kinds->nr; i++) {
    hwloc_bitmap_free(kinds->kinds[i].cpuset);
    free(kinds->kinds[i].compatible);
  }
  kinds->nr = 0;
}

static struct hwloc_darwin_cpukind *
hwloc__darwin_cpukinds_add(struct hwloc_darwin_cpukinds *kinds,
                           char cluster_type, const char *compatible)
{
  if (kinds->nr == MAX_KINDS) {
    if (HWLOC_SHOW_ALL_ERRORS())
      fprintf(stderr, "hwloc/darwin: failed to add new cpukinds, already %u used\n", kinds->nr);
    return NULL;
  }

  kinds->kinds[kinds->nr].cluster_type = cluster_type;
  kinds->kinds[kinds->nr].compatible = compatible ? strdup(compatible) : NULL;
  kinds->kinds[kinds->nr].perflevel = -1;
  kinds->kinds[kinds->nr].cpuset = hwloc_bitmap_alloc();
  if (!kinds->kinds[kinds->nr].cpuset) {
    /* cancel this new kind, cpu won't be in any kind */
    free(kinds->kinds[kinds->nr].compatible);
    hwloc_bitmap_free(kinds->kinds[kinds->nr].cpuset);
    return NULL;
  }

  kinds->nr++;
  return &kinds->kinds[kinds->nr-1];
}

#if (defined HWLOC_HAVE_DARWIN_FOUNDATION) && (defined HWLOC_HAVE_DARWIN_IOKIT)

static void hwloc__darwin_cpukinds_add_cpu(struct hwloc_darwin_cpukinds *kinds,
                                           char cluster_type, const char *compatible,
                                           unsigned cpu)
{
  struct hwloc_darwin_cpukind *kind;
  unsigned i;

  for(i=0; i<kinds->nr; i++) {
    if (kinds->kinds[i].cluster_type == cluster_type) {
      if (compatible) {
        if (!kinds->kinds[i].compatible)
          kinds->kinds[i].compatible = strdup(compatible);
        else if (strcmp(kinds->kinds[i].compatible, compatible))
          fprintf(stderr, "hwloc/darwin/cpukinds: got a different compatible string inside same cluster type %c\n", cluster_type);
      }
      kind = &kinds->kinds[i];
      goto found;
    }
  }

  kind = hwloc__darwin_cpukinds_add(kinds, cluster_type, compatible);
  if (!kind)
    return;

 found:
  hwloc_bitmap_set(kind->cpuset, cpu);
}

#include <IOKit/IOKitLib.h>
#include <CoreFoundation/CoreFoundation.h>
#include <Availability.h>

#if (defined __MAC_OS_X_VERSION_MIN_REQUIRED) && (__MAC_OS_X_VERSION_MIN_REQUIRED < 120000)
#define kIOMainPortDefault kIOMasterPortDefault
#endif

static int hwloc__darwin_look_iokit_cpukinds(struct hwloc_darwin_cpukinds *kinds,
                                             int *matched_perflevels)
{
  io_registry_entry_t cpus_root;
  io_iterator_t cpus_iter;
  io_registry_entry_t cpus_child;
  kern_return_t kret;
  unsigned i;
#define DT_PLANE "IODeviceTree"
  io_string_t cpu_plane_string = DT_PLANE ":/cpus";
  io_name_t dt_plane_name = DT_PLANE;

  hwloc_debug("\nLooking at cpukinds under %s\n", (const char *) cpu_plane_string);

  cpus_root = IORegistryEntryFromPath(kIOMainPortDefault, cpu_plane_string);
  if (!cpus_root) {
    fprintf(stderr, "hwloc/darwin/cpukinds: failed to find %s\n", (const char *) cpu_plane_string);
    return -1;
  }

  kret = IORegistryEntryGetChildIterator(cpus_root, dt_plane_name, &cpus_iter);
  if (kret != KERN_SUCCESS) {
    if (HWLOC_SHOW_ALL_ERRORS())
      fprintf(stderr, "hwloc/darwin/cpukinds: failed to create iterator\n");
    IOObjectRelease(cpus_root);
    return -1;
  }

  while ((cpus_child = IOIteratorNext(cpus_iter)) != 0) {
    CFTypeRef ref;
    unsigned logical_cpu_id;
    char cluster_type;
    char compatible[HWLOC_DARWIN_COMPATIBLE_MAX+2]; /* room for two \0 at the end */

#ifdef HWLOC_DEBUG
    {
      /* get the name */
      io_name_t name;
      kret = IORegistryEntryGetNameInPlane(cpus_child, dt_plane_name, name);
      if (kret != KERN_SUCCESS) {
        hwloc_debug("failed to find cpu name\n");
      } else {
        hwloc_debug("looking at cpu `%s'\n", name);
      }
    }
#endif

    /* get logical-cpu-id */
    ref = IORegistryEntrySearchCFProperty(cpus_child, dt_plane_name, CFSTR("logical-cpu-id"), kCFAllocatorDefault, kNilOptions);
    if (!ref) {
      /* this may happen on old/x86 systems that aren't hybrid, don't warn */
      hwloc_debug("failed to find logical-cpu-id\n");
      continue;
    }
    if (CFGetTypeID(ref) != CFNumberGetTypeID()) {
      if (HWLOC_SHOW_ALL_ERRORS())
        fprintf(stderr, "hwloc/darwin/cpukinds: unexpected `logical-cpu-id' CF type %s\n",
                CFStringGetCStringPtr(CFCopyTypeIDDescription(CFGetTypeID(ref)), kCFStringEncodingUTF8));
      CFRelease(ref);
      continue;
    }
    {
      long long lld_value;
      if (!CFNumberGetValue(ref, kCFNumberLongLongType, &lld_value)) {
        if (HWLOC_SHOW_ALL_ERRORS())
          fprintf(stderr, "hwloc/darwin/cpukinds: failed to get logical-cpu-id\n");
        CFRelease(ref);
        continue;
      }
      hwloc_debug("got logical-cpu-id %lld\n", lld_value);
      logical_cpu_id = lld_value;
    }
    CFRelease(ref);

#ifdef HWLOC_DEBUG
    /* get logical-cluster-id */
    ref = IORegistryEntrySearchCFProperty(cpus_child, dt_plane_name, CFSTR("logical-cluster-id"), kCFAllocatorDefault, kNilOptions);
    if (!ref) {
      hwloc_debug("failed to find logical-cluster-id\n");
      continue;
    }
    if (CFGetTypeID(ref) != CFNumberGetTypeID()) {
      hwloc_debug("unexpected `logical-cluster-id' CF type is %s\n",
                  CFStringGetCStringPtr(CFCopyTypeIDDescription(CFGetTypeID(ref)), kCFStringEncodingUTF8));
      CFRelease(ref);
      continue;
    }
    {
      long long lld_value;
      if (!CFNumberGetValue(ref, kCFNumberLongLongType, &lld_value)) {
        hwloc_debug("failed to get logical-cluster-id\n");
        CFRelease(ref);
        continue;
      }
      hwloc_debug("got logical-cluster-id %lld\n", lld_value);
    }
    CFRelease(ref);
#endif

    /* get cluster-type */
    ref = IORegistryEntrySearchCFProperty(cpus_child, dt_plane_name, CFSTR("cluster-type"), kCFAllocatorDefault, kNilOptions);
    if (!ref) {
      if (HWLOC_SHOW_ALL_ERRORS())
        fprintf(stderr, "hwloc/darwin/cpukinds: failed to find cluster-type\n");
      continue;
    }
    if (CFGetTypeID(ref) != CFDataGetTypeID()) {
      if (HWLOC_SHOW_ALL_ERRORS())
        fprintf(stderr, "hwloc/darwin/cpukinds: unexpected `cluster-type' CF type %s\n",
                CFStringGetCStringPtr(CFCopyTypeIDDescription(CFGetTypeID(ref)), kCFStringEncodingUTF8));
      CFRelease(ref);
      continue;
    }
    if (CFDataGetLength(ref) < 2) {
      if (HWLOC_SHOW_ALL_ERRORS())
        fprintf(stderr, "hwloc/darwin/cpukinds: only got %ld bytes from cluster-type data\n",
                CFDataGetLength(ref));
      CFRelease(ref);
      continue;
    }
    {
      UInt8 u8_values[2];
      CFDataGetBytes(ref, CFRangeMake(0, 2), u8_values);
      if (u8_values[1] == 0) {
        hwloc_debug("got cluster-type %c\n", u8_values[0]);
        cluster_type = u8_values[0];
      } else {
        if (HWLOC_SHOW_ALL_ERRORS())
          fprintf(stderr, "hwloc/darwin/cpukinds: got more than one character in cluster-type data %c%c...\n",
                  u8_values[0], u8_values[1]);
        CFRelease(ref);
        continue;
      }
    }
    CFRelease(ref);

    /* get compatible */
    ref = IORegistryEntrySearchCFProperty(cpus_child, dt_plane_name, CFSTR("compatible"), kCFAllocatorDefault, kNilOptions);
    if (!ref) {
      if (HWLOC_SHOW_ALL_ERRORS())
        fprintf(stderr, "hwloc/darwin/cpukinds: failed to find compatible\n");
      continue;
    }
    if (CFGetTypeID(ref) != CFDataGetTypeID()) {
      if (HWLOC_SHOW_ALL_ERRORS())
        fprintf(stderr, "hwloc/darwin/cpukinds: unexpected `compatible' CF type %s\n",
                CFStringGetCStringPtr(CFCopyTypeIDDescription(CFGetTypeID(ref)), kCFStringEncodingUTF8));
      CFRelease(ref);
      continue;
    }
    {
      unsigned length;
      length = CFDataGetLength(ref);
      if (length > HWLOC_DARWIN_COMPATIBLE_MAX)
        length = HWLOC_DARWIN_COMPATIBLE_MAX;
      CFDataGetBytes(ref, CFRangeMake(0, length), (UInt8*) compatible);
      compatible[length] = 0;
      compatible[length+1] = 0;
      for(i=0; i<length; i++)
        if (!compatible[i] && compatible[i+1])
          compatible[i] = ';';
      if (!compatible[0]) {
        if (HWLOC_SHOW_ALL_ERRORS())
          fprintf(stderr, "hwloc/darwin/cpukinds: compatible is empty\n");
        CFRelease(ref);
        continue;
      }
      hwloc_debug("got compatible %s\n", compatible);
      CFRelease(ref);
    }

    IOObjectRelease(cpus_child);

    hwloc__darwin_cpukinds_add_cpu(kinds, cluster_type, compatible, logical_cpu_id);
  }
  IOObjectRelease(cpus_iter);
  IOObjectRelease(cpus_root);

  /* try to match sysctl hw.perflevel,
   * perflevel0 always refers to the highest performance core type in the system.
   * if we only have E and P, E=perflevel1, P=perflevel0, otherwise we don't know.
   */
  *matched_perflevels = 1;
  for(i=0; i<kinds->nr; i++) {
    /*
     * cluster types: https://developer.apple.com/news/?id=vk3m204o
     * E=Efficiency, P=Performance
     */
    if (kinds->kinds[i].cluster_type == 'E') {
      kinds->kinds[i].perflevel = 1;
    } else if (kinds->kinds[1].cluster_type == 'P') {
      kinds->kinds[i].perflevel = 0;
    } else {
      *matched_perflevels = 0;
      if (HWLOC_SHOW_ALL_ERRORS())
        fprintf(stderr, "hwloc/darwin/cpukinds: unrecognized cluster type %c compatible %s, cannot match perflevels\n",
                kinds->kinds[i].cluster_type, kinds->kinds[i].compatible);
    }
  }

  hwloc_debug("\n");
  return 0;
}
#endif /* !HWLOC_HAVE_DARWIN_FOUNDATION || !HWLOC_HAVE_DARWIN_IOKIT */

static int hwloc__darwin_cpukinds_register(struct hwloc_topology *topology,
                                           struct hwloc_darwin_cpukinds *kinds)
{
  unsigned i;
  int got_efficiency = kinds->nr ? 1 : 0;

  for(i=0; i<kinds->nr; i++) {
    struct hwloc_info_s infoattr;
    unsigned nr_info = 0;
    int efficiency;
    hwloc_debug_2args_bitmap("building cpukind with perflevel %u compatible `%s' and cpuset %s\n",
                             kinds->kinds[i].perflevel,
                             kinds->kinds[i].compatible,
                             kinds->kinds[i].cpuset);
    if (kinds->kinds[i].compatible) {
      infoattr.name = (char *) "DarwinCompatible";
      infoattr.value = kinds->kinds[i].compatible;
      nr_info = 1;
    }
    if (kinds->kinds[i].perflevel >= 0) {
      /* perflevel0 always refers to the highest performance core type in the system. */
      efficiency = kinds->nr - 1 - kinds->kinds[i].perflevel;
    } else {
      efficiency = HWLOC_CPUKIND_EFFICIENCY_UNKNOWN;
      got_efficiency = 0;
    }
    hwloc_internal_cpukinds_register(topology, kinds->kinds[i].cpuset, efficiency, &infoattr, nr_info, 0);
    free(kinds->kinds[i].compatible);
    /* the cpuset is given to the callee */
  }

  hwloc_debug("\n");

  topology->support.discovery->cpukind_efficiency = got_efficiency;
  return 0;
}

static void hwloc__darwin_build_numa_level(struct hwloc_topology *topology,
                                           unsigned nrobjs, unsigned width,
                                           uint64_t size,
                                           int *gotnuma, int *gotnumamemory)
{
  unsigned j;
  for (j = 0; j < nrobjs; j++) {
    hwloc_obj_t obj = hwloc_alloc_setup_object(topology, HWLOC_OBJ_NUMANODE, j);
    obj->cpuset = hwloc_bitmap_alloc();
    hwloc_bitmap_set_range(obj->cpuset, j*width, (j+1)*width-1);
    obj->nodeset = hwloc_bitmap_alloc();
    hwloc_bitmap_set(obj->nodeset, j);
    (*gotnuma)++;

    hwloc_debug_1arg_bitmap("node %u has cpuset %s\n",
                            j, obj->cpuset);
    if (size) {
      obj->attr->numanode.local_memory = size;
      (*gotnumamemory)++;
    }
    obj->attr->numanode.page_types_len = 2;
    obj->attr->numanode.page_types = malloc(2*sizeof(*obj->attr->numanode.page_types));
    memset(obj->attr->numanode.page_types, 0, 2*sizeof(*obj->attr->numanode.page_types));
    obj->attr->numanode.page_types[0].size = hwloc_getpagesize();
#if HAVE_DECL__SC_LARGE_PAGESIZE
    obj->attr->numanode.page_types[1].size = sysconf(_SC_LARGE_PAGESIZE);
#endif
    hwloc__insert_object_by_cpuset(topology, NULL, obj ,"darwin:numanode");
  }
}

static void hwloc__darwin_build_cache_level(struct hwloc_topology *topology,
                                            unsigned nrobjs, unsigned width,
                                            hwloc_obj_type_t type,
                                            unsigned depth, uint64_t size, int64_t linesize, int64_t ways)
{
  unsigned j;
  for (j = 0; j < nrobjs; j++) {
    hwloc_obj_t obj = hwloc_alloc_setup_object(topology, type, HWLOC_UNKNOWN_INDEX);
    obj->cpuset = hwloc_bitmap_alloc();
    hwloc_bitmap_set_range(obj->cpuset, j*width, (j+1)*width-1);
    hwloc_debug_2args_bitmap("L%ucache %u has cpuset %s\n",
                             depth, j, obj->cpuset);
    obj->attr->cache.depth = depth;
    obj->attr->cache.type = depth > 1 ? HWLOC_OBJ_CACHE_UNIFIED
      : hwloc_obj_type_is_icache(type) ? HWLOC_OBJ_CACHE_INSTRUCTION : HWLOC_OBJ_CACHE_DATA;
    obj->attr->cache.size = size;
    obj->attr->cache.linesize = linesize;
    obj->attr->cache.associativity = ways;
    hwloc__insert_object_by_cpuset(topology, NULL, obj, "darwin:cache");
  }
}

static void hwloc__darwin_build_perflevel_cache_level(struct hwloc_topology *topology,
                                                    hwloc_bitmap_t cpuset, unsigned width,
                                                    hwloc_obj_type_t type,
                                                    unsigned depth, uint64_t size, int64_t linesize)
{
  unsigned j,k;
  int next = -1;
  hwloc_debug_2args_bitmap("looking at perflevel cache depth %u width %u inside cpuset %s\n", depth, width, cpuset);
  for (j = 0;; j++) {
    hwloc_bitmap_t objcpuset;
    hwloc_obj_t obj;

    objcpuset = hwloc_bitmap_alloc();
    for(k=0; k<width; k++) {
      next = hwloc_bitmap_next(cpuset, next);
      if (next < 0)
        break;
      hwloc_bitmap_set(objcpuset, next);
    }
    if (!k) {
      /* failed to find any remaining cpu in cpuset, stop */
      hwloc_bitmap_free(objcpuset);
      return;
    }

    obj = hwloc_alloc_setup_object(topology, type, HWLOC_UNKNOWN_INDEX);
    obj->cpuset = objcpuset;

    hwloc_debug_2args_bitmap("L%ucache %u has cpuset %s\n",
                             depth, j, obj->cpuset);
    obj->attr->cache.depth = depth;
    obj->attr->cache.type = depth > 1 ? HWLOC_OBJ_CACHE_UNIFIED
      : hwloc_obj_type_is_icache(type) ? HWLOC_OBJ_CACHE_INSTRUCTION : HWLOC_OBJ_CACHE_DATA;
    obj->attr->cache.size = size;
    obj->attr->cache.linesize = linesize;
    obj->attr->cache.associativity = 0; /* unknown */
    hwloc__insert_object_by_cpuset(topology, NULL, obj, "darwin:perflevel:cache");
  }
}

struct gothybrid {
  unsigned l1i, l1d, l2, l3;
};

static void hwloc__darwin_look_perflevel_caches(struct hwloc_topology *topology,
                                                unsigned level,
                                                hwloc_bitmap_t cpuset,
                                                int64_t linesize,
                                                struct gothybrid *gothybrid)
{
  char name[64];
  int64_t size;

  snprintf(name, sizeof(name), "hw.perflevel%u.l1icachesize", level);
  if (hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_L1ICACHE)
      && !hwloc_get_sysctlbyname(name, &size)) {
    /* hw.perflevel%u.cpusperl1i missing, assume it's per PU */
    hwloc_debug("found perflevel %u l1icachesize %ld, assuming width 1\n", level, (long) size);
    hwloc__darwin_build_perflevel_cache_level(topology, cpuset, 1, HWLOC_OBJ_L1ICACHE, 1, size, linesize);
    gothybrid->l1i++;
  }

  snprintf(name, sizeof(name), "hw.perflevel%u.l1dcachesize", level);
  if (hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_L1CACHE)
      && !hwloc_get_sysctlbyname(name, &size)) {
    /* hw.perflevel%u.cpusperl1d missing, assume it's per PU */
    hwloc_debug("found perflevel %u l1dcachesize %ld, assuming width 1\n", level, (long) size);
    hwloc__darwin_build_perflevel_cache_level(topology, cpuset, 1, HWLOC_OBJ_L1CACHE, 1, size, linesize);
    gothybrid->l1d++;
  }

  snprintf(name, sizeof(name), "hw.perflevel%u.l2cachesize", level);
  if (hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_L2CACHE)
      && !hwloc_get_sysctlbyname(name, &size)) {
    int64_t cpus;

    hwloc_debug("found perflevel %u l2cachesize %ld\n", level, (long) size);

    snprintf(name, sizeof(name), "hw.perflevel%u.cpusperl2", level);
    if (hwloc_get_sysctlbyname(name, &cpus)) {
      hwloc_debug("couldn't find perflevel %u cpusperl2, assuming width 1\n", level);
      cpus = 1;
    } else {
      hwloc_debug("found perflevel %u cpusperl2 %ld\n", level, (long) cpus);
    }

    {
      /* hw.perflevelN.l2perflevels = "bitmap, where bit  number of CPUs of the same type that share L2",
       * not available yet, warn if we see one.
       */
      size_t s;
      snprintf(name, sizeof(name), "hw.perflevel%u.l2perflevels", level);
      if (!sysctlbyname(name, NULL, &s, NULL, 0))
        if (HWLOC_SHOW_ALL_ERRORS())
          fprintf(stderr, "hwloc/darwin: key %s succeeded size %lu, please report to hwloc developers.\n", name, (unsigned long) s);
    }

    /* assume PUs are contigous for now. */
    hwloc__darwin_build_perflevel_cache_level(topology, cpuset, cpus, HWLOC_OBJ_L2CACHE, 2, size, linesize);
    gothybrid->l2++;
  }

  snprintf(name, sizeof(name), "hw.perflevel%u.l3cachesize", level);
  if (hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_L3CACHE)
      && !hwloc_get_sysctlbyname(name, &size)) {
    int64_t cpus;

    hwloc_debug("found perflevel %u l3cachesize %ld\n", level, (long) size);

    snprintf(name, sizeof(name), "hw.perflevel%u.cpusperl3", level);
    if (hwloc_get_sysctlbyname(name, &cpus)) {
      hwloc_debug("couldn't find perflevel %u cpusperl3, assuming width 1\n", level);
      cpus = 1;
    } else {
      hwloc_debug("found perflevel %u cpusperl3 %ld\n", level, (long) cpus);
    }

    {
      /* hw.perflevelN.l3perflevels = "bitmap, where bit  number of CPUs of the same type that share L2",
       * not available yet, warn if we see one.
       */
      size_t s;
      snprintf(name, sizeof(name), "hw.perflevel%u.l3perflevels", level);
      if (!sysctlbyname(name, NULL, &s, NULL, 0))
        if (HWLOC_SHOW_ALL_ERRORS())
          fprintf(stderr, "hwloc/darwin: key %s succeeded size %lu, please report to hwloc developers.\n", name, (unsigned long) s);
    }

    hwloc__darwin_build_perflevel_cache_level(topology, cpuset, cpus, HWLOC_OBJ_L3CACHE, 3, size, linesize);
    gothybrid->l3++;
  }
}

static int
hwloc_look_darwin(struct hwloc_backend *backend, struct hwloc_disc_status *dstatus)
{
  /*
   * This backend uses the underlying OS.
   * However we don't enforce topology->is_thissystem so that
   * we may still force use this backend when debugging with !thissystem.
   */

  struct hwloc_topology *topology = backend->topology;
  int64_t _nprocs;
  unsigned nprocs;
  int64_t _npackages;
  unsigned i, cpu;
  struct hwloc_obj *obj;
  size_t size;
  int64_t l1dcachesize, l1icachesize;
  int64_t cacheways[2];
  int64_t l2cachesize;
  int64_t l3cachesize;
  int64_t cachelinesize;
  int64_t memsize;
  int64_t _tmp;
  char cpumodel[64];
  char cpuvendor[64];
  char cpufamilynumber[20], cpumodelnumber[20], cpustepping[20];
  int gotnuma = 0;
  int gotnumamemory = 0;
  struct gothybrid gothybrid;
  int64_t nperflevels = 0;
  struct hwloc_darwin_cpukinds kinds;
  int cpukinds_from_sysctl;
  char *env;

  memset(&gothybrid, 0, sizeof(gothybrid));

  assert(dstatus->phase == HWLOC_DISC_PHASE_CPU);

  if (topology->levels[0][0]->cpuset)
    /* somebody discovered things */
    return -1;

  if (hwloc_get_sysctlbyname("hw.nperflevels", &nperflevels))
    nperflevels = 0;
  else
    hwloc_debug("\n%d sysctl perflevels\n", (int) nperflevels);

  kinds.nr = 0;
  cpukinds_from_sysctl = -1; /* try IOKit, then sysctl */
  env = getenv("HWLOC_DARWIN_CPUKINDS_FROM_SYSCTL");
  if (env)
    cpukinds_from_sysctl = atoi(env);

  /* IOKit reports the precise list of cores,
   * while sysctl only says how many cores per perflevel, not which.
   * (TODO: hw.l[23]perflevels may help when available when those keys will be supported).
   * Not very important since we cannot bind on Mac OS X,
   * but that's why we use IOKit first.
   */
#if (defined HWLOC_HAVE_DARWIN_FOUNDATION) && (defined HWLOC_HAVE_DARWIN_IOKIT)
  if (cpukinds_from_sysctl != 1) {
    int matched_perflevels = 0;
    hwloc__darwin_look_iokit_cpukinds(&kinds, &matched_perflevels);
    hwloc_debug("Found %u kinds with matched=%u in IOKit for %ld sysctl perflevels\n",
                kinds.nr, matched_perflevels, (long) nperflevels);
    if (nperflevels > 0 && cpukinds_from_sysctl == -1 && (!matched_perflevels || nperflevels != kinds.nr)) {
      /* sysctl has perflevel info, but we couldn't rank iokit kinds accordingly,
       * it means we won't be able to find out which perflevel caches correspond to which cpukind.
       * remove iokit cpukinds and fallback to sysctl cpukinds.
       */
      hwloc_debug("Clearing IOKit cpukinds to read them from sysctl\n");
      hwloc__darwin_cpukinds_clear(&kinds);
    }
  }
#endif

  if (nperflevels > 0 && (cpukinds_from_sysctl == 1 || (cpukinds_from_sysctl ==  -1 && !kinds.nr))) {
    /* if there are sysctl perflevels, and either we found nothing in IOKit and sysctl is allowed, or we're forced to use sysctl */
    unsigned totalcpus = 0;
    hwloc_debug("\nReading cpukinds from %d sysctl perflevels...\n", (int) nperflevels);
    for(i=0; i<nperflevels; i++) {
      char name[64];
      int64_t ncpus;
      snprintf(name, sizeof(name), "hw.perflevel%u.logicalcpu", i);
      if (!hwloc_get_sysctlbyname(name, &ncpus)) {
        struct hwloc_darwin_cpukind *kind;
        hwloc_debug("found %d cpus in perflevel %u\n", (int)ncpus, i);
        kind = hwloc__darwin_cpukinds_add(&kinds, '?', NULL);
        if (kind) {
          hwloc_bitmap_set_range(kind->cpuset, totalcpus, totalcpus+ncpus-1);
          kind->perflevel = i;
          totalcpus += ncpus;
        }
      }
    }
  }
  hwloc_debug("%s", "\n");

  hwloc_alloc_root_sets(topology->levels[0][0]);

  /* Don't use hwloc_fallback_nbprocessors() because it would return online cpus only,
   * while we need all cpus when computing logical_per_package, etc below.
   * We don't know which CPUs are offline, but Darwin doesn't support binding anyway.
   *
   * TODO: try hw.logicalcpu_max
   */

  if (hwloc_get_sysctlbyname("hw.logicalcpu", &_nprocs) || _nprocs <= 0)
    /* fallback to deprecated way */
    if (hwloc_get_sysctlbyname("hw.ncpu", &_nprocs) || _nprocs <= 0)
      return -1;

  nprocs = _nprocs;
  topology->support.discovery->pu = 1;

  hwloc_debug("%u procs\n", nprocs);

  size = sizeof(cpuvendor);
  if (sysctlbyname("machdep.cpu.vendor", cpuvendor, &size, NULL, 0))
    cpuvendor[0] = '\0';

  size = sizeof(cpumodel);
  if (sysctlbyname("machdep.cpu.brand_string", cpumodel, &size, NULL, 0))
    cpumodel[0] = '\0';

  if (hwloc_get_sysctlbyname("machdep.cpu.family", &_tmp))
    cpufamilynumber[0] = '\0';
  else
    snprintf(cpufamilynumber, sizeof(cpufamilynumber), "%lld", (long long) _tmp);
  if (hwloc_get_sysctlbyname("machdep.cpu.model", &_tmp))
    cpumodelnumber[0] = '\0';
  else
    snprintf(cpumodelnumber, sizeof(cpumodelnumber), "%lld", (long long) _tmp);
  /* .extfamily and .extmodel are already added to .family and .model */
  if (hwloc_get_sysctlbyname("machdep.cpu.stepping", &_tmp))
    cpustepping[0] = '\0';
  else
    snprintf(cpustepping, sizeof(cpustepping), "%lld", (long long) _tmp);

  if (!hwloc_get_sysctlbyname("hw.packages", &_npackages) && _npackages > 0) {
    unsigned npackages = _npackages;
    int64_t _cores_per_package;
    unsigned cores_per_package;
    int64_t _logical_per_package;
    unsigned logical_per_package;

    hwloc_debug("%u packages\n", npackages);

    if (!hwloc_get_sysctlbyname("machdep.cpu.thread_count", &_logical_per_package) && _logical_per_package > 0)
      /* official/modern way */
      logical_per_package = _logical_per_package;
    else if (!hwloc_get_sysctlbyname("machdep.cpu.logical_per_package", &_logical_per_package) && _logical_per_package > 0)
      /* old way, gives the max supported by this "kind" of processor,
       * can be larger than the actual number for this model.
       */
      logical_per_package = _logical_per_package;
    else
      /* Assume the trivia.  */
      logical_per_package = nprocs / npackages;

    hwloc_debug("%u threads per package\n", logical_per_package);

    if (nprocs == npackages * logical_per_package
	&& hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_PACKAGE))
      for (i = 0; i < npackages; i++) {
        obj = hwloc_alloc_setup_object(topology, HWLOC_OBJ_PACKAGE, i);
        obj->cpuset = hwloc_bitmap_alloc();
        for (cpu = i*logical_per_package; cpu < (i+1)*logical_per_package; cpu++)
          hwloc_bitmap_set(obj->cpuset, cpu);

        hwloc_debug_1arg_bitmap("package %u has cpuset %s\n",
                   i, obj->cpuset);

        if (cpuvendor[0] != '\0')
          hwloc_obj_add_info(obj, "CPUVendor", cpuvendor);
        if (cpumodel[0] != '\0')
          hwloc_obj_add_info(obj, "CPUModel", cpumodel);
        if (cpufamilynumber[0] != '\0')
          hwloc_obj_add_info(obj, "CPUFamilyNumber", cpufamilynumber);
        if (cpumodelnumber[0] != '\0')
          hwloc_obj_add_info(obj, "CPUModelNumber", cpumodelnumber);
        if (cpustepping[0] != '\0')
          hwloc_obj_add_info(obj, "CPUStepping", cpustepping);

        hwloc__insert_object_by_cpuset(topology, NULL, obj, "darwin:package");
      }
    else {
      if (cpuvendor[0] != '\0')
        hwloc_obj_add_info(topology->levels[0][0], "CPUVendor", cpuvendor);
      if (cpumodel[0] != '\0')
        hwloc_obj_add_info(topology->levels[0][0], "CPUModel", cpumodel);
      if (cpufamilynumber[0] != '\0')
        hwloc_obj_add_info(topology->levels[0][0], "CPUFamilyNumber", cpufamilynumber);
      if (cpumodelnumber[0] != '\0')
        hwloc_obj_add_info(topology->levels[0][0], "CPUModelNumber", cpumodelnumber);
      if (cpustepping[0] != '\0')
        hwloc_obj_add_info(topology->levels[0][0], "CPUStepping", cpustepping);
    }

    if (!hwloc_get_sysctlbyname("machdep.cpu.core_count", &_cores_per_package) && _cores_per_package > 0)
      /* official/modern way */
      cores_per_package = _cores_per_package;
    else if (!hwloc_get_sysctlbyname("machdep.cpu.cores_per_package", &_cores_per_package) && _cores_per_package > 0)
      /* old way, gives the max supported by this "kind" of processor,
       * can be larger than the actual number for this model.
       */
      cores_per_package = _cores_per_package;
    else
      /* no idea */
      cores_per_package = 0;

    if (cores_per_package > 0
	&& hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_CORE)) {
      hwloc_debug("%u cores per package\n", cores_per_package);

      if (!(logical_per_package % cores_per_package))
        for (i = 0; i < npackages * cores_per_package; i++) {
          obj = hwloc_alloc_setup_object(topology, HWLOC_OBJ_CORE, i);
          obj->cpuset = hwloc_bitmap_alloc();
          for (cpu = i*(logical_per_package/cores_per_package);
               cpu < (i+1)*(logical_per_package/cores_per_package);
               cpu++)
            hwloc_bitmap_set(obj->cpuset, cpu);

          hwloc_debug_1arg_bitmap("core %u has cpuset %s\n",
                     i, obj->cpuset);
          hwloc__insert_object_by_cpuset(topology, NULL, obj, "darwin:core");
        }
    }
  } else {
    if (cpuvendor[0] != '\0')
      hwloc_obj_add_info(topology->levels[0][0], "CPUVendor", cpuvendor);
    if (cpumodel[0] != '\0')
      hwloc_obj_add_info(topology->levels[0][0], "CPUModel", cpumodel);
    if (cpufamilynumber[0] != '\0')
      hwloc_obj_add_info(topology->levels[0][0], "CPUFamilyNumber", cpufamilynumber);
    if (cpumodelnumber[0] != '\0')
      hwloc_obj_add_info(topology->levels[0][0], "CPUModelNumber", cpumodelnumber);
    if (cpustepping[0] != '\0')
      hwloc_obj_add_info(topology->levels[0][0], "CPUStepping", cpustepping);
  }

  if (hwloc_get_sysctlbyname("hw.cachelinesize", &cachelinesize))
    cachelinesize = 0;

  hwloc_debug("%s", "\nReading caches from sysctl perflevels\n");
  for(i=0; i<kinds.nr; i++)
    if (kinds.kinds[i].perflevel >= 0)
      hwloc__darwin_look_perflevel_caches(topology, kinds.kinds[i].perflevel, kinds.kinds[i].cpuset, cachelinesize, &gothybrid);

  if (hwloc_get_sysctlbyname("hw.l1dcachesize", &l1dcachesize))
    l1dcachesize = 0;

  if (hwloc_get_sysctlbyname("hw.l1icachesize", &l1icachesize))
    l1icachesize = 0;

  if (hwloc_get_sysctlbyname("hw.l2cachesize", &l2cachesize))
    l2cachesize = 0;

  if (hwloc_get_sysctlbyname("hw.l3cachesize", &l3cachesize))
    l3cachesize = 0;

  if (hwloc_get_sysctlbyname("machdep.cpu.cache.L1_associativity", &cacheways[0]))
    cacheways[0] = 0;
  else if (cacheways[0] == 0xff)
    cacheways[0] = -1;

  if (hwloc_get_sysctlbyname("machdep.cpu.cache.L2_associativity", &cacheways[1]))
    cacheways[1] = 0;
  else if (cacheways[1] == 0xff)
    cacheways[1] = -1;

  if (hwloc_get_sysctlbyname("hw.memsize", &memsize))
    memsize = 0;

  hwloc_debug("%s", "\n");
  if (!sysctlbyname("hw.cacheconfig", NULL, &size, NULL, 0)) {
    unsigned n = size / sizeof(uint32_t);
    uint64_t *cacheconfig;
    uint64_t *cachesize;
    uint32_t *cacheconfig32;

    cacheconfig = malloc(n * sizeof(*cacheconfig));
    cachesize = malloc(n * sizeof(*cachesize));
    cacheconfig32 = malloc(n * sizeof(*cacheconfig32));

    if (cacheconfig && cachesize && cacheconfig32
	&& (!sysctlbyname("hw.cacheconfig", cacheconfig, &size, NULL, 0))) {
      /* Yeech. Darwin seemingly has changed from 32bit to 64bit integers for
       * cacheconfig, with apparently no way for detection. Assume the machine
       * won't have more than 4 billion cpus */
      if (cacheconfig[0] > 0xFFFFFFFFUL) {
        memcpy(cacheconfig32, cacheconfig, size);
        for (i = 0 ; i < size / sizeof(uint32_t); i++)
          cacheconfig[i] = cacheconfig32[i];
      }

      memset(cachesize, 0, sizeof(uint64_t) * n);
      size = sizeof(uint64_t) * n;
      if (sysctlbyname("hw.cachesize", cachesize, &size, NULL, 0)) {
        if (n > 0)
          cachesize[0] = memsize;
        if (n > 1)
          cachesize[1] = l1dcachesize;
        if (n > 2)
          cachesize[2] = l2cachesize;
        if (n > 3)
          cachesize[3] = l3cachesize;
      }
      else
        /* Even on 64b systems, cachesize[0] is bogus when memsize is more than 4G. */
        if (n > 0 && cachesize[0] < (uint64_t) memsize)
          cachesize[0] = memsize;

      hwloc_debug("%s", "non-hybrid caches");
      for (i = 0; i < n && cacheconfig[i]; i++)
        hwloc_debug(" %"PRIu64"(%"PRIu64"kB)", cacheconfig[i], cachesize[i] / 1024);

      /* Now we know how many caches there are */
      n = i;
      hwloc_debug("\n%u non-hybrid cache levels\n", n - 1);

      /* slot 0 is memory */
      hwloc__darwin_build_numa_level(topology, nprocs / cacheconfig[0], cacheconfig[0], cachesize[0],
                                     &gotnuma, &gotnumamemory);
      /* other slots are caches L1d, L2u, L3u, etc.
       * L1i doesn't have an explicit slot but we can assume things are shared similarly to L1d in first slot.
       */
      if (!gothybrid.l1i && n>=2 && l1icachesize
          && hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_L1ICACHE)) {
        hwloc__darwin_build_cache_level(topology, nprocs / cacheconfig[1], cacheconfig[1],
                                        HWLOC_OBJ_L1ICACHE,
                                        1, l1icachesize, cachelinesize, 0 /* unknown */);
      }
      /* Use slots for L1d, L2u, L3u now. */
      for (i = 1; i < n; i++) {
        if (i == 1 && gothybrid.l1d)
          continue;
        if (i == 2 && gothybrid.l2)
          continue;
        if (i == 3 && gothybrid.l3)
          continue;
        if (hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_L1CACHE+i-1)) {
          hwloc__darwin_build_cache_level(topology, nprocs / cacheconfig[i], cacheconfig[i],
                                          HWLOC_OBJ_L1CACHE+i-1,
                                          i, cachesize[i], cachelinesize,
                                          i <= sizeof(cacheways) / sizeof(cacheways[0]) ? cacheways[i-1] : 0);
        }
      }
    }
    free(cacheconfig);
    free(cachesize);
    free(cacheconfig32);
  }

  if (gotnuma)
    topology->support.discovery->numa = 1;
  if (gotnumamemory)
    topology->support.discovery->numa_memory = 1;

  /* add PU objects */
  hwloc_setup_pu_level(topology, nprocs);

  if (topology->flags & HWLOC_TOPOLOGY_FLAG_NO_CPUKINDS)
    hwloc__darwin_cpukinds_clear(&kinds);
  else
    hwloc__darwin_cpukinds_register(topology, &kinds);

  hwloc_obj_add_info(topology->levels[0][0], "Backend", "Darwin");
  hwloc_add_uname_info(topology, NULL);
  return 0;
}

void
hwloc_set_darwin_hooks(struct hwloc_binding_hooks *hooks __hwloc_attribute_unused,
		       struct hwloc_topology_support *support __hwloc_attribute_unused)
{
}

static struct hwloc_backend *
hwloc_darwin_component_instantiate(struct hwloc_topology *topology,
				   struct hwloc_disc_component *component,
				   unsigned excluded_phases __hwloc_attribute_unused,
				   const void *_data1 __hwloc_attribute_unused,
				   const void *_data2 __hwloc_attribute_unused,
				   const void *_data3 __hwloc_attribute_unused)
{
  struct hwloc_backend *backend;
  backend = hwloc_backend_alloc(topology, component);
  if (!backend)
    return NULL;
  backend->discover = hwloc_look_darwin;
  return backend;
}

static struct hwloc_disc_component hwloc_darwin_disc_component = {
  "darwin",
  HWLOC_DISC_PHASE_CPU,
  HWLOC_DISC_PHASE_GLOBAL,
  hwloc_darwin_component_instantiate,
  50,
  1,
  NULL
};

const struct hwloc_component hwloc_darwin_component = {
  HWLOC_COMPONENT_ABI,
  NULL, NULL,
  HWLOC_COMPONENT_TYPE_DISC,
  0,
  &hwloc_darwin_disc_component
};
