#pragma once

#include <library/cpp/lwtrace/all.h>

#define DRR_PROBE(name, ...) GLOBAL_LWPROBE(SCHEDULING_DRR_PROVIDER, name, ## __VA_ARGS__)

#define SCHEDULING_DRR_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(ActivateQueue, GROUPS("Scheduling", "Drr"), \
      TYPES(TString,TString), \
      NAMES("parent","queue")) \
    PROBE(DropCache, GROUPS("Scheduling", "Drr"), \
      TYPES(TString,TString), \
      NAMES("parent","queue")) \
    PROBE(CacheDropped, GROUPS("Scheduling", "Drr"), \
      TYPES(TString,TString), \
      NAMES("parent","queue")) \
    PROBE(Info, GROUPS("Scheduling", "DrrDetails"), \
      TYPES(TString,TString), \
      NAMES("drr","info")) \
    /**/

LWTRACE_DECLARE_PROVIDER(SCHEDULING_DRR_PROVIDER)
