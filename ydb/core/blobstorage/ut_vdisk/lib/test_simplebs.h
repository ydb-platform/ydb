#pragma once

#include "defs.h"
#include "prepare.h"
#include "helpers.h"

#define SIMPLE_CLASS_DEF_NO_PARAMS(name)    \
struct name {                               \
    name()                                  \
    {}                                      \
    void operator ()(TConfiguration *conf); \
};


#define SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(name)         \
struct name {                                           \
    IDataSetPtr DataSetPtr;                             \
    const bool WaitForCompaction;                       \
    const ui32 VDiskNum;                                \
    name(IDataSetPtr ds, bool waitForCompaction,        \
         ui32 vdiskNum)                                 \
        : DataSetPtr(ds)                                \
        , WaitForCompaction(waitForCompaction)          \
        , VDiskNum(vdiskNum)                            \
    {}                                                  \
    void operator ()(TConfiguration *conf);             \
};


SIMPLE_CLASS_DEF_NO_PARAMS(TSimpleGetFromEmptyDB)
SIMPLE_CLASS_DEF_NO_PARAMS(TRangeGetFromEmptyDB)

// put/extreme_get from one vdisk
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3Put3Get)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3Put1SeqGetAll)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3Put1SeqGet2)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3Put1SeqSubsOk)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3Put1SeqSubsError)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3Put1GetMissingKey)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3Put1GetMissingPart)

// put/extreme_get from one HANDOFF vdisk
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimpleHnd6Put1SeqGet)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimpleHnd2Put1Get)

// put/range_get from one vdisk
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3PutRangeGetAllForward)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3PutRangeGetAllBackward)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3PutRangeGetNothingForward)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3PutRangeGetNothingBackward)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3PutRangeGetMiddleForward)
SIMPLE_CLASS_DEF_COMP_DISK_PARAMS(TSimple3PutRangeGetMiddleBackward)

#undef SIMPLE_CLASS_DEF_NO_PARAMS
#undef SIMPLE_CLASS_DEF_COMP_DISK_PARAMS
