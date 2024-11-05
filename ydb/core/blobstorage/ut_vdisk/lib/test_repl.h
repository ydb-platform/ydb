#pragma once

#include "defs.h"
#include "prepare.h"

class IDataSet;

///////////////////////////////////////////////////////////////////////////
struct TReadUntilSuccess {
    const IDataSet *DataSet;
    ui32 VDiskNumber;
    TDuration RepeatTimeout;
    bool Multipart;

    TReadUntilSuccess(const IDataSet *dataSet, ui32 vdiskNumber, const TDuration &repeatTimeout, bool multipart = false)
        : DataSet(dataSet)
        , VDiskNumber(vdiskNumber)
        , RepeatTimeout(repeatTimeout)
        , Multipart(multipart)
    {}

    void operator ()(TConfiguration *conf);
};

///////////////////////////////////////////////////////////////////////////
struct TTestReplDataWriteAndSync {
    const IDataSet *DataSet;

    TTestReplDataWriteAndSync(const IDataSet *dataSet)
        : DataSet(dataSet)
    {}

    void operator ()(TConfiguration *conf);
};

///////////////////////////////////////////////////////////////////////////
struct TTestReplDataWriteAndSyncMultipart {
    const IDataSet *DataSet;

    TTestReplDataWriteAndSyncMultipart(const IDataSet *dataSet)
        : DataSet(dataSet)
    {}

    void operator ()(TConfiguration *conf);
};


#define SIMPLE_CLASS_DEF_NO_PARAMS(name)    \
struct name {                               \
void operator ()(TConfiguration *conf); \
};

SIMPLE_CLASS_DEF_NO_PARAMS(TTestReplProxyData)
SIMPLE_CLASS_DEF_NO_PARAMS(TTestReplProxyKeepBits)
SIMPLE_CLASS_DEF_NO_PARAMS(TTestCollectAllSimpleDataset)
SIMPLE_CLASS_DEF_NO_PARAMS(TTestStub)

#undef SIMPLE_CLASS_DEF_NO_PARAMS
