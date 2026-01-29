#pragma once

#include "public.h"
#include <ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib/protos/nbs2_load.pb.h>

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    EBlockStoreRequest RequestType;
    TBlockRange64 BlockRange;
};

////////////////////////////////////////////////////////////////////////////////

struct IRequestGenerator
{
    virtual ~IRequestGenerator() = default;

    virtual bool Next(TRequest* request) = 0;
    virtual void Complete(TBlockRange64 blockRange) = 0;
    virtual TString Describe() const = 0;
    virtual bool HasMoreRequests() const = 0;

    virtual TInstant Peek()
    {
        return TInstant::Max();
    }
};

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateArtificialRequestGenerator(
    ILoggingServicePtr loggingService,
    NProto::TRangeTest range);

IRequestGeneratorPtr CreateRealRequestGenerator(
    ILoggingServicePtr loggingService,
    TString profileLogPath,
    TString diskId,
    TString startTime,
    TString endTime,
    bool fullSpeed,
    ui64 maxRequestsInMemory,
    std::atomic<bool>& shouldStop);

}   // namespace NCloud::NBlockStore::NLoadTest
