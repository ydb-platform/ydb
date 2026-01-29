#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <library/cpp/threading/future/core/future.h>

#include <memory>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

using LoadTestSendReadRequestFunctionCB = std::function<void(NYdb::NBS::NProto::TError, const void *udata)>;
using LoadTestSendReadRequestFunction = std::function<void(TBlockRange64, LoadTestSendReadRequestFunctionCB, const void *udata)>;

struct LoadTestSendRequestCallbacks {
    LoadTestSendReadRequestFunction Read;
};

class TBootstrap;

struct TModuleFactories;

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

struct IRequestGenerator;
using IRequestGeneratorPtr = std::shared_ptr<IRequestGenerator>;

struct ITestRunner;
using ITestRunnerPtr = std::shared_ptr<ITestRunner>;

struct TTestResults;
using TTestResultsPtr = std::unique_ptr<TTestResults>;

struct IClientFactory;

}   // namespace NCloud::NBlockStore::NLoadTest
