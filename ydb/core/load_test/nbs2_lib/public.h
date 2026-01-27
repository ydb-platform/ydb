#pragma once

#include <memory>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

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
