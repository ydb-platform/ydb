#pragma once

#include "tasks_runner_proxy.h"
#include "file_cache.h"

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYql::NTaskRunnerProxy {

struct TPipeFactoryOptions {
    TString ExecPath;
    IFileCache::TPtr FileCache;
    THashMap<TString, TString> Env;
    bool EnablePorto = false;
    TString PortoLayer;
    int MaxProcesses = 1;
    TString ContainerName;
    TString PortoCtlPath = "/usr/bin/porto";
    TMaybe<TString> Revision; // revision override for tests
};

IProxyFactory::TPtr CreatePipeFactory(const TPipeFactoryOptions& options);

} // namespace NYql::NTaskRunnerProxy
