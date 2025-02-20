#pragma once

#include "settings.h"

#include <library/cpp/getopt/modchooser.h>

#include <util/stream/file.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/services/services.pb.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimrRun {

class TMainBase : public TMainClassArgs {
#ifdef PROFILE_MEMORY_ALLOCATIONS
public:
    static void FinishProfileMemoryAllocations();
#endif

protected:
    void RegisterKikimrOptions(NLastGetopt::TOpts& options, TServerSettings& settings);

    void FillLogConfig(NKikimrConfig::TLogConfig& config) const;

    static IOutputStream* GetDefaultOutput(const TString& file);

    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> CreateFunctionRegistry() const;

protected:
    inline static IOutputStream* ProfileAllocationsOutput = nullptr;

private:
    inline static std::vector<std::unique_ptr<TFileOutput>> FileHolders;

    std::optional<NActors::NLog::EPriority> DefaultLogPriority;
    std::unordered_map<NKikimrServices::EServiceKikimr, NActors::NLog::EPriority> LogPriorities;

    TString UdfsDirectory;
    TVector<TString> UdfsPaths;
    bool ExcludeLinkedUdfs;
};

}  // namespace NKikimrRun
