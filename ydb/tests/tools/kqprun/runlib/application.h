#pragma once

#include "settings.h"

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/getopt/modchooser.h>

#include <util/stream/file.h>
#include <util/system/env.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/services/services.pb.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimrRun {

class TMainBase : public TMainClassArgs {
public:
    TMainBase();

#ifdef PROFILE_MEMORY_ALLOCATIONS
    static void FinishProfileMemoryAllocations();
#endif

protected:
    void RegisterKikimrOptions(NLastGetopt::TOpts& options, TServerSettings& settings);

    virtual void RegisterLogOptions(NLastGetopt::TOpts& options);

    static IOutputStream* GetDefaultOutput(const TString& file);

    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> CreateFunctionRegistry() const;

    void ReplaceYqlTokenTemplate(TString& text) const;

    void SetupActorSystemConfig(NKikimrConfig::TAppConfig& config) const;

    void SetupLogsConfig(NKikimrConfig::TLogConfig& config) const;

protected:
    inline static NColorizer::TColors CoutColors = NColorizer::AutoColors(Cout);
    inline static IOutputStream* ProfileAllocationsOutput = nullptr;
    inline static const TString YqlToken = GetEnv(YQL_TOKEN_VARIABLE);

    std::optional<NActors::NLog::EPriority> DefaultLogPriority;
    std::unordered_map<NKikimrServices::EServiceKikimr, NActors::NLog::EPriority> LogPriorities;
    std::optional<NActors::NLog::EPriority> FqLogPriority;
    std::optional<NActors::NLog::EPriority> KqpLogPriority;
    std::optional<NActors::NLog::EPriority> RuntimeLogPriority;
    std::optional<NActors::NLog::EPriority> TabletsLogPriority;
    std::optional<NActors::NLog::EPriority> BsLogPriority;
    std::optional<NActors::NLog::EPriority> ServerIoLogPriority;

private:
    inline static std::vector<std::unique_ptr<TFileOutput>> FileHolders;

    TString UdfsDirectory;
    TVector<TString> UdfsPaths;
    bool ExcludeLinkedUdfs;
    std::optional<ui64> UserPoolSize;
};

}  // namespace NKikimrRun
