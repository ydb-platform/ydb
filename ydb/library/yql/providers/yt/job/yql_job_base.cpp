#include "yql_job_base.h"
#include "yql_job_stats_writer.h"
#include "yql_job_factory.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/context.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/debug_info.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/yexception.h>
#include <util/folder/path.h>
#include <util/system/rusage.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/error.h>
#include <util/system/datetime.h>
#include <util/datetime/cputimer.h>
#include <util/ysaveload.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

TStatKey Job_ThreadsCount("Job_ThreadsCount", false);
TStatKey Job_ElapsedTime("Job_ElapsedTime", false);
TStatKey Job_UserTime("Job_UserTime", false);
TStatKey Job_SystemTime("Job_SystemTime", false);
TStatKey Job_MajorPageFaults("Job_MajorPageFaults", false);

class TEnvSecureParamsProvider : public NUdf::ISecureParamsProvider {
public:
    TEnvSecureParamsProvider(const TString& envName)
    {
        const TString& yson = GetEnv(envName + "_secure_params");
        // Absent variable is not an error
        if (!yson)
            return;
        auto attrs = NYT::NodeFromYsonString(yson);
        YQL_ENSURE(attrs.IsMap());

        SecureMap = attrs.AsMap();
    }

    bool GetSecureParam(NUdf::TStringRef key, NUdf::TStringRef& value) const override {
        auto p = SecureMap.FindPtr(TString(key.Data(), key.Size()));
        if (!p)
            return false;
        if (!p->IsString())
            return false;

        value = p->AsString();
        return true;
    }

private:
    NYT::TNode::TMapType SecureMap;
};

NKikimr::NUdf::TCounter TJobCountersProvider::GetCounter(const NKikimr::NUdf::TStringRef& module,
    const NKikimr::NUdf::TStringRef& name, bool deriv) {
    Y_UNUSED(deriv);
    auto fullName = std::make_pair(TString(TStringBuf(module)), TString(TStringBuf(name)));
    return NKikimr::NUdf::TCounter(&Counters_[fullName]);
}

NKikimr::NUdf::TScopedProbe TJobCountersProvider::GetScopedProbe(const NKikimr::NUdf::TStringRef& module,
    const NKikimr::NUdf::TStringRef& name) {
    auto fullName = std::make_pair(TString(TStringBuf(module)), TString(TStringBuf(name)));
    return NKikimr::NUdf::TScopedProbe(this, &Probes_[fullName]);
}

void TJobCountersProvider::Acquire(void* cookie) {
    auto state = (TProbeState*)cookie;
    state->LastAcquire = GetCycleCount();
}

void TJobCountersProvider::Release(void* cookie) {
    auto state = (TProbeState*)cookie;
    state->TotalCycles += GetCycleCount() - state->LastAcquire;
}

TString MakeLocalPath(TString fileName) {
    TString localPath = fileName;
    if (localPath.StartsWith(TStringBuf("/home/"))) {
        localPath = localPath.substr(TStringBuf("/home").length()); // Keep leading slash
    }
    if (!localPath.StartsWith('/')) {
        localPath.prepend('/');
    }
    localPath.prepend('.');
    return localPath;
}

class TJobTransformProvider {
public:
    TJobTransformProvider(THashMap<TString, TRuntimeNode>* extraArgs)
        : ExtraArgs(extraArgs)
    {
    }

    TCallableVisitFunc operator()(TInternName name) {
        if (name == "FilePathJob") {
            return [](TCallable& callable, const TTypeEnvironment& env) {
                YQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 argument");
                const TString path(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                return TRuntimeNode(BuildDataLiteral(MakeLocalPath(path), NUdf::TDataType<char*>::Id, env), true);
            };
        }

        if (name == "FileContentJob") {
            return [](TCallable& callable, const TTypeEnvironment& env) {
                YQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 argument");
                const TString path(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                auto content = TFileInput(MakeLocalPath(path)).ReadAll();
                return TRuntimeNode(BuildDataLiteral(content, NUdf::TDataType<char*>::Id, env), true);
            };
        }

        auto cutName = name.Str();
        if (cutName.SkipPrefix("Yt")) {
            if (cutName == "TableIndex" || cutName == "TablePath" || cutName == "TableRecord" || cutName == "IsKeySwitch" || cutName == "RowNumber") {
                return [this](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                    return GetExtraArg(TString{callable.GetType()->GetName()},
                        *AS_TYPE(TDataType, callable.GetType()->GetReturnType())->GetDataSlot(), env);
                };
            }
            if (cutName == "Input") {
                // Rename and add additional args
                return [this](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                    TCallableBuilder callableBuilder(env,
                        TStringBuilder() << callable.GetType()->GetName() << "Job", callable.GetType()->GetReturnType(), false);

                    if (const auto args = callable.GetInputsCount()) {
                        callableBuilder.Add(GetExtraArg("YtTableIndex", NUdf::EDataSlot::Uint32, env));
                        callableBuilder.Add(GetExtraArg("YtTablePath", NUdf::EDataSlot::String, env));
                        callableBuilder.Add(GetExtraArg("YtTableRecord", NUdf::EDataSlot::Uint64, env));
                        callableBuilder.Add(GetExtraArg("YtIsKeySwitch", NUdf::EDataSlot::Bool, env));
                        callableBuilder.Add(GetExtraArg("YtRowNumber", NUdf::EDataSlot::Uint64, env));
                        for (ui32 i: xrange(args)) {
                            callableBuilder.Add(callable.GetInput(i));
                        }
                    }

                    return TRuntimeNode(callableBuilder.Build(), false);
                };
            }
            if (cutName == "Output") {
                // Rename
                return [](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                    TCallableBuilder callableBuilder(env,
                        TStringBuilder() << callable.GetType()->GetName() << "Job", callable.GetType()->GetReturnType(), false);
                    for (ui32 i: xrange(callable.GetInputsCount())) {
                        callableBuilder.Add(callable.GetInput(i));
                    }
                    return TRuntimeNode(callableBuilder.Build(), false);
                };
            }
        }

        return TCallableVisitFunc();
    }

private:
    TRuntimeNode GetExtraArg(const TString& name, NUdf::EDataSlot slot, const TTypeEnvironment& env) {
        YQL_ENSURE(ExtraArgs, "Unexpected " << name << " usage");
        TRuntimeNode& node = (*ExtraArgs)[name];
        if (!node) {
            TCallableBuilder builder(env, "Arg", TDataType::Create(NUdf::GetDataTypeInfo(slot).TypeId, env), true);
            node = TRuntimeNode(builder.Build(), false);
        }
        return node;
    }

private:
    THashMap<TString, TRuntimeNode>* ExtraArgs;
};

TYqlJobBase::~TYqlJobBase() {
    try {
        if (JobStats) {
            JobStats->SetStat(Job_ElapsedTime, (GetCycleCount() - StartCycles) / GetCyclesPerMillisecond());
            TRusage ru;
            ru.Fill();
            JobStats->SetStat(Job_UserTime, ru.Utime.MilliSeconds());
            JobStats->SetStat(Job_SystemTime, ru.Stime.MilliSeconds());
            JobStats->SetStat(Job_MajorPageFaults, ru.MajorPageFaults);

            WriteJobStats(JobStats.Get(), JobCountersProvider);
        }
    } catch (...) {
        /* do not throw exceptions in destructor */
    }
}

void TYqlJobBase::Init() {
    StartCycles = GetCycleCount();
    StartTime = ThreadCPUTime();

    auto funcRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
    funcRegistry->SetBackTraceCallback(&NYql::NBacktrace::KikimrBackTrace);
    if (GetEnv(TString("YQL_DETERMINISTIC_MODE"))) {
        RandomProvider = CreateDeterministicRandomProvider(1);
        TimeProvider = CreateDeterministicTimeProvider(10000000);
    }
    else {
        RandomProvider = CreateDefaultRandomProvider();
        TimeProvider = CreateDefaultTimeProvider();
    }

    const bool hasTmpfs = TFsPath("_yql_tmpfs").IsDirectory();
    Y_UNUSED(hasTmpfs); // _win_ compiler

    for (const auto& a: FileAliases) {
        TFsPath lnk = TFsPath(".") / a.first;
        TFsPath parent = lnk.Parent();
        if (!parent.Exists()) {
            parent.MkDirs();
        } else {
            // Local mode support. Overwrite existing link if any
            if (lnk.IsSymlink()) {
                lnk.DeleteIfExists();
            }
        }
#ifndef _win_
        TFsPath lnkTarget = TFsPath(".").RealPath() / a.second;
        if (hasTmpfs && !a.first.EndsWith(TStringBuf(".debug"))) {
            NFs::Copy(lnkTarget, TFsPath("_yql_tmpfs") / a.second);
            lnkTarget = TFsPath("_yql_tmpfs") / a.second;
        }
        YQL_ENSURE(NFs::SymLink(lnkTarget, lnk), "Failed to create file alias from "
            << lnkTarget.GetPath().Quote() << " to " << lnk.GetPath().Quote()
            << ": " << LastSystemErrorText());
#endif
    }

    if (TFsPath(NCommon::PgCatalogFileName).Exists()) {
        TFileInput file(TString{NCommon::PgCatalogFileName});
        NPg::ImportExtensions(file.ReadAll(), false,
            NKikimr::NMiniKQL::CreateExtensionLoader().get());
    }

    FillStaticModules(*funcRegistry);
    for (const auto& mod: UdfModules) {
        auto path = mod.first;
#ifdef _win_
        path += ".dll";
#endif
        funcRegistry->LoadUdfs(path, {}, 0, mod.second);
    }

    FunctionRegistry.Reset(funcRegistry.Release());

    Alloc.Reset(new TScopedAlloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
        FunctionRegistry->SupportsSizedAllocators()));
    Env.Reset(new TTypeEnvironment(*Alloc));
    CodecCtx.Reset(new NCommon::TCodecContext(*Env, *FunctionRegistry));
    if (!GetEnv(TString("YQL_SUPPRESS_JOB_STATISTIC"))) {
        JobStats = CreateDefaultStatsRegistry();
    }
    SecureParamsProvider.Reset(new TEnvSecureParamsProvider("YT_SECURE_VAULT"));
}

void TYqlJobBase::Save(IOutputStream& s) const {
    ::SaveMany(&s,
        UdfModules,
        FileAliases,
        UdfValidateMode,
        OptLLVM,
        TableNames
    );
}

void TYqlJobBase::Load(IInputStream& s) {
    ::LoadMany(&s,
        UdfModules,
        FileAliases,
        UdfValidateMode,
        OptLLVM,
        TableNames
    );
}

void TYqlJobBase::Do(const NYT::TRawJobContext& jobContext) {
    DoImpl(jobContext.GetInputFile(), jobContext.GetOutputFileList());
    if (JobStats) {
        JobStats->SetStat(Job_ThreadsCount, GetRunnigThreadsCount());
    }
}

TCallableVisitFuncProvider TYqlJobBase::MakeTransformProvider(THashMap<TString, TRuntimeNode>* extraArgs) const {
    return TJobTransformProvider(extraArgs);
}

} // NYql
