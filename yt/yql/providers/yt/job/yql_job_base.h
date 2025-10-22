#pragma once

#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/public/udf/udf_validate.h>
#include <yql/essentials/public/udf/udf_counter.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/io/job_reader.h>
#include <yt/cpp/mapreduce/io/job_writer.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>


namespace NKikimr {
    namespace NMiniKQL {
        class IFunctionRegistry;
    }
}

class IRandomProvider;
class ITimeProvider;

namespace NYql {

struct TJobCountersProvider : public NKikimr::NUdf::ICountersProvider, public NKikimr::NUdf::IScopedProbeHost {
    NKikimr::NUdf::TCounter GetCounter(const NKikimr::NUdf::TStringRef& module, const NKikimr::NUdf::TStringRef& name, bool deriv) override;
    NKikimr::NUdf::TScopedProbe GetScopedProbe(const NKikimr::NUdf::TStringRef& module, const NKikimr::NUdf::TStringRef& name) override;
    void Acquire(void* cookie) override;
    void Release(void* cookie) override;

    struct TProbeState {
        i64 TotalCycles = 0;
        i64 LastAcquire;
    };

    THashMap<std::pair<TString, TString>, i64> Counters_;
    THashMap<std::pair<TString, TString>, TProbeState> Probes_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TYqlJobBase {
protected:
    TYqlJobBase() = default;
    virtual ~TYqlJobBase();

public:
    void AddUdfModule(const TString& udfModule, const TString& udfPrefix) {
        UdfModules.insert({udfModule, udfPrefix});
    }
    void AddFileAlias(const TString& alias, const TString& filePath) {
        FileAliases[alias] = filePath;
    }

    void SetUdfValidateMode(NKikimr::NUdf::EValidateMode mode) {
        UdfValidateMode = mode;
    }

    void SetOptLLVM(const TString& optLLVM) {
        OptLLVM = optLLVM;
    }

    void SetTableNames(const TVector<TString>& tableNames) {
        TableNames = tableNames;
    }

    void SetRuntimeLogLevel(NUdf::ELogLevel level) {
        RuntimeLogLevel = level;
    }

    void SetLangVer(TLangVersion langver) {
        LangVer = langver;
    }

    virtual void Save(IOutputStream& stream) const;
    virtual void Load(IInputStream& stream);

protected:
    NKikimr::NMiniKQL::TCallableVisitFuncProvider MakeTransformProvider(THashMap<TString, NKikimr::NMiniKQL::TRuntimeNode>* extraArgs = nullptr, const TString& prefix = "Yt") const;

    void Init();

    void Finish();

protected:
    // Serializable part (don't forget to add new members to Save/Load)
    THashMap<TString, TString> UdfModules; // udf module path -> udf module prefix
    THashMap<TString, TString> FileAliases;
    NKikimr::NUdf::EValidateMode UdfValidateMode = NKikimr::NUdf::EValidateMode::None;
    TString OptLLVM;
    TVector<TString> TableNames;
    NUdf::ELogLevel RuntimeLogLevel = NUdf::ELogLevel::Info;
    TLangVersion LangVer = UnknownLangVersion;
    // End serializable part

    ui64 StartCycles = 0;
    ui64 StartTime = 0;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    THolder<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    THolder<NKikimr::NMiniKQL::TTypeEnvironment> Env;
    NKikimr::NMiniKQL::IStatsRegistryPtr JobStats;
    TJobCountersProvider JobCountersProvider;
    THolder<NKikimr::NUdf::ISecureParamsProvider> SecureParamsProvider;
    NUdf::TUniquePtr<NUdf::ILogProvider> LogProvider;
    THolder<NCommon::TCodecContext> CodecCtx;
};

} // NYql
