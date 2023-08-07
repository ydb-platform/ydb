#include "yql_job_user.h"
#include "yql_job_factory.h"

#include <ydb/library/yql/providers/yt/lib/lambda_builder/lambda_builder.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/schema/parser/yql_type_parser.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <yt/cpp/mapreduce/client/structured_table_formats.h>
#include <yt/cpp/mapreduce/io/yamr_table_reader.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/maybe.h>
#include <util/generic/xrange.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/system/rusage.h>
#include <util/system/datetime.h>
#include <util/ysaveload.h>


namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {

    const static TStatKey Mkql_TotalRuntimeNodes("Mkql_TotalRuntimeNodes", false);
    const static TStatKey Mkql_BuildGraphRssDelta("Mkql_BuildGraphRssDelta", false);
    const static TStatKey Job_InitTime("Job_InitTime", false);
    const static TStatKey Job_CalcTime("Job_CalcTime", false);

    NYT::TFormat MakeTableYaMRFormat(const TString& inputSpec) {
        NYT::TNode inAttrs;
        TStringStream err;
        if (!NCommon::ParseYson(inAttrs, inputSpec, err)) {
            ythrow yexception() << "Invalid input attrs: " << err.Str();
        }
        YQL_ENSURE(inAttrs.IsMap(), "Expect Map type of output meta attrs, but got type " << inAttrs.GetType());
        YQL_ENSURE(inAttrs.HasKey(YqlIOSpecTables), "Expect " << TString{YqlIOSpecTables}.Quote() << " key");

        auto& inputSpecs = inAttrs[YqlIOSpecTables].AsList();
        YQL_ENSURE(!inputSpecs.empty(), "Expect list with at least one element in input attrs: " << inputSpec);

        TVector<TMaybe<NYT::TNode>> formats;
        THashMap<TString, NYT::TNode> specRegistry;
        for (auto& attrs: inputSpecs) {
            NYT::TNode spec;
            if (attrs.IsString()) {
                auto refName = attrs.AsString();
                if (auto p = specRegistry.FindPtr(refName)) {
                    spec = *p;
                } else {
                    YQL_ENSURE(inAttrs.HasKey(YqlIOSpecRegistry) && inAttrs[YqlIOSpecRegistry].HasKey(refName), "Bad input registry reference: " << refName);
                    NYT::TNode& r = specRegistry[refName];
                    r = inAttrs[YqlIOSpecRegistry][refName];
                    spec = r;
                }
            } else {
                spec = attrs;
            }
            formats.push_back(spec.HasKey(FORMAT_ATTR_NAME) ? MakeMaybe(spec[FORMAT_ATTR_NAME]) : Nothing());
        }

        NYT::TNode format = NYT::GetCommonTableFormat(formats).GetOrElse(NYT::TNode("yamred_dsv"));
        format.Attributes()["lenval"] = true;
        format.Attributes()["has_subkey"] = true;
        format.Attributes()["enable_table_index"] = true;
        return NYT::TFormat(format);
    }
}


std::pair<NYT::TFormat, NYT::TFormat> TYqlUserJob::GetIOFormats(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry) const {
    if (!UseSkiff) {
        return std::make_pair(YamrInput ? MakeTableYaMRFormat(InputSpec) : NYT::TFormat::YsonBinary(), NYT::TFormat::YsonBinary());
    }
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    NCommon::TCodecContext codecCtx(env, *functionRegistry);

    TType* itemType = nullptr;
    if (InputType) {
        TStringStream err;
        TProgramBuilder pgmBuilder(env, *functionRegistry);
        itemType = NCommon::ParseTypeFromYson(TStringBuf{InputType}, pgmBuilder, err);
        YQL_ENSURE(itemType, << err.Str());
    }

    TMkqlIOSpecs specs;
    specs.SetUseSkiff(OptLLVM, SkiffSysFields);
    specs.Init(codecCtx, InputSpec, InputGroups, TableNames, itemType, AuxColumns, OutSpec);

    return std::make_pair(YamrInput ? MakeTableYaMRFormat(InputSpec) : specs.MakeInputFormat(AuxColumns), specs.MakeOutputFormat());
}

void TYqlUserJob::Save(IOutputStream& s) const {
    TYqlJobBase::Save(s);
    ::SaveMany(&s,
        UseSkiff,
        SkiffSysFields,
        YamrInput,
        LambdaCode,
        InputSpec,
        OutSpec,
        InputGroups,
        AuxColumns,
        InputType,
        RowOffsets
    );
}

void TYqlUserJob::Load(IInputStream& s) {
    TYqlJobBase::Load(s);
    ::LoadMany(&s,
        UseSkiff,
        SkiffSysFields,
        YamrInput,
        LambdaCode,
        InputSpec,
        OutSpec,
        InputGroups,
        AuxColumns,
        InputType,
        RowOffsets
    );
}

void TYqlUserJob::DoImpl(const TFile& inHandle, const TVector<TFile>& outHandles) {
    TYqlJobBase::Init();

    TLambdaBuilder builder(FunctionRegistry.Get(), *Alloc,
        Env.Get(), RandomProvider.Get(), TimeProvider.Get(), JobStats.Get(), &JobCountersProvider, SecureParamsProvider.Get());

    TType* itemType = nullptr;
    if (InputType) {
        TStringStream err;
        TProgramBuilder pgmBuilder(*Env, *FunctionRegistry);
        itemType = NCommon::ParseTypeFromYson(TStringBuf{InputType}, pgmBuilder, err);
        YQL_ENSURE(itemType, << err.Str());
    }

    YQL_ENSURE(LambdaCode);
    TRuntimeNode rootNode = DeserializeRuntimeNode(LambdaCode, *Env);
    THashMap<TString, TRuntimeNode> extraArgs;
    rootNode = builder.TransformAndOptimizeProgram(rootNode, MakeTransformProvider(&extraArgs));

    MkqlIOSpecs.Reset(new TMkqlIOSpecs());
    if (UseSkiff) {
        MkqlIOSpecs->SetUseSkiff(OptLLVM, SkiffSysFields);
    }
    MkqlIOSpecs->Init(*CodecCtx, InputSpec, InputGroups, TableNames, itemType, AuxColumns, OutSpec, JobStats.Get());
    if (!RowOffsets.empty()) {
        MkqlIOSpecs->SetTableOffsets(RowOffsets);
    }

    TIntrusivePtr<TJobMkqlWriterImpl> mkqlWriter = MakeIntrusive<TJobMkqlWriterImpl>(*MkqlIOSpecs, outHandles);
    TIntrusivePtr<NYT::IReaderImplBase> reader;

    if (itemType) {
        if (YamrInput) {
            reader = MakeIntrusive<NYT::TYaMRTableReader>(MakeIntrusive<NYT::TJobReader>(inHandle));
        }
        else {
            reader = MakeIntrusive<TJobMkqlReaderImpl>(inHandle);
        }
    }

    std::vector<NKikimr::NMiniKQL::TNode*> entryPoints(1, rootNode.GetNode());
    for (auto& item: extraArgs) {
        entryPoints.push_back(item.second.GetNode());
    }
    auto maxRss = TRusage::Get().MaxRss;
    CompGraph = builder.BuildGraph(
        GetJobFactory(*CodecCtx, OptLLVM, MkqlIOSpecs.Get(), reader.Get(), mkqlWriter.Get()),
        UdfValidateMode,
        NUdf::EValidatePolicy::Fail, OptLLVM,
        EGraphPerProcess::Single,
        Explorer,
        rootNode,
        std::move(entryPoints)
    );

    MKQL_SET_STAT(JobStats, Mkql_BuildGraphRssDelta, TRusage::Get().MaxRss - maxRss);
    MKQL_SET_STAT(JobStats, Mkql_TotalRuntimeNodes, Explorer.GetNodes().size());
    MKQL_SET_STAT(JobStats, Job_InitTime, (ThreadCPUTime() - StartTime) / 1000);

    auto beginCalcTime = ThreadCPUTime();

    if (CompGraph) {
        for (size_t i: xrange(extraArgs.size())) {
            if (auto entry = CompGraph->GetEntryPoint(i + 1, false)) {
                entry->SetValue(CompGraph->GetContext(), NUdf::TUnboxedValue::Zero());
            }
        }

        CodecCtx->HolderFactory = &CompGraph->GetHolderFactory();
        CompGraph->Prepare();
        BindTerminator.Reset(new TBindTerminator(CompGraph->GetTerminator()));

        if (auto mkqlReader = dynamic_cast<TJobMkqlReaderImpl*>(reader.Get())) {
            mkqlReader->SetSpecs(*MkqlIOSpecs, CompGraph->GetHolderFactory());
            mkqlReader->Next(); // Prefetch first record to unify behavior with TYaMRTableReader
        }
    }

    NUdf::TUnboxedValue value = CompGraph->GetValue();
    if (rootNode.GetStaticType()->IsStream()) {
        NUdf::TUnboxedValue item;
        const auto status = value.Fetch(item);
        YQL_ENSURE(status == NUdf::EFetchStatus::Finish);
    } else {
        YQL_ENSURE(value.IsFinish());
    }

    MKQL_SET_STAT(JobStats, Job_CalcTime, (ThreadCPUTime() - beginCalcTime) / 1000);

    if (auto mkqlReader = dynamic_cast<TJobMkqlReaderImpl*>(reader.Get())) {
        mkqlReader->Finish();
    }
    reader.Drop();
    mkqlWriter->Finish();
    mkqlWriter.Drop();

    MkqlIOSpecs->Clear();
    MkqlIOSpecs.Destroy();
}

} // NYql
