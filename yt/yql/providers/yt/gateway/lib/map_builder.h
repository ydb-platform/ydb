#pragma once

#include "exec_ctx.h"
#include "qb2.h"
#include <yt/yql/providers/yt/job/yql_job_user_base.h>
#include <yt/yql/providers/yt/lib/lambda_builder/lambda_builder.h>
#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

class TMapJobBuilder: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TMapJobBuilder>;

    virtual ~TMapJobBuilder() = default;

    TMapJobBuilder(const TString& jobPrefix = "Yt");

    template<class ExecCtxPtr>
    void SetMapJobParams(
        TYqlUserJobBase* mapJob,
        ExecCtxPtr execCtx,
        TRemapperMap& remapperMap,
        TSet<TString>& remapperAllFiles,
        bool& useSkiff,
        bool& forceYsonInputFormat,
        bool testRun
    ) {

        TVector<ui32> groups;
        TVector<TString> tables;
        TVector<ui64> rowOffsets;
        ui64 currentRowOffset = 0;

        bool hasTablesWithoutQB2Premapper = false;
        ui64 inputsNum = 0;

        for (const TInputInfo& table: execCtx->InputTables_) {
            auto tablePath = table.Path;
            if (!table.QB2Premapper.IsUndefined()) {
                bool tableUseSkiff = false;

                ProcessTableQB2Premapper(table.QB2Premapper, table.Name, tablePath, inputsNum,
                    remapperMap, remapperAllFiles, tableUseSkiff);

                useSkiff = useSkiff && tableUseSkiff;
            }
            else {
                hasTablesWithoutQB2Premapper = true;
            }

            if (!groups.empty() && groups.back() != table.Group) {
                currentRowOffset = 0;
            }

            ++inputsNum;
            groups.push_back(table.Group);
            tables.push_back(table.Temp ? TString() : table.Name);
            rowOffsets.push_back(currentRowOffset);
            currentRowOffset += table.Records;
        }

        if (useSkiff && !remapperMap.empty()) {
            // Disable skiff in case of mix of QB2 and normal tables
            if (hasTablesWithoutQB2Premapper) {
                useSkiff = false;
            } else {
                UpdateQB2PremapperUseSkiff(remapperMap, useSkiff);
                forceYsonInputFormat = useSkiff;
            }
        }

        const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);
        mapJob->SetInputSpec(execCtx->GetInputSpec(!useSkiff || forceYsonInputFormat, nativeTypeCompat, false));
        mapJob->SetOutSpec(execCtx->GetOutSpec(!useSkiff, nativeTypeCompat));
        if (!groups.empty() && groups.back() != 0) {
            mapJob->SetInputGroups(groups);
        }
        mapJob->SetTableNames(tables);
        mapJob->SetRowOffsets(rowOffsets);


        mapJob->SetYamrInput(execCtx->YamrInput);
        mapJob->SetUseSkiff(useSkiff, testRun ? TMkqlIOSpecs::ESystemField(0) : TMkqlIOSpecs::ESystemField::RowIndex);

        mapJob->SetOptLLVM(execCtx->Options_.OptLLVM());
        mapJob->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());
        mapJob->SetRuntimeLogLevel(execCtx->Options_.RuntimeLogLevel());
        mapJob->SetLangVer(execCtx->Options_.LangVer());
    }

    template<class ExecCtxPtr>
    TString SetMapLambdaCode(TYqlUserJobBase* mapJob, NNodes::TYtMap map, ExecCtxPtr execCtx, TExprContext& ctx) {
        TString mapLambda;
        {
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                execCtx->FunctionRegistry_->SupportsSizedAllocators());
            alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TGatewayLambdaBuilder builder(execCtx->FunctionRegistry_, alloc);
            mapLambda = builder.BuildLambdaWithIO(Prefix_, *execCtx->MkqlCompiler_, map.Mapper(), ctx);
        }
        mapJob->SetLambdaCode(mapLambda);
        return mapLambda;
    }

    void SetBlockInput(TYqlUserJobBase* mapJob, NNodes::TYtMap map);

    void SetBlockOutput(TYqlUserJobBase* mapJob, NNodes::TYtMap map);

    void SetInputType(TYqlUserJobBase* mapJob, NNodes::TYtMap map);

private:
    const TString Prefix_;
};

} // namespace NYql
