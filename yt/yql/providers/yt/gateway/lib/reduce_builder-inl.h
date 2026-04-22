#pragma once

namespace NYql {

template<class ExecCtxPtr>
TString TReduceJobBuilder::SetReduceLambdaCode(TYqlUserJobBase* reduceJob, NNodes::TYtReduce reduce, ExecCtxPtr execCtx, TExprContext& ctx) {
    TString reduceLambda;
    {
        TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
            execCtx->FunctionRegistry_->SupportsSizedAllocators());
        alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
        TGatewayLambdaBuilder builder(execCtx->FunctionRegistry_, alloc);
        reduceLambda = builder.BuildLambdaWithIO(*execCtx->MkqlCompiler_, reduce.Reducer(), ctx);
    }
    reduceJob->SetLambdaCode(reduceLambda);
    return reduceLambda;
}

template<class ExecCtxPtr>
void TReduceJobBuilder::SetReduceJobParams(
    TYqlUserJobBase* reduceJob,
    ExecCtxPtr execCtx,
    const TVector<ui32>& groups,
    const TVector<TString>& tables,
    const TVector<ui64>& rowOffsets,
    const THashSet<TString>& auxColumns
) {
    const bool useSkiff = execCtx->Options_.Config()->UseSkiff.Get(execCtx->Cluster_).GetOrElse(DEFAULT_USE_SKIFF);
    const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);
    reduceJob->SetInputSpec(execCtx->GetInputSpec(!useSkiff, nativeTypeCompat, false));
    reduceJob->SetOutSpec(execCtx->GetOutSpec(!useSkiff, nativeTypeCompat));

    YQL_ENSURE(!groups.empty());
    if (groups.back() != 0) {
        reduceJob->SetInputGroups(groups);
    }
    reduceJob->SetTableNames(tables);
    reduceJob->SetRowOffsets(rowOffsets);

    reduceJob->SetAuxColumns(auxColumns);
    reduceJob->SetUseSkiff(useSkiff, TMkqlIOSpecs::ESystemField::RowIndex | TMkqlIOSpecs::ESystemField::KeySwitch);
    reduceJob->SetYamrInput(execCtx->YamrInput);
    reduceJob->SetOptLLVM(execCtx->Options_.OptLLVM());
    reduceJob->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());
    reduceJob->SetRuntimeLogLevel(execCtx->Options_.RuntimeLogLevel());
    reduceJob->SetLangVer(execCtx->Options_.LangVer());
}

} // namespace NYql
