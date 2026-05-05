#pragma once

#include "interface.h"

#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <util/generic/function.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>

namespace NYql::NPureCalc {
class TProgramFactory: public IProgramFactory {
private:
    TProgramFactoryOptions Options_;
    TExprContext ExprContext_;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry_;
    IModuleResolver::TPtr ModuleResolver_;
    TUserDataTable UserData_;
    EBlockEngineMode BlockEngineMode_;
    IOutputStream* ExprOutputStream_;
    THashMap<TString, TString> Modules_;
    NKikimr::NUdf::ICountersProvider* CountersProvider_;

public:
    explicit TProgramFactory(const TProgramFactoryOptions&);
    ~TProgramFactory() override;

public:
    void AddUdfModule(
        const TStringBuf& moduleName,
        NKikimr::NUdf::TUniquePtr<NKikimr::NUdf::IUdfModule>&& module) override;

    void SetCountersProvider(NKikimr::NUdf::ICountersProvider* provider) override;

    IPullStreamWorkerFactoryPtr MakePullStreamWorkerFactory(const TInputSpecBase&, const TOutputSpecBase&, TString, ETranslationMode, ui16) override;
    IPullListWorkerFactoryPtr MakePullListWorkerFactory(const TInputSpecBase&, const TOutputSpecBase&, TString, ETranslationMode, ui16) override;
    IPushStreamWorkerFactoryPtr MakePushStreamWorkerFactory(const TInputSpecBase&, const TOutputSpecBase&, TString, ETranslationMode, ui16) override;
};
} // namespace NYql::NPureCalc
