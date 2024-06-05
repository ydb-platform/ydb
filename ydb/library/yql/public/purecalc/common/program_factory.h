#pragma once

#include "interface.h"

#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_user_data.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <util/generic/function.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>

namespace NYql {
    namespace NPureCalc {
        class TProgramFactory: public IProgramFactory {
        private:
            TProgramFactoryOptions Options_;
            TExprContext ExprContext_;
            TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry_;
            IModuleResolver::TPtr ModuleResolver_;
            TUserDataTable UserData_;
            EBlockEngineMode BlockEngineMode_;
            THashMap<TString, TString> Modules_;
            NKikimr::NUdf::ICountersProvider* CountersProvider_;

        public:
            explicit TProgramFactory(const TProgramFactoryOptions&);
            ~TProgramFactory() override;

        public:
            void AddUdfModule(
                const TStringBuf& moduleName,
                NKikimr::NUdf::TUniquePtr<NKikimr::NUdf::IUdfModule>&& module
            ) override;

            void SetCountersProvider(NKikimr::NUdf::ICountersProvider* provider) override;

            IPullStreamWorkerFactoryPtr MakePullStreamWorkerFactory(const TInputSpecBase&, const TOutputSpecBase&, TString, ETranslationMode, ui16) override;
            IPullListWorkerFactoryPtr MakePullListWorkerFactory(const TInputSpecBase&, const TOutputSpecBase&, TString, ETranslationMode, ui16) override;
            IPushStreamWorkerFactoryPtr MakePushStreamWorkerFactory(const TInputSpecBase&, const TOutputSpecBase&, TString, ETranslationMode, ui16) override;
        };
    }
}
