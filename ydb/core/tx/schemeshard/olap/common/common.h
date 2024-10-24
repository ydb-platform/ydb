#pragma once
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NSchemeShard {

    class IErrorCollector {
    public:
        virtual void AddError(const NEvSchemeShard::EStatus& errorStatus, const TString& errorMsg) = 0;
        virtual void AddError(const TString& errorMsg) = 0;
    };

    class TSimpleErrorCollector: public IErrorCollector {
        using TResult = TConclusionSpecialStatus<NEvSchemeShard::EStatus, NEvSchemeShard::EStatus::StatusSuccess, NKikimrScheme::StatusSchemeError>;
        TResult Result = TResult::Success();
    public:
        TSimpleErrorCollector() = default;

        const TResult* operator->() const {
            return &Result;
        }

        void AddError(const NEvSchemeShard::EStatus& errorStatus, const TString& errorMsg) override {
            AFL_VERIFY(Result.Ok());
            Result = TResult::Fail(errorStatus, errorMsg);
        }

        void AddError(const TString& errorMsg) override {
            AFL_VERIFY(Result.Ok());
            Result = TResult::Fail(errorMsg);
        }
    };

    class TProposeErrorCollector : public IErrorCollector {
        NEvSchemeShard::TEvModifySchemeTransactionResult& TxResult;
    public:
        TProposeErrorCollector(NEvSchemeShard::TEvModifySchemeTransactionResult& txResult)
            : TxResult(txResult)
        {}

        void AddError(const NEvSchemeShard::EStatus& errorStatus, const TString& errorMsg) override {
            TxResult.SetError(errorStatus, errorMsg);
        }

        void AddError(const TString& errorMsg) override {
            TxResult.SetError(NKikimrScheme::StatusSchemeError, errorMsg);
        }
    };
}
