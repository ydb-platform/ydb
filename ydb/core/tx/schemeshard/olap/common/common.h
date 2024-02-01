#pragma once
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr::NSchemeShard {

    class IErrorCollector {
    public:
        virtual void AddError(const TEvSchemeShard::EStatus& errorStatus, const TString& errorMsg) = 0;
        virtual void AddError(const TString& errorMsg) = 0;
    };

    class TProposeErrorCollector : public IErrorCollector {
        TEvSchemeShard::TEvModifySchemeTransactionResult& TxResult;
    public:
        TProposeErrorCollector(TEvSchemeShard::TEvModifySchemeTransactionResult& txResult)
            : TxResult(txResult)
        {}

        void AddError(const TEvSchemeShard::EStatus& errorStatus, const TString& errorMsg) override {
            TxResult.SetError(errorStatus, errorMsg);
        }

        void AddError(const TString& errorMsg) override {
            TxResult.SetError(NKikimrScheme::StatusSchemeError, errorMsg);
        }
    };
}
