#include "propose_transaction_base.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>


namespace NKikimr::NColumnShard {

    void TProposeTransactionBase::ProposeTransaction(const TTxController::TBasicTxInfo& txInfo, const TString& txBody, const TActorId source, const ui64 cookie, TTransactionContext& txc) {
        auto txOperator = TTxController::ITransactionOperatior::TFactory::MakeHolder(txInfo.TxKind, TTxController::TTxInfo(txInfo.TxKind, txInfo.TxId));
        if (!txOperator || !txOperator->Parse(txBody)) {
            TTxController::TProposeResult proposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, TStringBuilder() << "Error processing commit TxId# " << txInfo.TxId
                                                    << (txOperator ? ". Parsing error " : ". Unknown operator for txKind"));
            OnProposeError(proposeResult, txInfo);
            return;
        }

        auto txInfoPtr = Self->GetProgressTxController().GetTxInfo(txInfo.TxId);
        if (!!txInfoPtr) {
            if (txInfoPtr->Source != source || txInfoPtr->Cookie != cookie) {
                TTxController::TProposeResult proposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, TStringBuilder() << "Another commit TxId# " << txInfo.TxId << " has already been proposed");
                OnProposeError(proposeResult, txInfo);
            }
            TTxController::TProposeResult proposeResult;
            OnProposeResult(proposeResult, *txInfoPtr);
        } else {
            auto proposeResult = txOperator->Propose(*Self, txc, false);
            if (!!proposeResult) {
                const auto fullTxInfo = txOperator->TxWithDeadline() ? Self->GetProgressTxController().RegisterTxWithDeadline(txInfo.TxId, txInfo.TxKind, txBody, source, cookie, txc)
                                                                : Self->GetProgressTxController().RegisterTx(txInfo.TxId, txInfo.TxKind, txBody, source, cookie, txc);

                OnProposeResult(proposeResult, fullTxInfo);
            } else {
                OnProposeError(proposeResult, txInfo);
            }
        }
    }
}
