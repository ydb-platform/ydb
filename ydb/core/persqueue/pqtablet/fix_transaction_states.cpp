#include "fix_transaction_states.h"
#include <ydb/core/persqueue/common/key.h>
#include <util/generic/maybe.h>

namespace NKikimr::NPQ {

static const size_t TX_KEY_LENGTH = GetTxKey(0).size();

bool IsMainContextOfTransaction(const TString& key)
{
    Y_ENSURE(key.size() >= TX_KEY_LENGTH);
    return key.size() == TX_KEY_LENGTH;
}

void FixTransactionStates(const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges,
                          THashMap<ui64, TDistributedTransaction>& txs)
{
    TMaybe<ui64> txId;
    size_t current = 0;
    size_t expected = 0;

    for (const auto& readRange : readRanges) {
        for (size_t i = 0; i < readRange.PairSize(); ++i) {
            const auto& pair = readRange.GetPair(i);

            if (!IsMainContextOfTransaction(pair.GetKey())) {
                ++current;
                continue;
            }

            NKikimrPQ::TTransaction tx;
            AFL_ENSURE(tx.ParseFromString(pair.GetValue()));

            if (txId.Defined() && (current == expected)) {
                // All the parties managed to record the status. We need to fix the transaction status
                txs[*txId].State = NKikimrPQ::TTransaction::EXECUTED;
            }

            txId = tx.GetTxId();

            current = 0;
            expected = txs[*txId].Partitions.size();
        }
    }

    if (txId.Defined() && (current == expected)) {
        // All the partitions of the last transaction managed to record the status. We need to fix her status
        txs[*txId].State = NKikimrPQ::TTransaction::EXECUTED;
    }
}

}
