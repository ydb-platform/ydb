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

ui32 GetPartitionsCount(const NKikimrPQ::TTransaction& tx)
{
    switch (tx.GetKind()) {
    case NKikimrPQ::TTransaction::KIND_DATA:
        return tx.OperationsSize();
    case NKikimrPQ::TTransaction::KIND_CONFIG:
        return tx.GetPartitions().PartitionSize();
    case NKikimrPQ::TTransaction::KIND_UNKNOWN:
        AFL_ENSURE(false);
    }
}

THashMap<ui64, NKikimrPQ::TTransaction> CollectTransactions(const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges)
{
    THashMap<ui64, NKikimrPQ::TTransaction> txs;
    TMaybe<ui64> txId;
    ui32 current = 0;
    ui32 expected = 0;

    for (const auto& readRange : readRanges) {
        for (size_t i = 0; i < readRange.PairSize(); ++i) {
            const auto& pair = readRange.GetPair(i);

            NKikimrPQ::TTransaction tx;
            AFL_ENSURE(tx.ParseFromString(pair.GetValue()));
            AFL_ENSURE(tx.GetKind() != NKikimrPQ::TTransaction::KIND_UNKNOWN);

            if (!IsMainContextOfTransaction(pair.GetKey())) {
                AFL_ENSURE(txId.Defined() && (*txId == tx.GetTxId()));
                ++current;
                AFL_ENSURE(current <= expected);
                tx.SetState((current == expected) ? NKikimrPQ::TTransaction::EXECUTED : NKikimrPQ::TTransaction::CALCULATED);
                txs.insert_or_assign(*txId, std::move(tx));
                continue;
            }

            txId = tx.GetTxId();
            current = 0;
            expected = GetPartitionsCount(tx);

            txs.insert_or_assign(*txId, std::move(tx));
        }
    }

    return txs;
}

void FixTransactionStates(const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges,
                          THashMap<ui64, TDistributedTransaction>& txs)
{
    TMaybe<ui64> txId;
    TMaybe<ui64> step;
    TMaybe<bool> participantsPredicate;
    TMaybe<bool> selfPredicate;
    size_t current = 0;
    size_t expected = 0;

    auto fixTransaction = [&](TDistributedTransaction& tx) {
        tx.State = NKikimrPQ::TTransaction::EXECUTED;
        if (step.Defined()) {
            tx.Step = *step;
        }
        if (participantsPredicate.Defined()) {
            tx.ParticipantsDecision = *participantsPredicate
                ? NKikimrTx::TReadSetData::DECISION_COMMIT
                : NKikimrTx::TReadSetData::DECISION_ABORT;
        }
        if (selfPredicate.Defined()) {
            tx.SelfDecision = *selfPredicate
                ? NKikimrTx::TReadSetData::DECISION_COMMIT
                : NKikimrTx::TReadSetData::DECISION_ABORT;
        }
    };

    for (const auto& readRange : readRanges) {
        for (size_t i = 0; i < readRange.PairSize(); ++i) {
            const auto& pair = readRange.GetPair(i);

            NKikimrPQ::TTransaction tx;
            AFL_ENSURE(tx.ParseFromString(pair.GetValue()));

            if (tx.HasStep()) {
                AFL_ENSURE(!step.Defined() || (*step == tx.GetStep()));
                step = tx.GetStep();
            }

            if (tx.HasPredicate()) {
                AFL_ENSURE(!selfPredicate.Defined() || (*selfPredicate == tx.GetPredicate()));
                selfPredicate = tx.GetPredicate();
            }

            if (tx.PredicatesReceivedSize()) {
                TMaybe<bool> predicate;

                for (const auto& e : tx.GetPredicatesReceived()) {
                    if (!e.HasPredicate()) {
                        predicate = Nothing();
                        break;
                    }
                    if (!e.GetPredicate()) {
                        predicate = false;
                        break;
                    }
                    predicate = true;
                }

                AFL_ENSURE(!participantsPredicate.Defined() || (participantsPredicate == predicate));
                participantsPredicate = predicate;
            }

            if (!IsMainContextOfTransaction(pair.GetKey())) {
                ++current;
                continue;
            }

            if (txId.Defined() && (current == expected)) {
                // All the parties managed to record the status. We need to fix the transaction status
                fixTransaction(txs[*txId]);
            }

            txId = tx.GetTxId();

            current = 0;
            expected = txs[*txId].Partitions.size();
        }
    }

    if (txId.Defined() && (current == expected)) {
        // All the partitions of the last transaction managed to record the status. We need to fix her status
        fixTransaction(txs[*txId]);
    }
}

}
