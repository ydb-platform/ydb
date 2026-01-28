#include "fix_transaction_states.h"
#include <ydb/core/persqueue/common/key.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

static const size_t TX_KEY_LENGTH = GetTxKey(0).size();

bool IsMainContextOfTransaction(const TString& key)
{
    AFL_ENSURE(key.size() >= TX_KEY_LENGTH);
    return key.size() == TX_KEY_LENGTH;
}

ui32 GetPartitionsCount(const NKikimrPQ::TTransaction& tx)
{
    switch (tx.GetKind()) {
    case NKikimrPQ::TTransaction::KIND_DATA:
        return tx.OperationsSize();
    case NKikimrPQ::TTransaction::KIND_CONFIG:
        return tx.GetTabletConfig().PartitionsSize();
    case NKikimrPQ::TTransaction::KIND_UNKNOWN:
        AFL_ENSURE(false);
        return 0;
    }
}

THashMap<ui64, NKikimrPQ::TTransaction> CollectTransactions(const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges)
{
    THashMap<ui64, NKikimrPQ::TTransaction> txs;
    TMaybe<ui64> txId;
    ui32 current = 0;
    ui32 expected = 0;
    NKikimrPQ::TTransaction::EState state = NKikimrPQ::TTransaction::UNKNOWN;

    for (const auto& readRange : readRanges) {
        for (size_t i = 0; i < readRange.PairSize(); ++i) {
            const auto& pair = readRange.GetPair(i);

            NKikimrPQ::TTransaction tx;
            AFL_ENSURE(tx.ParseFromString(pair.GetValue()));
            AFL_ENSURE(tx.GetKind() != NKikimrPQ::TTransaction::KIND_UNKNOWN);

            if (IsMainContextOfTransaction(pair.GetKey())) {
                txId = tx.GetTxId();
                current = 0;
                expected = GetPartitionsCount(tx);
                state = tx.GetState();
                if (state == NKikimrPQ::TTransaction::CALCULATED) {
                    tx.SetState(NKikimrPQ::TTransaction::PLANNED);
                }
            } else {
                // не может быть бесхозных субтранзакций
                AFL_ENSURE(txId.Defined() && (*txId == tx.GetTxId()));
                ++current;
                AFL_ENSURE(current <= expected);
                // если есть записи от всех субтранзакций, то партиции уже выполнили транзакцию (stable-26-1)
                // если у основной транзакции статус EXECUTED, то таблетка уже выполнила транзакцию (stable-25-4)
                // иначе нам надо вернуть транзакцию в состояние PLANNED
                tx.SetState(((current == expected) || (state == NKikimrPQ::TTransaction::EXECUTED))
                            ? NKikimrPQ::TTransaction::EXECUTED
                            : NKikimrPQ::TTransaction::PLANNED);
            }

            txs.insert_or_assign(*txId, std::move(tx));
        }
    }

    // последняя транзакция
    // --------------------
    // если есть записи от всех субтранзакций, то сработает уcловие выше (EXECUTED)
    // если основная транзакция в состоянии EXECUTED, то сработает условие выше (EXECUTED)
    // если есть хоть одна субтранзакция, то сработает условие выше (PLANNED)
    // если нет ни одной субтранзакции, то либо останется в PREPARED, либо поменяется на PLANNED

    return txs;
}

}
