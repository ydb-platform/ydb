#pragma once

#include <util/stream/str.h>

#include <ydb/core/protos/data_integrity_trails.pb.h>
#include <ydb/library/actors/struct_log/structured_message.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

namespace NKikimr {
namespace NDataIntegrity {

inline void LogKeyValue(const TStringBuf key, const TStringBuf value, NActors::NStructuredLog::TStructuredMessage& ss) {
    NActors::NStructuredLog::TKeyName keyName(key);
    ss.AppendValue({std::move(keyName)}, TString(value));
}

template <class TransactionSettings>
inline void LogTxSettings(const TransactionSettings& txSettings, NActors::NStructuredLog::TStructuredMessage& ss) {
    switch (txSettings.tx_mode_case()) {
        case TransactionSettings::kSerializableReadWrite:
            LogKeyValue("TxMode", "SerializableReadWrite", ss);
            break;
        case TransactionSettings::kOnlineReadOnly:
            LogKeyValue("TxMode", "OnlineReadOnly", ss);
            LogKeyValue("AllowInconsistentReads", txSettings.online_read_only().allow_inconsistent_reads() ? "true" : "false", ss);
            break;
        case TransactionSettings::kStaleReadOnly:
            LogKeyValue("TxMode", "StaleReadOnly", ss);
            break;
        case TransactionSettings::kSnapshotReadOnly:
            LogKeyValue("TxMode", "SnapshotReadOnly", ss);
            break;
        case TransactionSettings::kSnapshotReadWrite:
            LogKeyValue("TxMode", "SnapshotReadWrite", ss);
            break;
        case TransactionSettings::kReadCommittedReadWrite:
            LogKeyValue("TxMode", "ReadCommittedReadWrite", ss);
            break;
        case TransactionSettings::TX_MODE_NOT_SET:
            LogKeyValue("TxMode", "Undefined", ss);
            break;
    }
}

template <class TxControl>
inline void LogTxControl(const TxControl& txControl, NActors::NStructuredLog::TStructuredMessage& ss)
{
    switch (txControl.tx_selector_case()) {
        case TxControl::kTxId:
            LogKeyValue("TxId", txControl.tx_id(), ss);
            break;
        case TxControl::kBeginTx:
            LogKeyValue("BeginTx", "true", ss);
            LogTxSettings(txControl.begin_tx(), ss);
            break;
        case TxControl::TX_SELECTOR_NOT_SET:
            break;
    }

    LogKeyValue("NeedCommitTx", txControl.commit_tx() ? "true" : "false", ss);
}

}
}
