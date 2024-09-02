#pragma once

namespace NKikimr {
namespace NDataIntegrity {

inline void LogKeyValue(const TString& key, const TString& value, TStringStream& ss, bool last = false) {
    ss << key << ": " << (value.Empty() ? "Empty" : value) << (last ? "" : ",");
}

template <class TransactionSettings>
inline void LogTxSettings(const TransactionSettings& txSettings, TStringStream& ss) {
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
        case TransactionSettings::TX_MODE_NOT_SET:
            LogKeyValue("TxMode", "Undefined", ss);
            break;
    }
}    

template <class TxControl>
inline void LogTxControl(const TxControl& txControl, TStringStream& ss)
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
