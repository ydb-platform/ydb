#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>

#include <util/stream/output.h>

namespace NYdb::inline Dev::NQuery {

//! Additional settings for an online read-only transaction.
struct TTxOnlineSettings {
    using TSelf = TTxOnlineSettings;

    //! Allows an individual read to observe inconsistent data; disabled by default.
    FLUENT_SETTING_DEFAULT(bool, AllowInconsistentReads, false);

    //! Constructs online read-only settings with consistent individual reads.
    TTxOnlineSettings() {}
};

//! Selects the isolation and access mode of a query transaction.
struct TTxSettings {
    using TSelf = TTxSettings;

    //! Constructs serializable read-write transaction settings.
    TTxSettings()
        : Mode_(TS_SERIALIZABLE_RW) {}

    //! Creates serializable read-write transaction settings.
    static TTxSettings SerializableRW() {
        return TTxSettings(TS_SERIALIZABLE_RW);
    }

    //! Creates online read-only transaction settings.
    static TTxSettings OnlineRO(const TTxOnlineSettings& settings = TTxOnlineSettings()) {
        return TTxSettings(TS_ONLINE_RO).OnlineSettings(settings);
    }

    //! Creates stale read-only transaction settings.
    static TTxSettings StaleRO() {
        return TTxSettings(TS_STALE_RO);
    }

    //! Creates snapshot read-only transaction settings.
    static TTxSettings SnapshotRO() {
        return TTxSettings(TS_SNAPSHOT_RO);
    }

    //! Creates snapshot read-write transaction settings.
    static TTxSettings SnapshotRW() {
        return TTxSettings(TS_SNAPSHOT_RW);
    }

    //! Creates read-committed read-write transaction settings.
    static TTxSettings ReadCommittedRW() {
        return TTxSettings(TS_READ_COMMITTED_RW);
    }

    //! Creates strict-serializable read-write transaction settings.
    static TTxSettings StrictSerializableRW() {
        return TTxSettings(TS_STRICT_SERIALIZABLE_RW);
    }

    //! Writes a human-readable transaction mode name to out.
    void Out(IOutputStream& out) const {
        switch (Mode_) {
        case TS_SERIALIZABLE_RW:
            out << "SerializableRW";
            break;
        case TS_ONLINE_RO:
            out << "OnlineRO";
            break;
        case TS_STALE_RO:
            out << "StaleRO";
            break;
        case TS_SNAPSHOT_RO:
            out << "SnapshotRO";
            break;
        case TS_SNAPSHOT_RW:
            out << "SnapshotRW";
            break;
        case TS_READ_COMMITTED_RW:
            out << "ReadCommittedRW";
            break;
        case TS_STRICT_SERIALIZABLE_RW:
            out << "StrictSerializableRW";
            break;
        default:
            out << "Unknown";
            break;
        }
    }

    //! Transaction isolation and access modes supported by Query Service.
    enum ETransactionMode {
        //! Serializable read-write mode.
        TS_SERIALIZABLE_RW,
        //! Online read-only mode.
        TS_ONLINE_RO,
        //! Stale read-only mode.
        TS_STALE_RO,
        //! Snapshot read-only mode.
        TS_SNAPSHOT_RO,
        //! Snapshot read-write mode.
        TS_SNAPSHOT_RW,
        //! Read-committed read-write mode.
        TS_READ_COMMITTED_RW,
        //! Strict-serializable read-write mode.
        TS_STRICT_SERIALIZABLE_RW,
    };

    //! Sets options used by online read-only mode.
    FLUENT_SETTING(TTxOnlineSettings, OnlineSettings);

    //! Returns the selected transaction mode.
    ETransactionMode GetMode() const {
        return Mode_;
    }
private:
    TTxSettings(ETransactionMode mode)
        : Mode_(mode) {}

    ETransactionMode Mode_;
};

} // namespace NYdb::NQuery
