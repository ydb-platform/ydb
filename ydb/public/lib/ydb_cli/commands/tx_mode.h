#pragma once

namespace NYdb::NConsoleClient {

enum class ETxMode {
    SerializableRW /* "serializable-rw" */,
    OnlineRO /* "online-ro" */,
    StaleRO /* "stale-ro" */,
    SnapshotRO /* "snapshot-ro" */,
    SnapshotRW /* "snapshot-rw" */,
    NoTx /* "no-tx" */,
};

} // namespace NYdb::NConsoleClient
