#include "write_orbit.h"

#include "probes.h"

#include <ydb/library/actors/core/monotonic.h>

namespace NKikimr::NColumnShard {

LWTRACE_USING(YDB_CS);

namespace {
ui64 PathIdOf(const NEvWrite::TWriteMeta& meta) {
    return meta.GetPathId().GetInternalPathId().GetRawValue();
}
}   // namespace

void TrackStartWrite(NLWTrace::TOrbit& orbit, ui64 pathId, ui64 tabletId, ui64 txId, ui64 cookie, const TString& sender,
    TDuration writeTimeout, ui64 size, const TString& modificationType, bool isBulk) {
    LWTRACK(StartWrite, orbit, pathId, tabletId, txId, cookie, sender, writeTimeout, size, modificationType, isBulk);
}

void TrackWritePrepareDataBlobs(const NEvWrite::TWriteMeta& meta, TDuration duration, ui64 blobBytes) {
    if (const auto& orbit = meta.GetOrbit()) {
        LWTRACK(WritePrepareDataBlobs, *orbit, PathIdOf(meta), meta.GetTabletId(), meta.GetTxId(), meta.GetCookie(), duration, blobBytes);
    }
}

void TrackWritePrepareIndexBlobs(const NEvWrite::TWriteMeta& meta, TDuration duration, ui64 blobBytes) {
    if (const auto& orbit = meta.GetOrbit()) {
        LWTRACK(WritePrepareIndexBlobs, *orbit, PathIdOf(meta), meta.GetTabletId(), meta.GetTxId(), meta.GetCookie(), duration, blobBytes);
    }
}

void TrackWritePrepareBlobs(
    const NEvWrite::TWriteMeta& meta, TDuration dataDuration, ui64 dataBytes, TDuration indexDuration, ui64 indexBytes) {
    TrackWritePrepareDataBlobs(meta, dataDuration, dataBytes);
    TrackWritePrepareIndexBlobs(meta, indexDuration, indexBytes);
}

void TrackWriteToBlobStorageStart(const NEvWrite::TWriteMeta& meta, ui64 blobBytes) {
    if (const auto& orbit = meta.GetOrbit()) {
        LWTRACK(WriteToBlobStorageStart, *orbit, PathIdOf(meta), meta.GetTabletId(), meta.GetTxId(), meta.GetCookie(), blobBytes);
    }
}

void TrackWriteToBlobStorage(const NEvWrite::TWriteMeta& meta, TDuration duration, ui64 blobBytes, bool success) {
    if (const auto& orbit = meta.GetOrbit()) {
        LWTRACK(WriteToBlobStorage, *orbit, PathIdOf(meta), meta.GetTabletId(), meta.GetTxId(), meta.GetCookie(), duration, blobBytes, success);
    }
}

void TrackWriteFinished(NLWTrace::TOrbit& orbit, ui64 pathId, ui64 tabletId, ui64 txId, ui64 cookie, const TString& sender, const TString& type,
    TDuration totalDuration, TDuration transactionTime, TDuration completeTime, TDuration txTotalTime, TDuration writeTime) {
    LWTRACK(WriteFinished, orbit, pathId, tabletId, txId, cookie, sender, type, totalDuration, transactionTime, completeTime, txTotalTime,
        writeTime);
}

void TrackWriteFinished(const NEvWrite::TWriteMeta& meta, const TString& type, TDuration transactionTime, TDuration completeTime,
    TDuration txTotalTime) {
    if (const auto& orbit = meta.GetOrbit()) {
        const auto now = TMonotonic::Now();
        TrackWriteFinished(*orbit, PathIdOf(meta), meta.GetTabletId(), meta.GetTxId(), meta.GetCookie(), meta.GetSource().ToString(), type,
            now - meta.GetOrbitStartInstant(), transactionTime, completeTime, txTotalTime, now - meta.GetWriteStartInstant());
    }
}

void TrackWriteFailed(NLWTrace::TOrbit& orbit, ui64 pathId, ui64 tabletId, ui64 txId, ui64 cookie, const TString& sender, const TString& type,
    const TString& status, const TString& reason) {
    LWTRACK(WriteFailed, orbit, pathId, tabletId, txId, cookie, sender, type, status, reason);
}

void TrackWriteFailed(const NEvWrite::TWriteMeta& meta, const TString& type, const TString& status, const TString& reason) {
    if (const auto& orbit = meta.GetOrbit()) {
        TrackWriteFailed(*orbit, PathIdOf(meta), meta.GetTabletId(), meta.GetTxId(), meta.GetCookie(), meta.GetSource().ToString(), type,
            status, reason);
    }
}

void TrackCommitWriteResult(ui64 tabletId, ui64 txId, ui64 cookie, const TString& sender, bool success, const TString& type,
    const TString& status, const TString& reason) {
    NLWTrace::TOrbit orbit;
    TrackStartWrite(orbit, 0, tabletId, txId, cookie, sender, TDuration::Max(), 0, "CommitWriteLock", true);
    if (success) {
        TrackWriteFinished(orbit, 0, tabletId, txId, cookie, sender, type, TDuration::Zero());
    } else {
        TrackWriteFailed(orbit, 0, tabletId, txId, cookie, sender, type, status, reason);
    }
}

}   // namespace NKikimr::NColumnShard
