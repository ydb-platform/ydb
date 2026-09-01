#pragma once

#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NColumnShard {

void TrackStartWrite(NLWTrace::TOrbit& orbit, ui64 pathId, ui64 tabletId, ui64 txId, ui64 cookie, const TString& sender,
    TDuration writeTimeout, ui64 size, const TString& modificationType, bool isBulk);

void TrackWritePrepareDataBlobs(const NEvWrite::TWriteMeta& meta, TDuration duration, ui64 blobBytes);
void TrackWritePrepareIndexBlobs(const NEvWrite::TWriteMeta& meta, TDuration duration, ui64 blobBytes);
void TrackWritePrepareBlobs(const NEvWrite::TWriteMeta& meta, TDuration dataDuration, ui64 dataBytes, TDuration indexDuration, ui64 indexBytes);
void TrackWriteToBlobStorageStart(const NEvWrite::TWriteMeta& meta, ui64 blobBytes);
void TrackWriteToBlobStorage(const NEvWrite::TWriteMeta& meta, TDuration duration, ui64 blobBytes, bool success);

void TrackWriteFinished(NLWTrace::TOrbit& orbit, ui64 pathId, ui64 tabletId, ui64 txId, ui64 cookie, const TString& sender, const TString& type,
    TDuration totalDuration, TDuration transactionTime = TDuration::Zero(), TDuration completeTime = TDuration::Zero(),
    TDuration txTotalTime = TDuration::Zero(), TDuration writeTime = TDuration::Zero());
void TrackWriteFinished(const NEvWrite::TWriteMeta& meta, const TString& type, TDuration transactionTime = TDuration::Zero(),
    TDuration completeTime = TDuration::Zero(), TDuration txTotalTime = TDuration::Zero());

void TrackWriteFailed(NLWTrace::TOrbit& orbit, ui64 pathId, ui64 tabletId, ui64 txId, ui64 cookie, const TString& sender, const TString& type,
    const TString& status, const TString& reason);
void TrackWriteFailed(const NEvWrite::TWriteMeta& meta, const TString& type, const TString& status, const TString& reason);

void TrackCommitWriteResult(ui64 tabletId, ui64 txId, ui64 cookie, const TString& sender, bool success, const TString& type,
    const TString& status, const TString& reason);

}   // namespace NKikimr::NColumnShard
