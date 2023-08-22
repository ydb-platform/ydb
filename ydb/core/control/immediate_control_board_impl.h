#pragma once

#include "defs.h"
#include "immediate_control_board_wrapper.h"

#include <yt/yt/core/misc/atomic_ptr.h>
#include <ydb/core/util/concurrent_rw_hash.h>

namespace NKikimr {

struct TAtomicControlBoard final : public TThrRefBase {
	TControlWrapper DisableByKeyFilter;
	TControlWrapper MaxTxInFly;
	TControlWrapper MaxTxLagMilliseconds;
	TControlWrapper CanCancelROWithReadSets;
	TControlWrapper PerShardReadSizeLimit;
	TControlWrapper CpuUsageReportThreshlodPercent;
	TControlWrapper CpuUsageReportIntervalSeconds;
	TControlWrapper HighDataSizeReportThreshlodBytes;
	TControlWrapper HighDataSizeReportIntervalSeconds;

	TControlWrapper DataTxProfileLogThresholdMs;
	TControlWrapper DataTxProfileBufferThresholdMs;
	TControlWrapper DataTxProfileBufferSize;

	TControlWrapper ReadColumnsScanEnabled;
	TControlWrapper ReadColumnsScanInUserPool;

	TControlWrapper BackupReadAheadLo;
	TControlWrapper BackupReadAheadHi;

	TControlWrapper TtlReadAheadLo;
	TControlWrapper TtlReadAheadHi;

	TControlWrapper EnablePrioritizedMvccSnapshotReads;
	TControlWrapper EnableUnprotectedMvccSnapshotReads;
	TControlWrapper EnableLockedWrites;
	TControlWrapper MaxLockedWritesPerKey;

	TControlWrapper EnableLeaderLeases;
	TControlWrapper MinLeaderLeaseDurationUs;
};

class TControlBoard : public TThrRefBase {
private:
    TConcurrentRWHashMap<TString, TIntrusivePtr<TControl>, 16> Board;

public:
    bool RegisterLocalControl(TControlWrapper control, TString name);

    bool RegisterSharedControl(TControlWrapper& control, TString name);

    void RestoreDefaults();

    void RestoreDefault(TString name);

    bool SetValue(TString name, TAtomic value, TAtomic &outPrevValue);

    // Only for tests
    void GetValue(TString name, TAtomic &outValue, bool &outIsControlExists) const;

    TString RenderAsHtml() const;
};

}
