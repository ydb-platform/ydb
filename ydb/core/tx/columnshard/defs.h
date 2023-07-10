#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/tx/ctor_logger.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/writer/put_status.h>

namespace NKikimr::NColumnShard {

using TLogThis = TCtorLogger<NKikimrServices::TX_COLUMNSHARD>;

struct TLimits {
    static constexpr const ui32 MIN_SMALL_BLOBS_TO_INSERT = 200;
    static constexpr const ui32 MIN_BYTES_TO_INSERT = 4 * 1024 * 1024;
    static constexpr const ui64 MAX_BYTES_TO_INSERT = 16 * 1024 * 1024;
    static constexpr const ui32 MAX_TX_RECORDS = 100000;
    static constexpr const ui64 MAX_BLOBS_TO_DELETE = NOlap::TCompactionLimits::MAX_BLOBS_TO_DELETE;

    static ui64 GetBlobSizeLimit();
    static ui64 GetMaxBlobSize();
    static void SetMaxBlobSize(const ui64 value);

    class TMaxBlobSizeGuard: TNonCopyable {
    private:
        const ui64 Size;
        const ui64 Original;
    public:
        TMaxBlobSizeGuard(const ui64 value)
            : Size(value)
            , Original(GetBlobSizeLimit())
        {
            SetMaxBlobSize(Size);
        }

        ~TMaxBlobSizeGuard() {
            SetMaxBlobSize(Original);
        }
    };

    static TMaxBlobSizeGuard MaxBlobSizeGuard(const ui64 value) {
        return TMaxBlobSizeGuard(value);
    }

    TControlWrapper MinInsertBytes;
    TControlWrapper MaxInsertBytes;
    TControlWrapper InsertTableSize;

    TLimits();

    void RegisterControls(TControlBoard& icb) {
        icb.RegisterSharedControl(MinInsertBytes, "ColumnShardControls.MinBytesToIndex");
        icb.RegisterSharedControl(MaxInsertBytes, "ColumnShardControls.MaxBytesToIndex");
        icb.RegisterSharedControl(InsertTableSize, "ColumnShardControls.InsertTableCommittedSize");
    }
};

struct TCompactionLimits {
    using TBase = NOlap::TCompactionLimits;

    TControlWrapper GoodBlobSize;
    ui32 GranuleBlobSplitSize;
    TControlWrapper GranuleExpectedSize;
    TControlWrapper GranuleOverloadSize;
    TControlWrapper InGranuleCompactInserts; // Trigger in-granule compaction to reduce count of portions' records
    TControlWrapper InGranuleCompactSeconds; // Trigger in-granule comcation to guarantee no PK intersections

    TCompactionLimits()
        : GoodBlobSize(TBase::MIN_GOOD_BLOB_SIZE, TBase::MIN_GOOD_BLOB_SIZE, TBase::MAX_BLOB_SIZE)
        , GranuleBlobSplitSize(TBase::MAX_BLOB_SIZE)
        , GranuleExpectedSize(5 * TBase::MAX_BLOB_SIZE, TBase::MAX_BLOB_SIZE, 100 * TBase::MAX_BLOB_SIZE)
        , GranuleOverloadSize(20 * TBase::MAX_BLOB_SIZE, TBase::MAX_BLOB_SIZE, 100 * TBase::MAX_BLOB_SIZE)
        , InGranuleCompactInserts(100, 10, 1000)
        , InGranuleCompactSeconds(2 * 60, 10, 3600)
    {}

    void RegisterControls(TControlBoard& icb) {
        icb.RegisterSharedControl(GoodBlobSize, "ColumnShardControls.IndexGoodBlobSize");
        icb.RegisterSharedControl(GranuleExpectedSize, "ColumnShardControls.GranuleTargetBytes");
        icb.RegisterSharedControl(GranuleOverloadSize, "ColumnShardControls.GranuleOverloadBytes");
        icb.RegisterSharedControl(InGranuleCompactInserts, "ColumnShardControls.MaxPortionsInGranule");
        icb.RegisterSharedControl(InGranuleCompactSeconds, "ColumnShardControls.CompactionDelaySec");
    }

    NOlap::TCompactionLimits Get() const {
        return NOlap::TCompactionLimits{
            .GoodBlobSize = (ui32)GoodBlobSize,
            .GranuleBlobSplitSize = GranuleBlobSplitSize,
            .GranuleExpectedSize = (ui32)GranuleExpectedSize,
            .GranuleOverloadSize = (ui32)GranuleOverloadSize,
            .InGranuleCompactInserts = (ui32)InGranuleCompactInserts,
            .InGranuleCompactSeconds = (ui32)InGranuleCompactSeconds
        };
    }
};

struct TUsage {
    ui64 CPUExecTime{};
    ui64 Network{};
    ui64 SourceMemorySize{};

    void Add(const TUsage& other) {
        CPUExecTime += other.CPUExecTime;
        Network += other.Network;
        SourceMemorySize += other.SourceMemorySize;
    }
};

class TCpuGuard {
public:
    TCpuGuard(TUsage& usage)
        : Usage(usage)
    {}

    ~TCpuGuard() {
        Usage.CPUExecTime = 1000000 * CpuTimer.PassedReset();
    }

private:
    TUsage& Usage;
    THPTimer CpuTimer;
};


// A helper to resolve DS groups where a tablet's blob ids
class TBlobGroupSelector : public NOlap::IBlobGroupSelector {
private:
    TIntrusiveConstPtr<TTabletStorageInfo> TabletInfo;

public:
    explicit TBlobGroupSelector(TIntrusiveConstPtr<TTabletStorageInfo> tabletInfo)
        : TabletInfo(tabletInfo)
    {}

    ui32 GetGroup(const TLogoBlobID& blobId) const override {
        return TabletInfo->GroupFor(blobId.Channel(), blobId.Generation());
    }
};

}
