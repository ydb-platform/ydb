#include "datashard_impl.h"
#include "range_ops.h"
#include "scan_common.h"
#include "upload_stats.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/kqp/common/kqp_types.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/core/ydb_convert/ydb_convert.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NDataShard {
using TTypes = TVector<std::pair<TString, Ydb::Type>>;

#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)

void ProtoYdbTypeFromTypeInfo(Ydb::Type* type, const NScheme::TTypeInfo typeInfo);

std::shared_ptr<TTypes> BuildTypes(const TUserTable& tableInfo, const NKikimrIndexBuilder::TColumnBuildSettings& buildSettings);

std::shared_ptr<TTypes> BuildTypes(const TUserTable& tableInfo, TProtoColumnsCRef indexColumns, TProtoColumnsCRef dataColumns);

std::shared_ptr<TTypes> BuildTypes(const TUserTable& tableInfo, const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings);

bool CheckNotNullConstraint(const TConstArrayRef<TCell>& cells);

bool BuildExtraColumns(TVector<TCell>& cells, const NKikimrIndexBuilder::TColumnBuildSettings& buildSettings, TString& err, TMemoryPool& valueDataPool);

struct TStatus {
    Ydb::StatusIds::StatusCode StatusCode = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    NYql::TIssues Issues;

    bool IsNone() const {
        return StatusCode == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    }

    bool IsSuccess() const {
        return StatusCode == Ydb::StatusIds::SUCCESS;
    }

    bool IsRetriable() const {
        return StatusCode == Ydb::StatusIds::UNAVAILABLE || StatusCode == Ydb::StatusIds::OVERLOADED;
    }

    TString ToString() const {
        return TStringBuilder()
               << "Status {"
               << " Code: " << Ydb::StatusIds_StatusCode_Name(StatusCode)
               << " Issues: " << Issues.ToString()
               << " }";
    }
};

struct TUploadLimits {
    ui64 BatchRowsLimit = 500;
    ui64 BatchBytesLimit = 1u << 23; // 8MB
    ui32 MaxUploadRowsRetryCount = 50;
    ui32 BackoffCeiling = 3;

    TDuration GetTimeoutBackouff(ui32 retryNo) const {
        return TDuration::Seconds(1u << Max(retryNo, BackoffCeiling));
    }
};

class TBufferData: public IStatHolder, public TNonCopyable {
public:
    TBufferData()
        : Rows(new TRows)
    {
    }

    ui64 GetRows() const override final {
        return Rows->size();
    }

    std::shared_ptr<TRows> GetRowsData() const {
        return Rows;
    }

    ui64 GetBytes() const override final {
        return ByteSize;
    }

    void FlushTo(TBufferData& other) {
        if (this == &other) {
            return;
        }

        Y_ABORT_UNLESS(other.Rows);
        Y_ABORT_UNLESS(other.IsEmpty());

        other.Rows.swap(Rows);
        other.ByteSize = ByteSize;
        other.LastKey = std::move(LastKey);

        Clear();
    }

    void Clear() {
        Rows->clear();
        ByteSize = 0;
        LastKey = {};
    }

    void AddRow(TSerializedCellVec&& key, TSerializedCellVec&& targetPk, TString&& targetValue) {
        Rows->emplace_back(std::move(targetPk), std::move(targetValue));
        ByteSize += Rows->back().first.GetBuffer().size() + Rows->back().second.size();
        LastKey = std::move(key);
    }

    bool IsEmpty() const {
        return Rows->empty();
    }

    bool IsReachLimits(const TUploadLimits& Limits) {
        return Rows->size() >= Limits.BatchRowsLimit || ByteSize > Limits.BatchBytesLimit;
    }

    void ExtractLastKey(TSerializedCellVec& out) {
        out = std::move(LastKey);
    }

    const TSerializedCellVec& GetLastKey() const {
        return LastKey;
    }

private:
    std::shared_ptr<TRows> Rows;
    ui64 ByteSize = 0;
    TSerializedCellVec LastKey;
};


class TBuildScanUpload: public TActor<TBuildScanUpload>, public NTable::IScan {
protected:
    const TUploadLimits Limits;

    const ui64 BuildIndexId;
    const TString TargetTable;
    const TScanRecord::TSeqNo SeqNo;

    const ui64 DataShardId;
    const TActorId ProgressActorId;

    TTags ScanTags;                             // first: columns we scan, order as in IndexTable
    std::shared_ptr<TTypes> UploadColumnsTypes; // columns types we upload to indexTable
    NTxProxy::EUploadRowsMode UploadMode;

    const TTags KeyColumnIds;
    const TVector<NScheme::TTypeInfo> KeyTypes;

    const TSerializedTableRange TableRange;
    const TSerializedTableRange RequestedRange;

    IDriver* Driver = nullptr;

    TBufferData ReadBuf;
    TBufferData WriteBuf;
    TSerializedCellVec LastUploadedKey;

    TActorId Uploader;
    ui64 RetryCount = 0;

    TUploadMonStats Stats = TUploadMonStats("tablets", "build_index_upload");
    TStatus UploadStatus;

    enum class ECheckingNotNullStatus {
        None,
        Ok,
        NullFound
    } CheckingNotNullStatus = ECheckingNotNullStatus::None;

    TBuildScanUpload(ui64 buildIndexId,
                     const TString& target,
                     const TScanRecord::TSeqNo& seqNo,
                     ui64 dataShardId,
                     const TActorId& progressActorId,
                     const TSerializedTableRange& range,
                     const TUserTable& tableInfo,
                     TUploadLimits limits);

    template <typename TAddRow>
    EScan FeedImpl(TArrayRef<const TCell> key, const TRow& row, TAddRow&& addRow) noexcept;
public:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                LOG_E("TBuildIndexScan: StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString());
        }
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType();

    ~TBuildScanUpload() override = default;

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept override;

    EScan Seek(TLead& lead, ui64 seq) noexcept override;

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override;

    void UploadStatusToMessage(NKikimrTxDataShard::TEvBuildIndexProgressResponse& msg);

    void Describe(IOutputStream& out) const noexcept override;

    TString Debug() const;

    EScan PageFault() noexcept override;

private:
    void HandleWakeup(const NActors::TActorContext& ctx);

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx);

    void RetryUpload();

    void Upload(bool isRetry = false);
};

class TBuildIndexScan final: public TBuildScanUpload {
    const ui32 TargetDataColumnPos; // positon of first data column in target table

public:
    TBuildIndexScan(ui64 buildIndexId,
                    const TString& target,
                    const TScanRecord::TSeqNo& seqNo,
                    ui64 dataShardId,
                    const TActorId& progressActorId,
                    const TSerializedTableRange& range,
                    TProtoColumnsCRef targetIndexColumns,
                    TProtoColumnsCRef targetDataColumns,
                    const TUserTable& tableInfo,
                    TUploadLimits limits);

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final;
};

class TBuildColumnsScan final: public TBuildScanUpload {
    TString ValueSerialized;

public:
    TBuildColumnsScan(ui64 buildIndexId,
                      const TString& target,
                      const TScanRecord::TSeqNo& seqNo,
                      ui64 dataShardId,
                      const TActorId& progressActorId,
                      const TSerializedTableRange& range,
                      const TUserTable& tableInfo,
                      TUploadLimits limits,
                      const NKikimrIndexBuilder::TColumnBuildSettings& columnBuildSettings
    );

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final;
};

class TCheckColumnScan final: public TBuildScanUpload {
public:
    TCheckColumnScan(ui64 buildIndexId,
                     const TString& target,
                     const TScanRecord::TSeqNo& seqNo,
                     ui64 dataShardId,
                     const TActorId& progressActorId,
                     const TSerializedTableRange& range,
                     const TUserTable& tableInfo,
                     TUploadLimits limits,
                     const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings
    );

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final;
};

TAutoPtr<NTable::IScan> CreateBuildIndexScan (
    ui64 buildIndexId,
    TString target,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TSerializedTableRange& range,
    TProtoColumnsCRef targetIndexColumns,
    TProtoColumnsCRef targetDataColumns,
    const NKikimrIndexBuilder::TColumnBuildSettings& columnsToBuild,
    const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings,
    const TUserTable& tableInfo,
    TUploadLimits limits
);
} // NKikimr::NDataShard