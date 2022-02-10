#pragma once

#include "blob_manager.h"

#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/tx_processing.h>

namespace arrow {
    class Schema;
}

namespace NKikimr::NColumnShard {

struct TEvPrivate {
    enum EEv {
        EvIndexing = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvWriteIndex,
        EvScanStats,
        EvReadFinished,
        EvPeriodicWakeup,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    /// Common event for Indexing and GranuleCompaction: write index data in TTxWriteIndex transaction.
    struct TEvWriteIndex : public TEventLocal<TEvWriteIndex, EvWriteIndex> {
        NKikimrProto::EReplyStatus PutStatus = NKikimrProto::UNKNOWN;
        NOlap::TIndexInfo IndexInfo;
        std::shared_ptr<NOlap::TColumnEngineChanges> IndexChanges;
        TVector<TString> Blobs;
        bool GranuleCompaction{false};
        TBlobBatch BlobBatch;
        TUsage ResourceUsage;
        TVector<ui32> YellowMoveChannels; 
        TVector<ui32> YellowStopChannels; 
        bool CacheData{false};

        TEvWriteIndex(const NOlap::TIndexInfo& indexInfo,
            std::shared_ptr<NOlap::TColumnEngineChanges> indexChanges,
            bool cacheData)
            : IndexInfo(indexInfo)
            , IndexChanges(indexChanges)
            , CacheData(cacheData)
        {}
    };

    struct TEvIndexing : public TEventLocal<TEvIndexing, EvIndexing> {
        std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;

        explicit TEvIndexing(std::unique_ptr<TEvPrivate::TEvWriteIndex> txEvent)
            : TxEvent(std::move(txEvent))
        {}
    };

    struct TEvCompaction : public TEventLocal<TEvCompaction, EvIndexing> {
        std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;

        explicit TEvCompaction(std::unique_ptr<TEvPrivate::TEvWriteIndex> txEvent)
            : TxEvent(std::move(txEvent))
        {
            TxEvent->GranuleCompaction = true;
        }
    };

    struct TEvScanStats : public TEventLocal<TEvScanStats, EvScanStats> {
        TEvScanStats(ui64 rows, ui64 bytes) : Rows(rows), Bytes(bytes) {}
        ui64 Rows;
        ui64 Bytes;
    };

    struct TEvReadFinished : public TEventLocal<TEvReadFinished, EvReadFinished> {
        explicit TEvReadFinished(ui64 requestCookie, ui64 txId = 0)
            : RequestCookie(requestCookie), TxId(txId)
        {}

        ui64 RequestCookie;
        ui64 TxId;
    };

    struct TEvPeriodicWakeup : public TEventLocal<TEvPeriodicWakeup, EvPeriodicWakeup> {
        TEvPeriodicWakeup(bool manual = false)
            : Manual(manual)
        {}

        bool Manual;
    };
};

using ITransaction = NTabletFlatExecutor::ITransaction;

template <typename T>
using TTransactionBase = NTabletFlatExecutor::TTransactionBase<T>;

class TColumnShard;

/// Load data from local database
class TTxInit : public TTransactionBase<TColumnShard> {
public:
    TTxInit(TColumnShard* self)
        : TBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_INIT; }

    void SetDefaults();
    bool ReadEverything(TTransactionContext& txc, const TActorContext& ctx);
};

/// Create local database on tablet start if none
class TTxInitSchema : public TTransactionBase<TColumnShard> {
public:
    TTxInitSchema(TColumnShard* self)
        : TBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }
};

/// Update local database on tablet start
class TTxUpdateSchema : public TTransactionBase<TColumnShard> {
public:
    TTxUpdateSchema(TColumnShard* self)
        : TBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_UPDATE_SCHEMA; }
};

/// Propose deterministic tx
class TTxProposeTransaction : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxProposeTransaction(TColumnShard* self, TEvColumnShard::TEvProposeTransaction::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_PROPOSE; }

private:
    TEvColumnShard::TEvProposeTransaction::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvProposeTransactionResult> Result;
};

/// Plan deterministic txs
class TTxPlanStep : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxPlanStep(TColumnShard* self, TEvTxProcessing::TEvPlanStep::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_PLANSTEP; }

private:
    TEvTxProcessing::TEvPlanStep::TPtr Ev;
    THashMap<TActorId, TVector<ui64>> TxAcks;
    std::unique_ptr<TEvTxProcessing::TEvPlanStepAccepted> Result;
};

/// Write portion of data in OLAP transaction
class TTxWrite : public TTransactionBase<TColumnShard> {
public:
    TTxWrite(TColumnShard* self, TEvColumnShard::TEvWrite::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_WRITE; }

private:
    TEvColumnShard::TEvWrite::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvWriteResult> Result;
};

/// Read portion of data in OLAP transaction
class TTxReadBase : public TTransactionBase<TColumnShard> {
protected:
    explicit TTxReadBase(TColumnShard* self)
        : TBase(self)
    {}

    NOlap::TReadMetadata::TPtr PrepareReadMetadata(
                                    const TActorContext& ctx,
                                    const TReadDescription& readDescription,
                                    const std::unique_ptr<NOlap::TInsertTable>& insertTable,
                                    const std::unique_ptr<NOlap::IColumnEngine>& index,
                                    TString& error) const;

protected:
    bool ParseProgram(
        const TActorContext& ctx,
        NKikimrSchemeOp::EOlapProgramType programType,
        TString serializedProgram,
        TReadDescription& read,
        const IColumnResolver& columnResolver
    );

protected:
    TString ErrorDescription;
};

class TTxRead : public TTxReadBase {
public:
    TTxRead(TColumnShard* self, TEvColumnShard::TEvRead::TPtr& ev)
        : TTxReadBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_READ; }

private:
    TEvColumnShard::TEvRead::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvReadResult> Result;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
};

class TTxScan : public TTxReadBase {
public:
    using TReadMetadataPtr = NOlap::TReadMetadataBase::TConstPtr;

    TTxScan(TColumnShard* self, TEvColumnShard::TEvScan::TPtr& ev)
        : TTxReadBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_START_SCAN; }

private:
    NOlap::TReadMetadataBase::TConstPtr CreateReadMetadata(const TActorContext& ctx, TReadDescription& read,
        bool isIndexStats, bool isReverse, ui64 limit);

private:
    TEvColumnShard::TEvScan::TPtr Ev;
    TVector<TReadMetadataPtr> ReadMetadataRanges;
};


class TTxReadBlobRanges : public TTransactionBase<TColumnShard> {
public:
    TTxReadBlobRanges(TColumnShard* self, TEvColumnShard::TEvReadBlobRanges::TPtr& ev)
        : TTransactionBase<TColumnShard>(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_READ_BLOB_RANGES; }

private:
    TEvColumnShard::TEvReadBlobRanges::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvReadBlobRangesResult> Result;
};


/// Common transaction for WriteIndex and GranuleCompaction.
/// For WriteIndex it writes new portion from InsertTable into index.
/// For GranuleCompaction it writes new portion of indexed data and mark old data with "switching" snapshot.
class TTxWriteIndex : public TTransactionBase<TColumnShard> {
public:
    TTxWriteIndex(TColumnShard* self, TEvPrivate::TEvWriteIndex::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_WRITE_INDEX; }

private:
    TEvPrivate::TEvWriteIndex::TPtr Ev;
};

}
