#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;


// Conversion between table partition storage formats in the local db.
//
// TablePartitions (`position` format): shards keyed by position in the table.
// TablePartitionsByShardIdx (`shardidx` format): shards keyed by ShardIdx.
//
// Both TTxTablePartitionsFormatSwitch (single table, manual) and
// TTxTablePartitionsFormatSweepStep (all tables, background) call
// SwitchTablePartitionsFormat() to perform the conversion.
// They are kept separate to simplify logic and allow manual switching
// independently of automatic sweeping.


// TablePartitionsFormatSwitch -- single table conversion

struct TSchemeShard::TTxTablePartitionsFormatSwitch : public TTransactionBase<TSchemeShard> {
    NMon::TEvRemoteHttpInfo::TPtr Ev;
    TPathId PathId;
    TString PathStr;
    bool ShardIdxFormat = false;
    EFormatSwitchStatus Status = EFormatSwitchStatus::Ok;

    TTxTablePartitionsFormatSwitch(TSelf* self, NMon::TEvRemoteHttpInfo::TPtr ev, TPathId pathId, bool shardIdxFormat)
        : TTransactionBase<TSchemeShard>(self)
        , Ev(std::move(ev))
        , PathId(pathId)
        , ShardIdxFormat(shardIdxFormat)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_TABLE_PARTITIONS_STORAGE_FORMAT_SWITCH;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);
        PathStr = TPath::Init(PathId, Self).PathString();
        Status = Self->SwitchTablePartitionsFormat(db, PathId, ShardIdxFormat);

        YDB_LOG_NOTICE_CTX(ctx, "TTxTablePartitionsFormatSwitch: path switch to format result",
            {"pathStr", PathStr},
            {"pathId", PathId},
            {"shardIdxFormat", (ShardIdxFormat ? "shardidx" : "position")},
            {"status", Status},
            {"schemeshardId", Self->TabletID()});

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TStringStream body;
        switch (Status) {
            case EFormatSwitchStatus::Ok:
                body << "OK\n"
                     << PathStr << "\n"
                     << PathId << "\n"
                     << "Switched to format '" << (ShardIdxFormat ? "shardidx" : "position") << "'.";
                break;
            case EFormatSwitchStatus::AlreadyDone:
                body << "ALREADY_DONE\n"
                     << PathStr << "\n"
                     << PathId << "\n"
                     << "Already in format '" << (ShardIdxFormat ? "shardidx" : "position") << "'.";
                break;
            case EFormatSwitchStatus::Busy:
                body << "BUSY\n"
                     << PathStr << "\n"
                     << PathId << "\n"
                     << "Under another operation. Retry later.";
                break;
            case EFormatSwitchStatus::NotATable:
                body << "NOT_A_TABLE\n"
                     << PathStr << "\n"
                     << PathId << "\n"
                     << "Not a table.";
                break;
        }
        ctx.Send(Ev->Sender, new NMon::TEvRemoteBinaryInfoRes(
            TStringBuilder()
                << "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n"
                << body.Str()
                << "\n"
        ));
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxTablePartitionsFormatSwitch(
    NMon::TEvRemoteHttpInfo::TPtr ev, TPathId pathId, bool shardIdxFormat
) {
    return new TTxTablePartitionsFormatSwitch(this, std::move(ev), pathId, shardIdxFormat);
}


// TablePartitionsFormatSweepStep -- conversion sweep over all tables

struct TSchemeShard::TTxTablePartitionsFormatSweepStep : public TTransactionBase<TSchemeShard> {
    TDuration DelayNext;

    TTxTablePartitionsFormatSweepStep(TSelf* self)
        : TTransactionBase<TSchemeShard>(self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_TABLE_PARTITIONS_STORAGE_FORMAT_SWEEP_STEP;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& sweep = Self->TablePartitionsFormatSweep;

        if (sweep.Status != TTablePartitionsFormatSweepState::EStatus::Running) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        // Self-cancel if shardidx format support is disabled during the sweep.
        if (!AppData()->FeatureFlags.GetEnableTablePartitionsFormatShardIdx()) {
            YDB_LOG_NOTICE_CTX(ctx, "TablePartitionsFormatSweep step: flag EnableTablePartitionsFormatShardIdx turned off during sweep, cancelling");
            Self->CancelTablePartitionsFormatSweep(db);
            return true;
        }

        if (sweep.Queue.empty()) {
            Self->ClearTablePartitionsFormatSweep(db);
            YDB_LOG_NOTICE_CTX(ctx, "TablePartitionsFormatSweep complete: done skipped",
                {"#_sweep.Done", sweep.Done},
                {"#_sweep.Skipped", sweep.Skipped},
                {"schemeshardId", Self->TabletID()});
            return true;
        }

        const TPathId pathId = sweep.Queue.front();
        sweep.Queue.pop_front();

        // Cap overall time fraction taken by format switching to 10%.
        // That is a throughput guarantee, not a latency guarantee.
        const auto timeStart = ctx.Monotonic();
        const auto status = Self->SwitchTablePartitionsFormat(db, pathId, sweep.TargetIsShardIdx);
        DelayNext = Max(TDuration::MilliSeconds(100), (ctx.Monotonic() - timeStart) * 9);

        switch (status) {
            case EFormatSwitchStatus::Ok:
                ++sweep.Done;
                break;
            case EFormatSwitchStatus::AlreadyDone:
                ++sweep.Skipped;
                break;
            case EFormatSwitchStatus::Busy:
                sweep.Queue.push_back(pathId);
                break;
            case EFormatSwitchStatus::NotATable:
                break;
        }

        const auto pathStr = TPath::Init(pathId, Self).PathString();
        YDB_LOG_NOTICE_CTX(ctx, "TablePartitionsFormatSweep step: path switch to format result",
            {"pathStr", pathStr},
            {"pathId", pathId},
            {"targetIsShardIdx", (sweep.TargetIsShardIdx ? "shardidx" : "position")},
            {"status", status},
            {"schemeshardId", Self->TabletID()});

        if (sweep.Queue.empty()) {
            Self->ClearTablePartitionsFormatSweep(db);
            YDB_LOG_NOTICE_CTX(ctx, "TablePartitionsFormatSweep complete: done skipped",
                {"#_sweep.Done", sweep.Done},
                {"#_sweep.Skipped", sweep.Skipped},
                {"schemeshardId", Self->TabletID()});
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto& sweep = Self->TablePartitionsFormatSweep;

        if (sweep.Status == TTablePartitionsFormatSweepState::EStatus::Running) {
            ctx.Schedule(DelayNext, new TEvPrivate::TEvProgressTablePartitionsFormatSweep);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxTablePartitionsFormatSweepStep() {
    return new TTxTablePartitionsFormatSweepStep(this);
}


// TSchemeShard methods

void TSchemeShard::Handle(TEvPrivate::TEvProgressTablePartitionsFormatSweep::TPtr&, const TActorContext& ctx) {
    Execute(CreateTxTablePartitionsFormatSweepStep(), ctx);
}

void TSchemeShard::StartTablePartitionsFormatSweep(NIceDb::TNiceDb& db, bool targetIsShardIdx) {
    auto& sweep = TablePartitionsFormatSweep;

    if (!AppData()->FeatureFlags.GetEnableTablePartitionsFormatShardIdx()) {
        YDB_LOG_ERROR("TablePartitionsFormatSweep start: cannot start as EnableTablePartitionsFormatShardIdx is disabled",
            {"schemeshardId", TabletID()});
        return;
    }

    sweep.Status = TTablePartitionsFormatSweepState::EStatus::Running;
    sweep.TargetIsShardIdx = targetIsShardIdx;
    sweep.Done = 0;
    sweep.Skipped = 0;
    sweep.Queue.clear();

    for (const auto& [pathId, tableInfoPtr] : Tables) {
        if (tableInfoPtr->PartitionsInShardIdxFormat != sweep.TargetIsShardIdx) {
            sweep.Queue.push_back(pathId);
        }
    }

    if (sweep.Queue.empty()) {
        YDB_LOG_NOTICE("TablePartitionsFormatSweep start: no tables to process",
            {"schemeshardId", TabletID()});
        sweep.Status = TTablePartitionsFormatSweepState::EStatus::Idle;
        sweep.Queue.clear();
        return;
    }

    YDB_LOG_NOTICE("TablePartitionsFormatSweep start: tables to process tables total switch to",
        {"#_sweep.Queue.size", sweep.Queue.size()},
        {"#_Tables.size", Tables.size()},
        {"#targetIsShardIdx", (sweep.TargetIsShardIdx ? "shardidx" : "position")},
        {"schemeshardId", TabletID()});

    db.Table<Schema::SysParams>().Key(Schema::SysParam_TablePartitionsFormatSweepStatus)
        .Update(NIceDb::TUpdate<Schema::SysParams::Value>("1"))
    ;
    db.Table<Schema::SysParams>().Key(Schema::SysParam_TablePartitionsFormatSweepTarget)
        .Update(NIceDb::TUpdate<Schema::SysParams::Value>(targetIsShardIdx ? "1" : "0"))
    ;

    Send(SelfId(), new TEvPrivate::TEvProgressTablePartitionsFormatSweep);
}

void TSchemeShard::PauseTablePartitionsFormatSweep(NIceDb::TNiceDb& db) {
    auto& sweep = TablePartitionsFormatSweep;

    YDB_LOG_NOTICE("TablePartitionsFormatSweep pause: tables to process tables total switch to",
        {"#_sweep.Queue.size", sweep.Queue.size()},
        {"#_Tables.size", Tables.size()},
        {"#targetIsShardIdx", (sweep.TargetIsShardIdx ? "shardidx" : "position")},
        {"schemeshardId", TabletID()});

    sweep.Status = TTablePartitionsFormatSweepState::EStatus::Paused;

    db.Table<Schema::SysParams>().Key(Schema::SysParam_TablePartitionsFormatSweepStatus)
        .Update(NIceDb::TUpdate<Schema::SysParams::Value>("2"))
    ;
}

void TSchemeShard::ResumeTablePartitionsFormatSweep(NIceDb::TNiceDb& db) {
    auto& sweep = TablePartitionsFormatSweep;

    YDB_LOG_NOTICE("TablePartitionsFormatSweep resume: tables to process tables total switch to",
        {"#_sweep.Queue.size", sweep.Queue.size()},
        {"#_Tables.size", Tables.size()},
        {"#targetIsShardIdx", (sweep.TargetIsShardIdx ? "shardidx" : "position")},
        {"schemeshardId", TabletID()});

    sweep.Status = TTablePartitionsFormatSweepState::EStatus::Running;

    db.Table<Schema::SysParams>().Key(Schema::SysParam_TablePartitionsFormatSweepStatus)
        .Update(NIceDb::TUpdate<Schema::SysParams::Value>("1"))
    ;

    Send(SelfId(), new TEvPrivate::TEvProgressTablePartitionsFormatSweep);
}

void TSchemeShard::CancelTablePartitionsFormatSweep(NIceDb::TNiceDb& db) {
    auto& sweep = TablePartitionsFormatSweep;

    YDB_LOG_NOTICE("TablePartitionsFormatSweep cancel: tables to process tables total switch to",
        {"#_sweep.Queue.size", sweep.Queue.size()},
        {"#_Tables.size", Tables.size()},
        {"#targetIsShardIdx", (sweep.TargetIsShardIdx ? "shardidx" : "position")},
        {"schemeshardId", TabletID()});

    ClearTablePartitionsFormatSweep(db);
}

void TSchemeShard::ClearTablePartitionsFormatSweep(NIceDb::TNiceDb& db) {
    auto& sweep = TablePartitionsFormatSweep;

    sweep.Status = TTablePartitionsFormatSweepState::EStatus::Idle;
    sweep.Queue.clear();

    db.Table<Schema::SysParams>().Key(Schema::SysParam_TablePartitionsFormatSweepStatus).Delete();
    db.Table<Schema::SysParams>().Key(Schema::SysParam_TablePartitionsFormatSweepTarget).Delete();
}

// InitializeTablePartitionsFormatSweep -- (re)init sweep state on reboot.

void TSchemeShard::InitializeTablePartitionsFormatSweep() {
    auto& sweep = TablePartitionsFormatSweep;

    if (sweep.Status != TTablePartitionsFormatSweepState::EStatus::Idle) {
        // sweep running or paused
        ContinueTablePartitionsFormatSweep();

    } else if (AppData()->FeatureFlags.GetEnableTablePartitionsFormatShardIdx()
        && AppData()->FeatureFlags.GetEnableTablePartitionsFormatAutoConvert()
        && (TabletCounters->Simple()[COUNTER_FORMAT_POSITION_TABLE_COUNT].Get() > 0)
    ) {
        // sweep on idle but should be started
        Execute(CreateTxTablePartitionsFormatSweepAutoStart());
    }
}

// ContinueTablePartitionsFormatSweep -- continue running sweep on reboot.

void TSchemeShard::ContinueTablePartitionsFormatSweep() {
    auto& sweep = TablePartitionsFormatSweep;

    if (sweep.Status == TTablePartitionsFormatSweepState::EStatus::Idle) {
        return;
    }

    // Rebuild queue after restart: walk all tables, enqueue those not yet converted.
    sweep.Queue.clear();
    for (const auto& [pathId, tableInfoPtr] : Tables) {
        if (tableInfoPtr->PartitionsInShardIdxFormat != sweep.TargetIsShardIdx) {
            sweep.Queue.push_back(pathId);
        }
    }

    if (sweep.Status == TTablePartitionsFormatSweepState::EStatus::Paused) {
        return;
    }

    YDB_LOG_NOTICE("TablePartitionsFormatSweep continue: tables to process tables total switch to",
        {"#_sweep.Queue.size", sweep.Queue.size()},
        {"#_Tables.size", Tables.size()},
        {"#targetIsShardIdx", (sweep.TargetIsShardIdx ? "shardidx" : "position")},
        {"schemeshardId", TabletID()});

    Send(SelfId(), new TEvPrivate::TEvProgressTablePartitionsFormatSweep);
}

// TablePartitionsFormatSweepAutoStart -- auto start on reboot.
// Single way only: position -> shardidx.

struct TSchemeShard::TTxTablePartitionsFormatSweepAutoStart : public TTransactionBase<TSchemeShard> {

    TTxTablePartitionsFormatSweepAutoStart(TSelf* self)
        : TTransactionBase<TSchemeShard>(self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_TABLE_PARTITIONS_STORAGE_FORMAT_SWEEP_AUTO_START;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        auto& sweep = Self->TablePartitionsFormatSweep;

        // same check, just in case
        if (sweep.Status == TTablePartitionsFormatSweepState::EStatus::Idle
            && AppData()->FeatureFlags.GetEnableTablePartitionsFormatAutoConvert()
            && (Self->TabletCounters->Simple()[COUNTER_FORMAT_POSITION_TABLE_COUNT].Get() > 0)
        ) {
            NIceDb::TNiceDb db(txc.DB);
            Self->StartTablePartitionsFormatSweep(db, /*targetIsShardIdx*/ true);
        }

        return true;
    }

    void Complete(const TActorContext&) override {}
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxTablePartitionsFormatSweepAutoStart() {
    return new TTxTablePartitionsFormatSweepAutoStart(this);
}

}  // namespace NKikimr::NSchemeShard
