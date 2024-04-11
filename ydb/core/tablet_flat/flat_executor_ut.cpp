#include "flat_dbase_sz_env.h"
#include "flat_executor_ut_common.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TRowsModel {
        enum : ui32  {
            TableId = 101,
            ColumnKeyId = 1,
            ColumnValueId = 20,
            AltFamilyId = 1,
        };

        struct TTxSchema : public ITransaction {

            TTxSchema(TIntrusiveConstPtr<TCompactionPolicy> policy, bool groups = false)
                : Policy(std::move(policy))
                , Groups(groups)
            { }

            bool Execute(TTransactionContext &txc, const TActorContext &) override
            {
                if (txc.DB.GetScheme().GetTableInfo(TableId))
                    return true;

                txc.DB.Alter()
                    .AddTable("test" + ToString(ui32(TableId)), TableId)
                    .AddColumn(TableId, "key", ColumnKeyId, NScheme::TInt64::TypeId, false)
                    .AddColumn(TableId, "value", ColumnValueId, NScheme::TString::TypeId, false)
                    .AddColumnToKey(TableId, ColumnKeyId)
                    .SetCompactionPolicy(TableId, *Policy);

                if (Groups) {
                    txc.DB.Alter()
                        .SetFamily(TableId, AltFamilyId, NTable::NPage::ECache::None, NTable::NPage::ECodec::Plain)
                        .AddColumnToFamily(TableId, ColumnValueId, AltFamilyId);
                }

                return true;
            }

            void Complete(const TActorContext &ctx) override
            {
                ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            }

            TIntrusiveConstPtr<TCompactionPolicy> Policy;
            const bool Groups;
        };

        struct TTxAddRows : public ITransaction {

            TTxAddRows(ui64 key, ui64 rows, ui64 pack, ui32 bytes, TRowVersion writeVersion)
                : Key(key), Rows(rows), Pack(pack), Bytes(bytes), WriteVersion(writeVersion)
            {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override
            {
                for (auto end = Key + Min(Pack, Rows); Key < end; Key++, Rows--) {
                    const auto key = NScheme::TInt64::TInstance(Key);

                    TString str = Bytes ? TString(Bytes, (char)Key) : "value";
                    const auto val = NScheme::TString::TInstance(str);
                    NTable::TUpdateOp ops{ ColumnValueId, NTable::ECellOp::Set, val };

                    txc.DB.Update(TableId, NTable::ERowOp::Upsert, { key }, { ops }, WriteVersion);
                }

                return true;
            }

            void Complete(const TActorContext &ctx) override
            {
                if (Rows > 0) {
                    auto *next = new TTxAddRows(Key, Rows, Pack, Bytes, WriteVersion);

                    ctx.Send(ctx.SelfID, new NFake::TEvExecute{ next });
                } else {
                    ctx.Send(ctx.SelfID, new NFake::TEvReturn);
                }
            }

        private:
            ui64 Key = 0;
            ui64 Rows = 0;
            const ui64 Pack = 1;
            const ui32 Bytes = 0;
            const TRowVersion WriteVersion;
        };

        struct TTxEraseRows : public ITransaction {
            TTxEraseRows(ui64 key, ui64 rows, ui64 pack, TRowVersion writeVersion)
                : Key(key), Rows(rows), Pack(pack), WriteVersion(writeVersion)
            {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override
            {
                for (auto end = Key + Min(Pack, Rows); Key < end; Key++, Rows--) {
                    const auto key = NScheme::TInt64::TInstance(Key);

                    txc.DB.Update(TableId, NTable::ERowOp::Erase, { key }, { }, WriteVersion);
                }

                return true;
            }

            void Complete(const TActorContext &ctx) override
            {
                if (Rows > 0) {
                    auto *next = new TTxEraseRows(Key, Rows, Pack, WriteVersion);

                    ctx.Send(ctx.SelfID, new NFake::TEvExecute{ next });
                } else {
                    ctx.Send(ctx.SelfID, new NFake::TEvReturn);
                }
            }

        private:
            ui64 Key = 0;
            ui64 Rows = 0;
            const ui64 Pack = 1;
            const TRowVersion WriteVersion;
        };

        NFake::TEvExecute* MakeScheme(TIntrusiveConstPtr<TCompactionPolicy> policy, bool groups = false)
        {
            return new NFake::TEvExecute{ new TTxSchema(std::move(policy), groups) };
        }

        NFake::TEvExecute* MakeRows(ui32 rows, ui32 bytes = 0, ui32 pack = 1)
        {
            auto key = std::exchange(Key, Key + rows);

            return
                new NFake::TEvExecute{ new TTxAddRows(key, rows, pack, bytes, WriteVersion) };
        }

        NFake::TEvExecute* MakeErase(ui32 rows, ui32 pack = 1)
        {
            auto key = std::exchange(Key, Key + rows);

            return
                new NFake::TEvExecute{ new TTxEraseRows(key, rows, pack, WriteVersion) };
        }

        TRowsModel& RowTo(ui64 key) noexcept
        {
            return Key = key, *this;
        }

        TRowsModel& VersionTo(TRowVersion writeVersion) noexcept
        {
            WriteVersion = writeVersion;
            return *this;
        }

        ui64 Rows() const { return Key - 1; };

        ui64 Key = 1;
        TRowVersion WriteVersion = TRowVersion::Min();
    };

    class TSnapshotModel : public NFake::TDummySnapshotContext {
    public:
        static TIntrusivePtr<TSnapshotModel> Create(ui32 tableId = TRowsModel::TableId) {
            return new TSnapshotModel(tableId);
        }

        NFake::TEvExecute* Start() {
            return new NFake::TEvExecute{ new TTxPrepare(this) };
        }

        const NTable::TSubset& Result() const {
            Y_ABORT_UNLESS(Subset);
            return *Subset;
        }

    private:
        TSnapshotModel(ui32 tableId) {
            Tables.push_back(tableId);
        }

        TConstArrayRef<ui32> TablesToSnapshot() const override {
            return Tables;
        }

        NFake::TEvExecute* OnFinished() override {
            return new NFake::TEvExecute{ new TTxFinished(this) };
        }

        struct TTxPrepare : public ITransaction {
            TTxPrepare(TSnapshotModel* self)
                : Self(self)
            { }

            bool Execute(TTransactionContext &txc, const TActorContext &) override
            {
                txc.Env.MakeSnapshot(Self);
                return true;
            }

            void Complete(const TActorContext &) override
            {
                // nothing
            }

        private:
            TSnapshotModel* Self;
        };

        struct TTxFinished : public ITransaction {
            TTxFinished(TSnapshotModel* self)
                : Self(self)
            { }

            bool Execute(TTransactionContext &txc, const TActorContext &) override
            {
                const ui32 table = Self->Tables.at(0);
                Self->Subset = txc.DB.Subset(table, Self->Edge(table).Head, { }, { });
                Y_ABORT_UNLESS(Self->Subset != nullptr);
                Y_ABORT_UNLESS(Self->Subset->Frozen.empty());
                txc.Env.DropSnapshot(Self);
                return true;
            }

            void Complete(const TActorContext &ctx) override
            {
                ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            }

        private:
            TSnapshotModel* Self;
        };

    private:
        TVector<ui32> Tables;
        TAutoPtr<NTable::TSubset> Subset;
    };

struct TDummyResult: public IDestructable {
    TDummyResult(ui64 count, ui64 expect)
        : Count(count), Expect(expect)
    {}

    ui64 Count = 0;
    ui64 Expect = 0;
};

class TDummyScan : public TActor<TDummyScan>, public NTable::IScan {
public:
    TDummyScan(TActorId tablet, bool postponed, EAbort abort, ui32 rows)
        : TActor(&TThis::StateWork)
        , Tablet(tablet)
        , ExpectedRows(rows)
        , Postponed(postponed)
        , Abort(abort)
    {}
    ~TDummyScan() {}

    void Describe(IOutputStream &out) const noexcept override
    {
        out << "DummyScan";
    }

    void Handle(TEvents::TEvWakeup::TPtr &, const TActorContext &)
    {
        Driver->Touch(EScan::Feed);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, Handle);
        default:
            break;
        }
    }

private:
    TInitialState Prepare(IDriver *driver, TIntrusiveConstPtr<TScheme> scheme) noexcept override
    {
        Driver = driver;
        Scheme = std::move(scheme);

        auto ctx = TActivationContext::AsActorContext();
        ctx.RegisterWithSameMailbox(this);

        if (Postponed) {
            ctx.Send(Tablet, new NFake::TEvReturn);
            return { EScan::Sleep, { } };
        } else {
            return { EScan::Feed, { } };
        }
    }

    EScan Seek(TLead &lead, ui64 seq) noexcept override
    {
        if (seq && Abort == EAbort::None)
            return EScan::Final;

        lead.To(Scheme->Tags(), LeadKey, NTable::ESeek::Lower);
        if (LeadKey) {
            ExpectedRowId = LeadKey[0].AsValue<ui64>();
        }
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow &) noexcept override
    {
        Y_ABORT_UNLESS(key[0].AsValue<ui64>() == ExpectedRowId);
        ++ExpectedRowId;
        ++StoredRows;
        return EScan::Feed;
    }

    EScan PageFault() noexcept override {
        PageFaults++;
        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override
    {
        Y_ABORT_UNLESS((int)Abort == (int)abort);

        auto ctx = ActorContext();
        if (abort == EAbort::None) {
            if (ExpectedRows != StoredRows) {
                Cerr << "Expected " << ExpectedRows << " rows but got " << StoredRows << Endl;
            }
            Y_ABORT_UNLESS(ExpectedRows == StoredRows);
            if (ExpectedPageFaults != Max<ui64>() && ExpectedPageFaults != PageFaults) {
                Cerr << "Expected " << ExpectedPageFaults << " page faults but got " << PageFaults << Endl;
            }
            Y_ABORT_UNLESS(ExpectedPageFaults == Max<ui64>() || ExpectedPageFaults == PageFaults);
        }

        Die(ctx);

        return new TDummyResult(StoredRows, ExpectedRows);
    }

public:
    TArrayRef<const TCell> LeadKey;
    ui64 ExpectedPageFaults = Max<ui64>();

private:
    TActorId Tablet;
    IDriver *Driver = nullptr;
    TIntrusiveConstPtr<TScheme> Scheme;
    ui64 StoredRows = 0;
    ui64 PageFaults = 0;
    ui64 ExpectedRowId = 1;
    ui64 ExpectedRows = 0;
    bool Postponed = false;
    EAbort Abort;
};

struct TEvTestFlatTablet {
    enum EEv {
        EvScanFinished = 2015 + EventSpaceBegin(TKikimrEvents::ES_TABLET),
        EvQueueScan,
        EvStartQueuedScan,
        EvMakeScanSnapshot,
        EvCancelScan,
        EvSnapshotComplete,

        EvEnd
    };

    struct TEvScanFinished : public TEventLocal<TEvScanFinished, EvScanFinished> {};
    struct TEvQueueScan : public TEventLocal<TEvQueueScan, EvQueueScan> {
        TEvQueueScan(ui32 rows, bool postponed = false, bool snap = false, NTable::EAbort abort = NTable::EAbort::None)
            : Postponed(postponed)
            , UseSnapshot(snap)
            , Abort(abort)
            , ReadVersion(TRowVersion::Max())
            , ExpectRows(rows)
        {}

        TEvQueueScan(ui32 rows, TRowVersion snapshot, bool postponed = false, NTable::EAbort abort = NTable::EAbort::None)
            : Postponed(postponed)
            , UseSnapshot(false)
            , Abort(abort)
            , ReadVersion(snapshot)
            , ExpectRows(rows)
        {}

        bool Postponed;
        bool UseSnapshot;
        NTable::EAbort Abort;
        const TRowVersion ReadVersion;
        const ui32 ExpectRows = 0;
        ui64 ExpectedPageFaults = Max<ui64>();

        std::optional<std::pair<ui64, ui64>> ReadAhead;
        TArrayRef<const TCell> LeadKey;
    };
    struct TEvStartQueuedScan : public TEventLocal<TEvStartQueuedScan, EvStartQueuedScan> {};
    struct TEvMakeScanSnapshot : public TEventLocal<TEvMakeScanSnapshot, EvMakeScanSnapshot> {};
    struct TEvCancelScan : public TEventLocal<TEvCancelScan, EvCancelScan> {};

    struct TEvSnapshotComplete : public TEventLocal<TEvSnapshotComplete, EvSnapshotComplete> {
        TIntrusivePtr<TTableSnapshotContext> SnapContext;

        TEvSnapshotComplete(TIntrusivePtr<TTableSnapshotContext> snapContext)
            : SnapContext(std::move(snapContext))
        { }
    };
};

class TTestTableSnapshotContext : public TTableSnapshotContext {
public:
    TTestTableSnapshotContext(TVector<ui32> tables)
        : Tables(std::move(tables))
    { }

    TConstArrayRef<ui32> TablesToSnapshot() const override {
        return Tables;
    }

private:
    TVector<ui32> Tables;
};

class ITransactionWithExecutor : public ITransaction {
    friend class TTestFlatTablet;

protected:
    NFlatExecutorSetup::IExecutor* Executor = nullptr;
};

class TTestFlatTablet : public TActor<TTestFlatTablet>, public TTabletExecutedFlat {
    TDummyScan *Scan;
    ui64 ScanTaskId;
    ui64 ScanCookie;
    ui64 SnapshotId;
    TActorId Sender;

    void SnapshotComplete(TIntrusivePtr<TTableSnapshotContext> snapContext, const TActorContext&) override {
        Send(Sender, new TEvTestFlatTablet::TEvSnapshotComplete(std::move(snapContext)));
    }

    void CompactionComplete(ui32 table, const TActorContext&) override {
        Send(Sender, new NFake::TEvCompacted(table));
    }

    void ScanComplete(NTable::EAbort, TAutoPtr<IDestructable>, ui64 cookie, const TActorContext&) override
    {
        UNIT_ASSERT_VALUES_EQUAL(cookie, ScanCookie);
        Send(Sender, new TEvTestFlatTablet::TEvScanFinished);
    }

    void Handle(TEvTestFlatTablet::TEvMakeScanSnapshot::TPtr&) {
        SnapshotId = Executor()->MakeScanSnapshot(TRowsModel::TableId);
        UNIT_ASSERT(SnapshotId);
        Send(Sender, new TEvents::TEvWakeup);
    }

    void Handle(TEvTestFlatTablet::TEvCancelScan::TPtr &/*ev*/, const TActorContext &ctx) {
        Executor()->CancelScan(TRowsModel::TableId, ScanTaskId);
        ctx.Send(Sender, new TEvents::TEvWakeup);
    }

    void Handle(TEvTestFlatTablet::TEvQueueScan::TPtr &ev) {
        bool postpone = ev->Get()->Postponed;
        ui64 snap = ev->Get()->UseSnapshot ? SnapshotId : 0;
        auto abort = ev->Get()->Abort;
        auto rows = abort != NTable::EAbort::None ? 0 : ev->Get()->ExpectRows;
        Scan = new TDummyScan(SelfId(), postpone, abort, rows);
        Scan->LeadKey = ev->Get()->LeadKey;
        Scan->ExpectedPageFaults = ev->Get()->ExpectedPageFaults;
        TScanOptions options;
        if (snap) {
            Y_ABORT_UNLESS(ev->Get()->ReadVersion.IsMax(), "Cannot combine multiple snapshot techniques");
            options.SetSnapshotId(snap);
        } else if (!ev->Get()->ReadVersion.IsMax()) {
            options.SetSnapshotRowVersion(ev->Get()->ReadVersion);
        }
        if (auto readAhead = ev->Get()->ReadAhead) {
            options.SetReadAhead(readAhead->first, readAhead->second);
        }
        ScanTaskId = Executor()->QueueScan(TRowsModel::TableId, Scan, ScanCookie, options);
    }

    void Handle(TEvTestFlatTablet::TEvStartQueuedScan::TPtr &/*ev*/, const TActorContext &ctx) {
        ctx.Send(Scan->SelfId(), new TEvents::TEvWakeup);
    }

    void Handle(NFake::TEvExecute::TPtr &ev, const TActorContext &ctx) {
        for (auto& f : ev->Get()->Funcs) {
            if (auto* tx = dynamic_cast<ITransactionWithExecutor*>(f.Get())) {
                tx->Executor = Executor();
            }
            Execute(f.Release(), ctx);
        }
    }

    void Handle(NFake::TEvCompact::TPtr &ev, const TActorContext&) {
        if (ev->Get()->MemOnly) {
            Executor()->CompactMemTable(ev->Get()->Table);
        } else {
            Executor()->CompactTable(ev->Get()->Table);
        }
        Send(Sender, new TEvents::TEvWakeup);
    }

    void Handle(NFake::TEvReturn::TPtr&, const TActorContext&) {
        Send(Sender, new TEvents::TEvWakeup);
    }

    void Handle(TEvents::TEvPoison::TPtr &, const TActorContext &ctx) {
        Become(&TThis::StateBroken);
        Executor()->DetachTablet(ctx), Detach(ctx); /* see TDummy tablet */
        ctx.Send(Sender, new TEvents::TEvGone);
    }

    void DefaultSignalTabletActive(const TActorContext&) override {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext&) override {
        Become(&TThis::StateWork);
        SignalTabletActive(SelfId());
        Send(Sender, new TEvents::TEvWakeup);
    }

    void OnDetach(const TActorContext &ctx) override {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override {
        Die(ctx);
    }

public:
    TTestFlatTablet(const TActorId &sender, const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, nullptr)
        , Scan(nullptr)
        , ScanTaskId(0)
        , ScanCookie(123)
        , SnapshotId(0)
        , Sender(sender)
    {}

    STFUNC(StateInit) {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTestFlatTablet::TEvQueueScan, Handle);
            HFunc(TEvTestFlatTablet::TEvStartQueuedScan, Handle);
            hFunc(TEvTestFlatTablet::TEvMakeScanSnapshot, Handle);
            HFunc(TEvTestFlatTablet::TEvCancelScan, Handle);
            HFunc(NFake::TEvExecute, Handle);
            HFunc(NFake::TEvCompact, Handle);
            HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
            HFunc(NFake::TEvReturn, Handle);
            HFunc(TEvents::TEvPoison, Handle);
        default:
            HandleDefaultEvents(ev, SelfId());
            break;
        }
    }

    STFUNC(StateBroken) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        }
    }
};


/**
 * Test scan going in parallel with compactions.
 *
 * 1. Fill table with rows so that one more row will cause three cascading compaction.
 * 2. Create scan. When scan is activated it sends wake-up to tablet and then sleeps.
 * 3. Add one more row and wait for compactions to finish.
 * 4. Resume scan.
 * 5. Check number of scanned rows.
 */
Y_UNIT_TEST_SUITE(TFlatTableCompactionScan) {
    Y_UNIT_TEST(TestCompactionScan) {
        TMyEnvBase env;
        TRowsModel data;

        env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });

        env.WaitForWakeUp();

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy();
        policy->InMemSizeToSnapshot = 40 * 1024 *1024;
        policy->InMemStepsToSnapshot = 10;
        policy->InMemForceStepsToSnapshot = 10;
        policy->InMemForceSizeToSnapshot = 64 * 1024 * 1024;
        policy->InMemResourceBrokerTask = NLocalDb::LegacyQueueIdToTaskName(0);
        policy->ReadAheadHiThreshold = 100000;
        policy->ReadAheadLoThreshold = 50000;
        policy->Generations.push_back({100 * 1024 * 1024, 5, 5, 200 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), true});
        policy->Generations.push_back({400 * 1024 * 1024, 5, 5, 800 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(2), false});
        for (auto& gen : policy->Generations) {
            gen.ExtraCompactionPercent = 0;
            gen.ExtraCompactionMinSize = 0;
            gen.ExtraCompactionExpPercent = 0;
            gen.ExtraCompactionExpMaxSize = 0;
            gen.UpliftPartSize = 0;
        }

        env.SendSync(data.MakeScheme(std::move(policy)));
        env.SendAsync(data.MakeRows(249));
        env.WaitFor<NFake::TEvCompacted>(28);
        env.WaitForWakeUp();

        env.SendSync(new TEvTestFlatTablet::TEvQueueScan(data.Rows(), true));
        env.SendAsync(data.MakeRows(1));
        env.WaitFor<NFake::TEvCompacted>(3);
        env.WaitForWakeUp();
        env.SendAsync(new TEvTestFlatTablet::TEvStartQueuedScan());
        TAutoPtr<IEventHandle> handle;
        env->GrabEdgeEventRethrow<TEvTestFlatTablet::TEvScanFinished>(handle);
        env.SendSync(new TEvents::TEvPoison, false, true);
    }
}


Y_UNIT_TEST_SUITE(TFlatTableExecutorTxLimit) {

    struct TTxSchema : public ITransaction {
        TTxSchema(TActorId owner) : Owner(owner) { }

        bool Execute(TTransactionContext &txc, const TActorContext&) override
        {
            txc.DB.Alter().SetExecutorLimitInFlyTx(2);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(Owner, new NFake::TEvResult);
        }

        const TActorId Owner;
    };

    struct TTxNoop : public ITransaction {
        TTxNoop(TActorId owner) : Owner(owner) { }

        bool Execute(TTransactionContext&, const TActorContext&) override
        {
            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(Owner, new NFake::TEvResult);
        }

        const TActorId Owner;
    };

    Y_UNIT_TEST(TestExecutorTxLimit) {
        TMyEnvBase env;

        env.FireDummyTablet();
        env.SendAsync(new NFake::TEvExecute{ new TTxSchema(env.Edge) });
        env.WaitFor<NFake::TEvResult>();

        for (size_t seq = 0; seq++ < 5;)
           env.SendAsync(new NFake::TEvExecute{ new TTxNoop(env.Edge) });

        env.WaitFor<NFake::TEvResult>(5);
        env.SendSync(new TEvents::TEvPoison, false, true);
    }
}


Y_UNIT_TEST_SUITE(TFlatTableReschedule) {

    class TTxRollbackOnReschedule : public ITransaction {
    public:
        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            Y_ABORT_UNLESS(!Done);

            i64 keyId = 42;

            int attempt = ++Attempt;
            if (attempt >= 2) {
                TVector<NTable::TTag> tags;
                tags.push_back(TRowsModel::ColumnValueId);
                TVector<TRawTypeValue> key;
                key.emplace_back(&keyId, sizeof(keyId), NScheme::TTypeInfo(NScheme::TInt64::TypeId));
                NTable::TRowState row;
                auto ready = txc.DB.Select(TRowsModel::TableId, key, tags, row);
                if (ready == NTable::EReady::Page) {
                    return false;
                }
                Y_ABORT_UNLESS(ready == NTable::EReady::Gone);
            }

            TString valueText = "value";
            const auto key = NScheme::TInt64::TInstance(keyId);
            const auto val = NScheme::TString::TInstance(valueText);
            NTable::TUpdateOp updateOp{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, val };
            txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { key }, { updateOp });

            if (attempt == 1) {
                txc.Reschedule();
                return false;
            }

            Done = true;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            Y_ABORT_UNLESS(Done);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        int Attempt = 0;
        bool Done = false;
    };

    Y_UNIT_TEST(TestExecuteReschedule) {
        TMyEnvBase env;
        TRowsModel rows;

        env.FireDummyTablet();
        env.SendSync(rows.MakeScheme(new TCompactionPolicy()));
        env.SendSync(new NFake::TEvExecute{ new TTxRollbackOnReschedule });
    }

} // namespace Y_UNIT_TEST_SUITE(TFlatTableExecuteReschedule)


Y_UNIT_TEST_SUITE(TFlatTableBackgroundCompactions) {

    using namespace NKikimrResourceBroker;
    using namespace NResourceBroker;

    struct TIsTaskSubmission {
        TIsTaskSubmission(const TString &type, ui32 maxPriority = Max<ui32>())
            : Type(type)
            , MaxPriority(maxPriority)
        {}

        bool operator()(IEventHandle &ev) {
            if (ev.GetTypeRewrite() == NResourceBroker::TEvResourceBroker::EvSubmitTask) {
                auto *e = ev.Get<NResourceBroker::TEvResourceBroker::TEvSubmitTask>();
                if (e->Task.Type == Type && e->Task.Priority <= MaxPriority) {
                    return true;
                }
            }

            return false;
        }

        TString Type;
        ui32 MaxPriority;
    };

    struct TIsTaskUpdate {
        TIsTaskUpdate(const TString &type, ui32 maxPriority = Max<ui32>())
            : Type(type)
            , MaxPriority(maxPriority)
        {}

        bool operator()(IEventHandle &ev) {
            if (ev.GetTypeRewrite() == NResourceBroker::TEvResourceBroker::EvUpdateTask) {
                auto *e = ev.Get<NResourceBroker::TEvResourceBroker::TEvUpdateTask>();
                if (e->Type == Type && e->Priority <= MaxPriority)
                    return true;
            }

            return false;
        }

        TString Type;
        ui32 MaxPriority;
    };

    struct TIsResourceAllocation {
        TIsResourceAllocation()
        {}

        bool operator()(IEventHandle &ev) {
            if (ev.GetTypeRewrite() == NResourceBroker::TEvResourceBroker::EvResourceAllocated)
                return true;

            return false;
        }
    };

    struct TMyEnvCompaction : public TMyEnvBase {
        TMyEnvCompaction()
        {
            Env.SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
            FireDummyTablet();
            SendSync(Rows.MakeScheme(MakeCompactionPolicy()));
        }

        ~TMyEnvCompaction()
        {
            SendSync(new TEvents::TEvPoison, false, true);
        }

        void BlockBackgroundQueue(ui64 block = 0, ui64 level = 0, bool instant = true)
        {
            ui64 id = 987987987987 + block;
            TAutoPtr<TEvResourceBroker::TEvSubmitTask> event
                = new TEvResourceBroker::TEvSubmitTask(id,
                                                       "bckg-block",
                                                       {{1, 0}},
                                                       "background_compaction",
                                                       level,
                                                       nullptr);
            SendEv(MakeResourceBrokerID(), event.Release());

            if (instant) {
                TAutoPtr<IEventHandle> handle;
                auto reply = Env.GrabEdgeEventRethrow<TEvResourceBroker::TEvResourceAllocated>(handle);
                UNIT_ASSERT_VALUES_EQUAL(reply->TaskId, id);
            }
        }

        void UnblockBackgroundQueue(ui64 block = 0)
        {
            ui64 id = 987987987987 + block;
            TAutoPtr<TEvResourceBroker::TEvFinishTask> event
                = new TEvResourceBroker::TEvFinishTask(id);
            SendEv(MakeResourceBrokerID(), event.Release());
        }

        static TCompactionPolicy* MakeCompactionPolicy() noexcept
        {
            auto *policy = new TCompactionPolicy();
            policy->InMemSizeToSnapshot = 40 * 1024 *1024;
            policy->InMemStepsToSnapshot = 10;
            policy->InMemForceStepsToSnapshot = 10;
            policy->InMemForceSizeToSnapshot = 64 * 1024 * 1024;
            policy->InMemResourceBrokerTask = NLocalDb::LegacyQueueIdToTaskName(0);
            policy->ReadAheadHiThreshold = 100000;
            policy->ReadAheadLoThreshold = 50000;
            policy->BackgroundSnapshotPolicy.Threshold = 50;
            policy->BackgroundSnapshotPolicy.ResourceBrokerTask = "background_compaction_gen0";
            policy->Generations.push_back({100 * 1024 * 1024, 10, 10, 200 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), true,
                        {50, 100, 0.0, "background_compaction_gen1"}});
            policy->Generations.push_back({400 * 1024 * 1024, 10, 10, 800 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(2), false,
                        {50, 200, 1.0, "background_compaction_gen2"}});
            for (auto& gen : policy->Generations) {
                gen.ExtraCompactionPercent = 0;
                gen.ExtraCompactionMinSize = 0;
                gen.ExtraCompactionExpPercent = 0;
                gen.ExtraCompactionExpMaxSize = 0;
                gen.UpliftPartSize = 0;
            }
            return policy;
        }

        TRowsModel Rows;
    };

    Y_UNIT_TEST(TestRunBackgroundSnapshot) {
        TMyEnvCompaction env;

        env.SendAsync(env.Rows.MakeRows(5));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TIsTaskSubmission("background_compaction_gen0"));
        env->DispatchEvents(options);
    }

    Y_UNIT_TEST(TestChangeBackgroundSnapshotToRegular) {
        TMyEnvCompaction env;

        env.BlockBackgroundQueue();

        env.SendAsync(env.Rows.MakeRows(5));
        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TIsTaskSubmission("background_compaction_gen0"));
        env->DispatchEvents(options1);

        env.SendAsync(env.Rows.MakeRows(5));
        TDispatchOptions options2;
        options2.FinalEvents.emplace_back(TIsTaskUpdate("compaction_gen0", 5));
        env->DispatchEvents(options2);
    }

    Y_UNIT_TEST(TestRunBackgroundCompactionGen1) {
        TMyEnvCompaction env;

        env.BlockBackgroundQueue();
        env.SendAsync(env.Rows.MakeRows(50));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TIsTaskSubmission("background_compaction_gen1"));
        env->DispatchEvents(options);
    }

    Y_UNIT_TEST(TestChangeBackgroundCompactionToRegular) {
        TMyEnvCompaction env;

        env.BlockBackgroundQueue();
        env.SendAsync(env.Rows.MakeRows(50));

        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TIsTaskSubmission("background_compaction_gen1"));
        env->DispatchEvents(options1);

        env.SendAsync(env.Rows.MakeRows(50));

        TDispatchOptions options2;
        options2.FinalEvents.emplace_back(TIsTaskUpdate("compaction_gen1", 5));
        env->DispatchEvents(options2);
    }

    Y_UNIT_TEST(TestRunBackgroundCompactionGen2) {
        TMyEnvCompaction env;

        env.BlockBackgroundQueue();
        env.SendAsync(env.Rows.MakeRows(500));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TIsTaskSubmission("background_compaction_gen2"));
        env->DispatchEvents(options);
    }

    Y_UNIT_TEST(TestChangeBackgroundSnapshotPriorityByTime) {
        TMyEnvCompaction env;

        env.BlockBackgroundQueue();
        env.SendAsync(env.Rows.MakeRows(5));

        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TIsTaskSubmission("background_compaction_gen0"));
        env->DispatchEvents(options1);

        TDispatchOptions options2;
        options2.FinalEvents.emplace_back(TIsTaskUpdate("background_compaction_gen0", 50));
        env->SetDispatchTimeout(TDuration::Minutes(1));
        env->DispatchEvents(options2);
    }

    Y_UNIT_TEST(TestChangeBackgroundCompactionPriorityByTime) {
        TMyEnvCompaction env;

        env.BlockBackgroundQueue();
        env.SendAsync(env.Rows.MakeRows(550));

        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TIsTaskSubmission("background_compaction_gen1"), 6);
        options1.FinalEvents.emplace_back(TIsTaskSubmission("background_compaction_gen2"));
        env->DispatchEvents(options1);

        TDispatchOptions options2;
        options2.FinalEvents.emplace_back(TIsTaskUpdate("background_compaction_gen2", 100));

        env->SetDispatchTimeout(TDuration::Minutes(1));
        env->DispatchEvents(options2);

        // GEN2 compaction should become more prioritized due to time factor.
        TDispatchOptions options3;
        env.BlockBackgroundQueue(1, 150, false);
        env.UnblockBackgroundQueue();
        options3.FinalEvents.emplace_back(TIsResourceAllocation());
        env->DispatchEvents(options3);

        env.SendAsync(env.Rows.MakeRows(10));

        TDispatchOptions options4;
        options4.FinalEvents.emplace_back(TIsTaskUpdate("background_compaction_gen1", 170));
        env->DispatchEvents(options4);
    }
}


Y_UNIT_TEST_SUITE(TFlatTablePostponedScan) {

    using namespace NKikimrResourceBroker;
    using namespace NResourceBroker;

    struct TMyEnvScans : public TMyEnvBase {

        TMyEnvScans()
        {
            Env.SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_INFO);
            Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_INFO);
            Env.SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_INFO);

            FireTablet(Edge, Tablet, [this](const TActorId &tablet, TTabletStorageInfo *info) {
                return new TTestFlatTablet(Edge, tablet, info);
            });

            WaitForWakeUp();

            SendSync(Rows.MakeScheme(MakeCompactionPolicy()));
        }

        ~TMyEnvScans()
        {
            SendSync(new TEvents::TEvPoison, false, true);
        }

        static TCompactionPolicy* MakeCompactionPolicy() noexcept
        {
            auto *policy = new TCompactionPolicy();
            policy->InMemSizeToSnapshot = 40 * 1024 *1024;
            policy->InMemStepsToSnapshot = 10;
            policy->InMemForceStepsToSnapshot = 10;
            policy->InMemForceSizeToSnapshot = 64 * 1024 * 1024;
            policy->InMemResourceBrokerTask = NLocalDb::LegacyQueueIdToTaskName(0);
            policy->ReadAheadHiThreshold = 100000;
            policy->ReadAheadLoThreshold = 50000;
            policy->Generations.push_back({100 * 1024 * 1024, 10, 10, 200 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), true});
            return policy;
        }

        TRowsModel Rows;
    };

    Y_UNIT_TEST(TestPostponedScan) {
        TMyEnvScans env;
        TAutoPtr<IEventHandle> handle;

        env.SendSync(env.Rows.MakeRows(111));
        env.SendSync(new TEvTestFlatTablet::TEvMakeScanSnapshot);
        env.SendSync(env.Rows.MakeRows(111));
        env.SendAsync(new TEvTestFlatTablet::TEvQueueScan(111, false, true));
        env->GrabEdgeEventRethrow<TEvTestFlatTablet::TEvScanFinished>(handle);
    }

    Y_UNIT_TEST(TestCancelFinishedScan) {
        TMyEnvScans env;
        TAutoPtr<IEventHandle> handle;

        env.SendSync(env.Rows.MakeRows(111));
        env.SendSync(new TEvTestFlatTablet::TEvMakeScanSnapshot);
        env.SendAsync(new TEvTestFlatTablet::TEvQueueScan(111, false, true));
        env->GrabEdgeEventRethrow<TEvTestFlatTablet::TEvScanFinished>(handle);

        env.SendSync(new TEvTestFlatTablet::TEvCancelScan);
    }

    Y_UNIT_TEST(TestCancelRunningPostponedScan) {
        TMyEnvScans env;
        TAutoPtr<IEventHandle> handle;

        env.SendSync(env.Rows.MakeRows(111));
        env.SendSync(new TEvTestFlatTablet::TEvMakeScanSnapshot);
        env.SendSync(new TEvTestFlatTablet::TEvQueueScan(111, true, true, NTable::EAbort::Term));
        env.SendSync(new TEvTestFlatTablet::TEvCancelScan);
//        env->GrabEdgeEventRethrow<TEvTestFlatTablet::TEvScanFinished>(handle);
    }

    Y_UNIT_TEST(TestPostponedScanSnapshotMVCC) {
        TMyEnvScans env;

        env.SendSync(env.Rows.VersionTo(TRowVersion(1, 10)).MakeRows(111));
        env.SendSync(new TEvTestFlatTablet::TEvQueueScan(111, TRowVersion(1, 15), true));
        env.SendSync(env.Rows.VersionTo(TRowVersion(2, 20)).MakeRows(111));
        env.SendAsync(new TEvTestFlatTablet::TEvStartQueuedScan());
        env.GrabEdgeEvent<TEvTestFlatTablet::TEvScanFinished>();
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorResourceProfile) {

    struct TTaskSequence {
        struct TEvent {
            ui32 EventType;
            TString TaskType;
        };

        TTaskSequence()
        {}

        void Add(ui32 event, const TString &task = "")
        {
            Events.push_back({event, task});
        }

        bool operator()(IEventHandle &ev) {
            if (Current == Events.size())
                return false;

            auto &event = Events[Current];
            bool match = false;
            if (ev.GetTypeRewrite() == event.EventType) {
                if (ev.GetTypeRewrite() == NResourceBroker::TEvResourceBroker::EvSubmitTask) {
                    auto *e = ev.Get<NResourceBroker::TEvResourceBroker::TEvSubmitTask>();
                    if (e->Task.Type == event.TaskType) {
                        match = true;
                    }
                } else if (ev.GetTypeRewrite() == NResourceBroker::TEvResourceBroker::EvUpdateTask) {
                    auto *e = ev.Get<NResourceBroker::TEvResourceBroker::TEvUpdateTask>();
                    if (e->Type == event.TaskType) {
                        match = true;
                    }
                } else {
                    Y_ABORT_UNLESS(!event.TaskType);
                    match = true;
                }
            }

            if (match) {
                ++Current;
            }

            return Current == Events.size();
        }

        TVector<TEvent> Events;
        size_t Current = 0;
    };

    struct TMemoryCheckEntry {
        struct TEntryEvents {
            TString MergeTask;
            TString UpdateTask;
            TString SubmitTask;
            ui32 FinishTasks;

            TEntryEvents(const TString &submitTask = "",
                         const TString &updateTask = "",
                         const TString &mergeTask = "",
                         ui32 finishTasks = 0)
                : MergeTask(mergeTask)
                , UpdateTask(updateTask)
                , SubmitTask(submitTask)
                , FinishTasks(finishTasks)
            {}
        };

        TVector<ui64> Keys;
        ui64 NewSize;
        bool Static;
        bool Hold;
        ui64 HoldSize;
        bool Use;
        bool Finish;
        bool OutOfMemory;
        TEntryEvents Events;

        TMemoryCheckEntry(std::initializer_list<ui64> keys, ui64 newSize, TEntryEvents events,
                          bool isStatic = false, bool outOfMemory = false)
            : Keys(keys)
            , NewSize(newSize)
            , Static(isStatic)
            , Hold(false)
            , HoldSize(0)
            , Use(false)
            , Finish(false)
            , OutOfMemory(outOfMemory)
            , Events(std::move(events))
        {}

        TMemoryCheckEntry(ui64 newSize, TEntryEvents events,
                          bool isStatic = false, bool outOfMemory = false)
            : NewSize(newSize)
            , Static(isStatic)
            , Hold(false)
            , HoldSize(0)
            , Use(false)
            , Finish(false)
            , OutOfMemory(outOfMemory)
            , Events(std::move(events))
        {}

        TMemoryCheckEntry(bool hold, ui64 newSize, TEntryEvents events,
                          bool isStatic = false, bool outOfMemory = false)
            : NewSize(newSize)
            , Static(isStatic)
            , Hold(hold)
            , HoldSize(0)
            , Use(false)
            , Finish(hold)
            , OutOfMemory(outOfMemory)
            , Events(std::move(events))
        {}

        TMemoryCheckEntry(bool hold, bool use, ui64 newSize, TEntryEvents events,
                          bool isStatic = false, bool outOfMemory = false)
            : NewSize(newSize)
            , Static(isStatic)
            , Hold(hold)
            , HoldSize(0)
            , Use(use)
            , Finish(hold)
            , OutOfMemory(outOfMemory)
            , Events(std::move(events))
        {}

        TMemoryCheckEntry(bool hold, bool use, bool finish, ui64 newSize, TEntryEvents events,
                          bool isStatic = false, bool outOfMemory = false)
            : NewSize(newSize)
            , Static(isStatic)
            , Hold(hold)
            , HoldSize(0)
            , Use(use)
            , Finish(finish)
            , OutOfMemory(outOfMemory)
            , Events(std::move(events))
        {}

        TMemoryCheckEntry(ui64 hold, ui64 newSize, TEntryEvents events,
                          bool isStatic = false, bool outOfMemory = false)
            : NewSize(newSize)
            , Static(isStatic)
            , Hold(true)
            , HoldSize(hold)
            , Use(false)
            , Finish(true)
            , OutOfMemory(outOfMemory)
            , Events(std::move(events))
        {}

        TMemoryCheckEntry(ui64 hold, bool use, ui64 newSize, TEntryEvents events,
                          bool isStatic = false, bool outOfMemory = false)
            : NewSize(newSize)
            , Static(isStatic)
            , Hold(true)
            , HoldSize(hold)
            , Use(use)
            , Finish(true)
            , OutOfMemory(outOfMemory)
            , Events(std::move(events))
        {}
    };

    struct TTxSetResourceProfile : public ITransaction {
        TString ProfileName;

        TTxSetResourceProfile(const TString &name) : ProfileName(name) {}

        bool Execute(TTransactionContext &txc, const TActorContext &) override {
            txc.DB.Alter().SetExecutorResourceProfile(ProfileName);
            return true;
        }

        void Complete(const TActorContext &ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxCheckResourceProfile : public ITransaction {
        TString ProfileName;

        TTxCheckResourceProfile(const TString &name) : ProfileName(name) {}

        bool Execute(TTransactionContext &txc, const TActorContext&) override {
            UNIT_ASSERT_VALUES_EQUAL(txc.DB.GetScheme().Executor.ResourceProfile, ProfileName);
            return true;
        }

        void Complete(const TActorContext &ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxRequestMemory : public ITransaction {
        struct TRequest {
            TVector<ui64> Keys;
            ui64 TxDataSize;
            bool StaticResult;
            bool OutOfMemoryResult;
            bool Hold;
            bool Use;
            ui64 HoldSize;
            bool Finish;
        };

        using TRequests = TVector<TRequest>;

        struct TCfg {
            bool Static = false;
            bool Dynamic = false;
        };

        const TCfg Cfg;
        TRequests Requests;
        size_t ReqNo = 0;
        TAutoPtr<TMemoryToken> Token;

        TTxRequestMemory(TRequests requests, size_t reqNo, TAutoPtr<TMemoryToken> token, TCfg cfg)
            : Cfg(cfg)
            , Requests(requests)
            , ReqNo(reqNo)
            , Token(token)
        {}

        bool Execute(TTransactionContext &txc, const TActorContext &/*ctx*/) override {
            if (ReqNo) {
                auto &prev = Requests[ReqNo - 1];

                if (!prev.Finish) {
                    UNIT_ASSERT(!prev.OutOfMemoryResult);
                    UNIT_ASSERT(Cfg.Dynamic || Cfg.Static || prev.TxDataSize <= txc.GetMemoryLimit());
                    if (prev.StaticResult)
                        UNIT_ASSERT_VALUES_EQUAL(txc.GetTaskId(), 0);
                    else
                        UNIT_ASSERT(txc.GetTaskId() > 0);
                }
            }

            if (ReqNo == Requests.size())
                return true;

            auto &req = Requests[ReqNo++];
            ui64 mem = txc.GetMemoryLimit();

            if (req.Use) {
                UNIT_ASSERT(Token);
                mem = Token->GCToken->Size;
                txc.UseMemoryToken(std::move(Token));
                Token = nullptr;
            }

            if (req.Hold) {
                if (req.HoldSize) {
                    Token = txc.HoldMemory(req.HoldSize);
                    if (!req.Use)
                        mem -= req.HoldSize;
                } else {
                    Token = txc.HoldMemory();
                    if (!req.Use)
                        mem = 0;
                }
            }

            if (req.Finish)
                return true;

            if (req.TxDataSize) {
                UNIT_ASSERT(req.TxDataSize >= mem);
                txc.RequestMemory(req.TxDataSize - mem);
            } else {
                txc.RequestMemory(1);
            }

            if (!req.Keys.empty()) {
                for (auto val : req.Keys) {
                    ui64 key1 = val;
                    TRawTypeValue key[] = {TRawTypeValue(&key1, sizeof(key1), NScheme::TTypeInfo(NScheme::NTypeIds::Int64))};
                    NTable::TTag tags[] = {TRowsModel::ColumnKeyId};
                    NTable::TRowState row;
                    txc.DB.Select(TRowsModel::TableId, {key, 1}, {tags, 1}, row);
                }
            }

            return false;
        }

        void Complete(const TActorContext &ctx) override {
            if (ReqNo)
                UNIT_ASSERT(!Requests[ReqNo - 1].OutOfMemoryResult);
            if (ReqNo == Requests.size())
                ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            else
                ctx.Send(ctx.SelfID, new NFake::TEvExecute{ new TTxRequestMemory(Requests, ReqNo, std::move(Token), Cfg) });
        }

        void Terminate(ETerminationReason reason, const TActorContext &ctx) override {
            UNIT_ASSERT_EQUAL(reason, ETerminationReason::MemoryLimitExceeded);
            UNIT_ASSERT(ReqNo);
            UNIT_ASSERT(Requests[ReqNo - 1].OutOfMemoryResult);

            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

        void ReleaseTxData(TTxMemoryProvider &provider, const TActorContext &/*ctx*/) override {
            if ((provider.GetTaskId() && Cfg.Dynamic)
                || (!provider.GetTaskId() && Cfg.Static))
                Token = provider.HoldMemory();
        }
    };

    struct TMyEnvProfiles : public TMyEnvBase {

        TMyEnvProfiles()
        {
            Env.SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
            Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
            SetupProfile("profile1");
            FireDummyTablet();
            SendSync(Rows.MakeScheme(MakeCompactionPolicy()));
            SendSync(new NFake::TEvExecute(new TTxSetResourceProfile("profile1")));

            Profile = Env.GetAppData().ResourceProfiles->GetProfile(NKikimrTabletBase::TTabletTypes::Unknown, "profile1");
        }

        ~TMyEnvProfiles()
        {
            SendSync(new TEvents::TEvPoison, false, true);
        }

        static TCompactionPolicy* MakeCompactionPolicy() noexcept
        {
            auto *policy = new TCompactionPolicy();
            policy->InMemSizeToSnapshot = 40 * 1024 *1024;
            policy->InMemStepsToSnapshot = 5;
            policy->InMemForceStepsToSnapshot = 5;
            policy->InMemForceSizeToSnapshot = 64 * 1024 * 1024;
            policy->InMemResourceBrokerTask = NLocalDb::LegacyQueueIdToTaskName(0);
            policy->ReadAheadHiThreshold = 100000;
            policy->ReadAheadLoThreshold = 50000;
            policy->Generations.push_back({100 * 1024 * 1024, 100, 100, 200 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), true});
            return policy;
        }

        void SetupProfile(const TString profileName)
        {
            auto &appData = Env.GetAppData();
            appData.ResourceProfiles = new TResourceProfiles;

            TResourceProfiles::TResourceProfile profile;
            profile.SetTabletType(NKikimrTabletBase::TTabletTypes::Unknown);
            profile.SetName(profileName);
            profile.SetStaticTabletTxMemoryLimit(100 << 20);
            profile.SetStaticTxMemoryLimit(15 << 10);
            profile.SetTxMemoryLimit(300 << 20);
            profile.SetInitialTxMemory(1 << 10);
            profile.SetSmallTxMemoryLimit(40 << 10);
            profile.SetMediumTxMemoryLimit(100 << 10);
            profile.SetSmallTxTaskType("small_transaction");
            profile.SetMediumTxTaskType("medium_transaction");
            profile.SetLargeTxTaskType("large_transaction");
            appData.ResourceProfiles->AddProfile(profile);
        }

        void CheckMemoryRequest(std::initializer_list<TMemoryCheckEntry> list,
                                    TTxRequestMemory::TCfg cfg = { },
                                    bool follower = false)
        {
            TAutoPtr<TTxRequestMemory> event = new TTxRequestMemory({ }, 0, nullptr, cfg);

            TTaskSequence sequence;

            for (auto &entry : list) {
                event->Requests.push_back({entry.Keys, entry.NewSize, entry.Static, entry.OutOfMemory,
                            entry.Hold, entry.Use, entry.HoldSize, entry.Finish});
                if (entry.Events.MergeTask) {
                    sequence.Add(NResourceBroker::TEvResourceBroker::EvUpdateTask, entry.Events.MergeTask);
                    sequence.Add(NResourceBroker::TEvResourceBroker::EvFinishTask);
                }
                if (entry.Events.UpdateTask) {
                    sequence.Add(NResourceBroker::TEvResourceBroker::EvUpdateTask, entry.Events.UpdateTask);
                    sequence.Add(NResourceBroker::TEvResourceBroker::EvResourceAllocated);
                }
                if (entry.Events.SubmitTask) {
                    sequence.Add(NResourceBroker::TEvResourceBroker::EvSubmitTask, entry.Events.SubmitTask);
                    sequence.Add(NResourceBroker::TEvResourceBroker::EvResourceAllocated);
                }
                for (ui32 i = 0; i < entry.Events.FinishTasks; ++i) {
                    sequence.Add(NResourceBroker::TEvResourceBroker::EvFinishTask);
                }
            }

            if (follower) {
                SendFollowerAsync(new NFake::TEvExecute{ event.Release() });
            } else {
                SendAsync(new NFake::TEvExecute{ event.Release() });
            }

            if (!sequence.Events.empty()) {
                TDispatchOptions options;
                options.FinalEvents.emplace_back(sequence);
                Env.DispatchEvents(options);
            }

            WaitForWakeUp();
        }

        void CheckMemoryRequestFollower(std::initializer_list<TMemoryCheckEntry> list,
                                     TTxRequestMemory::TCfg cfg = { })
        {
            return CheckMemoryRequest(list, std::move(cfg), /* follower */ true);
        }

        TResourceProfiles::TPtr Profile;
        TRowsModel Rows;
    };


    Y_UNIT_TEST(TestExecutorSetResourceProfile) {
        TMyEnvProfiles env;

        env.SendSync(new NFake::TEvExecute{ new TTxCheckResourceProfile("profile1") });
        env.SendSync(new TEvents::TEvPoison(), false, true);
        env.FireDummyTablet();
        env.SendSync(new NFake::TEvExecute{ new TTxCheckResourceProfile("profile1") }, true);
    }

    Y_UNIT_TEST(TestExecutorRequestTxData) {
        TMyEnvProfiles env;

        // Request static memory.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true}});
        // Request dynamic memory (small task).
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}}});
        // Request dynamic memory (medium task).
        env.CheckMemoryRequest(
                           {{50 << 10, {"medium_transaction"}}});
        // Request dynamic memory (large task).
        env.CheckMemoryRequest(
                           {{110 << 10, {"large_transaction"}}});
        // Request static memory, into small task, into medium task, into large task.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true},
                            {20 << 10, {"small_transaction"}},
                            {50 << 10, {"", "medium_transaction"}},
                            {110 << 10, {"", "large_transaction"}}});
    }

    Y_UNIT_TEST(TestExecutorStaticMemoryLimits) {
        TMyEnvProfiles env;

        const ui64 limit = env.Profile->GetStaticTabletTxMemoryLimit();

        // Check static tablet limit has priority over tx limits.
        env.Profile->SetStaticTxMemoryLimit(limit * 2);
        env.CheckMemoryRequest({{limit + 1, {"large_transaction"}}});

        // Check unlimited tablet memory.
        env.Profile->SetStaticTabletTxMemoryLimit(0);
        env.CheckMemoryRequest({{limit + 1, {}, true}});
        env.CheckMemoryRequest({{limit * 2 + 1, {"large_transaction"}}});

        // Check unlimited tx memory.
        env.Profile->SetStaticTxMemoryLimit(0);
        env.CheckMemoryRequest({{limit * 2 + 1, {}, true}});
    }

    Y_UNIT_TEST(TestExecutorReuseStaticMemory) {
        TMyEnvProfiles env;

        env.Profile->SetStaticTxMemoryLimit(100 << 20);
        // Check static tablet memory is freed and reused by transactions.
        for (int i = 0; i < 100; ++i) {
            if (i % 2) {
                env.CheckMemoryRequest(
                                   {{50 << 20, {}, true},
                                    {150 << 20, {"large_transaction"}}});
            } else {
                env.CheckMemoryRequest(
                                   {{50 << 20, {}, true}});
            }
        }
    }

    Y_UNIT_TEST(TestExecutorTxDataLimitExceeded) {
        TMyEnvProfiles().CheckMemoryRequest({{500 << 20, {}, false, true}});
    }

    Y_UNIT_TEST(TestExecutorRequestPages) {
        TMyEnvProfiles env;

        env.SendSync(env.Rows.MakeRows(100, 2 << 10));

        // Static memory for pages.
        env.CheckMemoryRequest(
                           {{{1}, 0, {}, true}});
        // Dynamic memory for pages.
        env.CheckMemoryRequest(
                           {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, 0, {"small_transaction"}}});
        // Dynamic memory for pages.
        env.CheckMemoryRequest(
                           {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                              16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30},
                             0, {"medium_transaction"}}});
        // Dynamic memory for pages.
        env.CheckMemoryRequest(
                           {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                              11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                              21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
                              31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
                              41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
                              51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
                              61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
                              71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
                              81, 82, 83, 84, 85, 86, 87, 88, 89, 90},
                             0, {"large_transaction"}}});
    }

    Y_UNIT_TEST(TestExecutorPageLimitExceeded) {
        TMyEnvProfiles env;

        env.Profile->SetTxMemoryLimit(50 << 10);
        env.SendSync(env.Rows.MakeRows(20, 10 << 10));

        // Pages are out of tx limit.
        env.CheckMemoryRequest(
                           {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, 0, {}, false, true}});
    }

    Y_UNIT_TEST(TestExecutorRequestMemory) {
        TMyEnvProfiles env;

        env.SendSync(env.Rows.MakeRows(100, 2 << 10));

        // Static memory.
        env.CheckMemoryRequest(
                           {{{1}, 4 << 10, {}, true}});
        // Dynamic memory.
        env.CheckMemoryRequest(
                           {{{1, 2, 3, 4, 5}, 14 << 10, {"small_transaction"}}});
        // Dynamic memory.
        env.CheckMemoryRequest(
                           {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 35 << 10, {"medium_transaction"}}});
        // Dynamic memory.
        env.CheckMemoryRequest(
                           {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 95 << 10, {"large_transaction"}}});
    }

    Y_UNIT_TEST(TestExecutorRequestMemoryFollower) {
        TMyEnvProfiles env;

        env.SendSync(env.Rows.MakeRows(100, 2 << 10));

        env.FireDummyFollower(1);

        // Static memory.
        env.CheckMemoryRequestFollower(
                           {{{1}, 4 << 10, {}, true}});
        // Dynamic memory.
        env.CheckMemoryRequestFollower(
                           {{{1, 2, 3, 4, 5}, 14 << 10, {"small_transaction"}}});
        // Dynamic memory.
        env.CheckMemoryRequestFollower(
                           {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 35 << 10, {"medium_transaction"}}});
        // Dynamic memory.
        env.CheckMemoryRequestFollower(
                           {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 95 << 10, {"large_transaction"}}});
    }

    Y_UNIT_TEST(TestExecutorMemoryLimitExceeded) {
        TMyEnvProfiles env;

        env.Profile->SetTxMemoryLimit(50 << 10);
        env.SendSync(env.Rows.MakeRows(20, 2 << 10));

        // Memory is out of tx limit.
        env.CheckMemoryRequest({{{1, 2, 3, 4, 5}, 45 << 10, {}, false, true}});
    }

    Y_UNIT_TEST(TestExecutorPreserveTxData) {
        TMyEnvProfiles env;

        // Preserved static replaces static.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true},
                            {true, 0, {}, true},
                            {12 << 10, {}, true},
                            {false, true, 0, {}, true}});
        // Preserved static replaces dynamic.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true},
                            {true, 0, {}, true},
                            {20 << 10, {"small_transaction"}},
                            {false, true, 0, {}}});
        // Preserved dynamic replaces static.
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}},
                            {true, 0, {}},
                            {10 << 10, {}, true},
                            {false, true, 0, {}}});
        // Preserved dynamic replaces dynamic.
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}},
                            {true, 0, {}},
                            {20 << 10, {"small_transaction"}},
                            {false, true, 0, {}}});

        // Preserved static replaces static.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true},
                            {true, 0, {}, true},
                            {12 << 10, {}, true},
                            {false, true, 11 << 10, {}, true}});
        // Preserved static replaces dynamic.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true},
                            {true, 0, {}, true},
                            {20 << 10, {"small_transaction"}},
                            {false, true, 30 << 10, {}}});
        // Preserved dynamic replaces static.
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}},
                            {true, 0, {}},
                            {10 << 10, {}, true},
                            {false, true, 50 << 10, {"", "medium_transaction"}}});
        // Preserved dynamic replaces dynamic.
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}},
                            {true, 0, {}, true},
                            {20 << 10, {"small_transaction"}},
                            {false, true, 50 << 10, {"", "medium_transaction", "small_transaction"}}});

        // Preserved static replaces static.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true},
                            {true, 0, {}, true},
                            {12 << 10, {}, true},
                            {false, true, 500 << 20, {}, true, true}});
        // Preserved static replaces dynamic.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true},
                            {true, 0, {}, true},
                            {20 << 10, {"small_transaction"}},
                            {false, true, 500 << 20, {}, false, true}});
        // Preserved dynamic replaces static.
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}},
                            {true, 0, {}},
                            {10 << 10, {}, true},
                            {false, true, 500 << 20, {}, false, true}});
        // Preserved dynamic replaces dynamic.
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}},
                            {true, 0, {}, true},
                            {20 << 10, {"small_transaction"}},
                            {false, true, 500 << 20, {}, false, true}});
    }

    Y_UNIT_TEST(TestExecutorTxDataGC) {
        TMyEnvProfiles env;

        // Preserve dynamic and drop it.
        env.CheckMemoryRequest(
                           {{50 << 20, {"large_transaction"}},
                            {true, 0, {}}});
    }

    Y_UNIT_TEST(TestExecutorTxPartialDataHold) {
        TMyEnvProfiles env;

        // Hold part of static data.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true},
                            {(ui64)(5 << 10), 0, {}, true},
                            {false, true, 0, {}, true}});
        // Hold part of dynamic data.
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}},
                            {(ui64)(10 << 10), 0, {}},
                            {false, true, 0, {}}});
    }

    Y_UNIT_TEST(TestExecutorTxHoldAndUse) {
        TMyEnvProfiles env;

        // Hold part of static data.
        env.CheckMemoryRequest(
                           {{10 << 10, {}, true},
                            {(ui64)(5 << 10), 0, {}, true},
                            {true, true, 0, {}, true}});
        // Hold part of dynamic data. Merge two small tasks into one medium.
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}},
                            {(ui64)(15 << 10), 0, {}},
                            {30 << 10, {"small_transaction"}},
                            {true, true, 0, {"", "", ""}},
                            {30 << 10, {"small_transaction"}},
                            {false, true, 0, {"", "", "medium_transaction"}}});
    }

    Y_UNIT_TEST(TestExecutorTxHoldOnRelease) {
        TMyEnvProfiles env;

        // Hold dynamic data on task extend. Merge two small tasks into one medium.
        env.CheckMemoryRequest(
                           {{20 << 10, {"small_transaction"}},
                            // 20KB is captured on ReleaseTxData -> small task is requested.
                            {30 << 10, {"small_transaction"}},
                            {false, true, 0, {"", "", "medium_transaction", 1}}},
                            { false /* static */, true /* dynamic */ });
    }

    Y_UNIT_TEST(TestUpdateConfig) {
        TMyEnvProfiles env;

        // Request dynamic memory (large task).
        env.CheckMemoryRequest({{110 << 10, {"large_transaction"}}});

        // Gen new profile with no required profile. EvUpdateConfig should cause
        // default profile to be chosen then.
        env.SetupProfile("profile2");
        env.SendAsync(new TEvTablet::TEvUpdateConfig(env->GetAppData().ResourceProfiles));

        // Profile name in schema is unmodified.
        env.SendSync(new NFake::TEvExecute{ new TTxCheckResourceProfile("profile1") });

        // But default profile is actually used now.
        env.CheckMemoryRequest({{200 << 20, {}, true}});
    }
}

Y_UNIT_TEST_SUITE(TFlatTableExecutorSliceOverlapScan) {

    Y_UNIT_TEST(TestSliceOverlapScan) {
        TMyEnvBase env;
        TRowsModel rows;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;
        policy->InMemForceSizeToSnapshot = 4 * 1024 * 1024;
        policy->Generations.push_back({0, 2, 2, ui64(-1),
            NLocalDb::LegacyQueueIdToTaskName(1), false});
        {
            // Don't allow any extras during compaction
            auto &gen = policy->Generations.back();
            gen.ExtraCompactionMinSize = 0;
            gen.ExtraCompactionPercent = 0;
            gen.ExtraCompactionExpPercent = 0;
            gen.ExtraCompactionExpMaxSize = 0;
        }

        env.SendSync(rows.MakeScheme(std::move(policy)));
        // Insert ~32MB of data, this will generate ~4x8MB slices
        env.SendSync(rows.RowTo(1000000).MakeRows(32768, 1024, 32768));
        env.WaitFor<NFake::TEvCompacted>();
        // Insert 32MB more data, ~4x8MB slices will half-overlap
        env.SendSync(rows.RowTo(1000000 - 16384).MakeRows(32768, 1024, 32768));
        // Don't do anything, this new part will trigger final compaction
        env.WaitFor<NFake::TEvCompacted>();
        // If we didn't crash, then traced scan worked correctly
        env.WaitFor<NFake::TEvCompacted>();
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorColumnGroups) {

    struct TTxSelectRows : public ITransaction {

        TTxSelectRows(ui64 fromKey, ui64 toKey)
            : FromKey(fromKey)
            , ToKey(toKey)
        { }

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            const auto fromKey = NScheme::TInt64::TInstance(FromKey);
            const auto toKey = NScheme::TInt64::TInstance(ToKey);
            TVector<TRawTypeValue> fromKeys{ { fromKey } };
            TVector<TRawTypeValue> toKeys{ { toKey } };

            NTable::TKeyRange keyRange;
            keyRange.MinKey = fromKeys;
            keyRange.MaxKey = toKeys;

            TVector<NTable::TTag> tags{ { TRowsModel::ColumnKeyId, TRowsModel::ColumnValueId } };

            if (!txc.DB.Precharge(TRowsModel::TableId, keyRange.MinKey, keyRange.MaxKey, tags, 0, 0, 0)) {
                return false;
            }

            auto it = txc.DB.IterateRange(TRowsModel::TableId, keyRange, tags);
            ui64 next = FromKey;
            ui64 last = Max<ui64>();
            for (;;) {
                auto ready = it->Next(NTable::ENext::Data);
                if (ready == NTable::EReady::Gone) {
                    break;
                }
                if (ready == NTable::EReady::Page) {
                    return false;
                }

                ui64 key = it->Row().Get(0).AsValue<ui64>();
                Y_ABORT_UNLESS(key == next,
                    "Found key %" PRIu64 ", expected %" PRIu64, key, next);

                ++next;
                last = key;
            }

            Y_ABORT_UNLESS(last == ToKey,
                "Last key %" PRIu64 ", expected %" PRIu64, last, ToKey);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        const ui64 FromKey;
        const ui64 ToKey;
    };

    Y_UNIT_TEST(TestManyRows) {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy();
        policy->InMemSizeToSnapshot = 40 * 1024 *1024;
        policy->InMemStepsToSnapshot = 10;
        policy->InMemForceStepsToSnapshot = 10;
        policy->InMemForceSizeToSnapshot = 64 * 1024 * 1024;
        policy->InMemResourceBrokerTask = NLocalDb::LegacyQueueIdToTaskName(0);
        policy->ReadAheadHiThreshold = 100000;
        policy->ReadAheadLoThreshold = 50000;
        policy->Generations.push_back({100 * 1024 * 1024, 2, 2, 200 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), true});
        policy->Generations.push_back({400 * 1024 * 1024, 2, 2, 800 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(2), false});
        for (auto& gen : policy->Generations) {
            gen.ExtraCompactionPercent = 0;
            gen.ExtraCompactionMinSize = 0;
            gen.ExtraCompactionExpPercent = 0;
            gen.ExtraCompactionExpMaxSize = 0;
            gen.UpliftPartSize = 0;
        }

        env.SendSync(rows.MakeScheme(policy, /* groups */ true));

        // Insert 2000 rows, this should force multiple compaction rounds
        env.SendSync(rows.RowTo(1000000).MakeRows(2000, 1024));

        // Wait for some compactions to complete
        env.WaitFor<NFake::TEvCompacted>(4);

        // Query some range and test for correctness
        env.SendSync(new NFake::TEvExecute{ new TTxSelectRows(1000015, 1001015) });

        // If we didn't crash, then the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorCachePressure) {

    struct TTxSmallCacheSize : public ITransaction {
        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            txc.DB.Alter()
                .SetExecutorCacheSize(30 * 1024);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxFullScan : public ITransaction {
        int& Attempts;

        TTxFullScan(int& attempts)
            : Attempts(attempts)
        { }

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            ++Attempts;

            if (!txc.DB.Precharge(TRowsModel::TableId, { }, { }, { }, 0, -1, -1))
                return false;

            auto iter = txc.DB.IterateRange(TRowsModel::TableId, { }, { });
            while (iter->Next(NTable::ENext::Data) == NTable::EReady::Data) {
                // iterate over all rows
            }

            return iter->Last() != NTable::EReady::Page;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    Y_UNIT_TEST(TestNotEnoughLocalCache) {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;
        policy->InMemForceSizeToSnapshot = 256 * 1024;

        env.SendSync(rows.MakeScheme(std::move(policy)));
        env.SendSync(new NFake::TEvExecute{ new TTxSmallCacheSize });

        // Insert ~512KB of data, this will be compacted and added to local
        // cache, however there would not be enough room and it would spill
        // to shared cache.
        env.SendSync(rows.RowTo(1000000).MakeRows(512, 1024, 512));
        env.WaitFor<NFake::TEvCompacted>();

        // Try a read transaction, it must complete without page faults
        int attempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(attempts) });
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorCompressedSelectRows) {

    struct TTxUpdateSchema : public ITransaction {
        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            using namespace NTable::NPage;

            txc.DB.Alter()
                .SetFamily(TRowsModel::TableId, 0, ECache::None, ECodec::LZ4);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxSelectRows : public ITransaction {
        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            i64 keyId;
            TVector<NTable::TTag> tags;
            tags.push_back(TRowsModel::ColumnValueId);
            TVector<TRawTypeValue> key;
            key.emplace_back(&keyId, sizeof(keyId), NScheme::TTypeInfo(NScheme::TInt64::TypeId));

            for (keyId = 1000000; keyId < 1000512; ++keyId) {
                NTable::TRowState row;
                auto ready = txc.DB.Select(TRowsModel::TableId, key, tags, row);
                Y_ABORT_UNLESS(ready == NTable::EReady::Data);
                Y_ABORT_UNLESS(row.GetRowState() == NTable::ERowOp::Upsert);
                TStringBuf selected = row.Get(0).AsBuf();
                Y_ABORT_UNLESS(selected.size() == 1024);
                TString expected(1024, (char)keyId);
                Y_ABORT_UNLESS(selected == expected);
            }

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    Y_UNIT_TEST(TestCompressedSelectRows) {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;
        policy->InMemForceSizeToSnapshot = 256 * 1024;

        env.SendSync(rows.MakeScheme(std::move(policy)));
        env.SendSync(new NFake::TEvExecute{ new TTxUpdateSchema });

        // Insert ~512KB of data, which will be immediately compacted
        env.SendSync(rows.RowTo(1000000).MakeRows(512, 1024, 512));
        env.WaitFor<NFake::TEvCompacted>();

        // Select all added rows one-by-one
        env.SendSync(new NFake::TEvExecute{ new TTxSelectRows });

        // If we didn't crash, the the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorVersionedRows) {

    using NTable::EReady;

    enum class EVariant {
        Normal,
        SmallBlobs,
        LargeBlobs,
    };

    struct TTxUpdateSchema : public ITransaction {
        const EVariant Variant;

        TTxUpdateSchema(EVariant variant)
            : Variant(variant)
        { }

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            using namespace NTable::NPage;

            switch (Variant) {
                case EVariant::Normal:
                    break;
                case EVariant::SmallBlobs:
                    // Rows bigger than 128 bytes stored in small blobs
                    txc.DB.Alter()
                        .SetFamilyBlobs(TRowsModel::TableId, 0, 128, -1);
                    break;
                case EVariant::LargeBlobs:
                    // Rows bigger than 128 bytes stored in large blobs
                    txc.DB.Alter()
                        .SetFamilyBlobs(TRowsModel::TableId, 0, -1, 128);
                    break;
            }

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxGenerateRows : public ITransaction {
        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            i64 base;

            // Insert 512KB of rows in 1/10
            base = 1000000;
            for (i64 key = base; key < base + 512; ++key) {
                const auto keyValue = NScheme::TInt64::TInstance(key);

                TString str(1024, (char)key);
                const auto valValue = NScheme::TString::TInstance(str);

                NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, valValue };

                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { keyValue }, { ops }, TRowVersion(1, 10));
            }

            // Erase all those rows in 2/20
            for (i64 key = base; key < base + 512; ++key) {
                const auto keyValue = NScheme::TInt64::TInstance(key);

                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Erase, { keyValue }, { }, TRowVersion(2, 20));
            }

            // Insert 512KB more rows in 3/30
            base = 2000000;
            for (i64 key = base; key < base + 512; ++key) {
                const auto keyValue = NScheme::TInt64::TInstance(key);

                TString str(1024, (char)key);
                const auto valValue = NScheme::TString::TInstance(str);

                NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, valValue };

                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { keyValue }, { ops }, TRowVersion(3, 30));
            }

            // Resurrect rows in 4/40
            base = 1000000;
            for (i64 key = base; key < base + 512; ++key) {
                const auto keyValue = NScheme::TInt64::TInstance(key);

                TString str(1024, (char)key);
                const auto valValue = NScheme::TString::TInstance(str);

                NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, valValue };

                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { keyValue }, { ops }, TRowVersion(4, 40));
            }

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxGenerateMoreRows : public ITransaction {
        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            i64 base;

            // Insert 512KB of rows in 5/50
            base = 3000000;
            for (i64 key = base; key < base + 512; ++key) {
                const auto keyValue = NScheme::TInt64::TInstance(key);

                TString str(1024, (char)key);
                const auto valValue = NScheme::TString::TInstance(str);

                NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, valValue };

                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { keyValue }, { ops }, TRowVersion(5, 50));
            }

            // Erase 1m+x rows in 6/60
            base = 1000000;
            for (i64 key = base; key < base + 512; ++key) {
                const auto keyValue = NScheme::TInt64::TInstance(key);

                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Erase, { keyValue }, { }, TRowVersion(6, 60));
            }

            // Erase 2m+x rows in 6/60
            base = 2000000;
            for (i64 key = base; key < base + 512; ++key) {
                const auto keyValue = NScheme::TInt64::TInstance(key);

                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Erase, { keyValue }, { }, TRowVersion(6, 60));
            }

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxVerifyRows : public ITransaction {
        const TRowVersion Snapshot;
        const TVector<std::pair<i64, i64>> KeysRanges;

        TTxVerifyRows(const TRowVersion& snapshot, const TVector<std::pair<i64, i64>>& keysRanges)
            : Snapshot(snapshot)
            , KeysRanges(keysRanges)
        { }

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            TVector<NTable::TTag> tags;
            tags.push_back(TRowsModel::ColumnKeyId);
            tags.push_back(TRowsModel::ColumnValueId);

            auto it = txc.DB.IterateRange(TRowsModel::TableId, { }, tags, Snapshot);

            auto rit = KeysRanges.begin();
            i64 expectedKey = rit != KeysRanges.end() ? rit->first : -1;
            for (;;) {
                auto ready = it->Next(NTable::ENext::Data);
                if (ready == EReady::Page) {
                    return false;
                }

                if (ready == EReady::Gone) {
                    break;
                }

                const auto& row = it->Row();

                i64 key = row.Get(0).AsValue<i64>();
                Y_VERIFY_S(key == expectedKey,
                    "Found key " << key << ", expected " << expectedKey << " at snapshot " << Snapshot);

                TStringBuf selected = row.Get(1).AsBuf();
                Y_ABORT_UNLESS(selected.size() == 1024);
                TString expected(1024, (char)key);
                Y_ABORT_UNLESS(selected == expected);

                Y_DEBUG_ABORT_UNLESS(rit != KeysRanges.end());
                if (++expectedKey == rit->second) {
                    expectedKey = ++rit != KeysRanges.end() ? rit->first : -1;
                }
            }

            Y_VERIFY_S(expectedKey == -1,
                "Expected key " << expectedKey << " not found at snapshot " << Snapshot);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxRemoveRowVersions : public ITransaction {
        const TRowVersion Lower;
        const TRowVersion Upper;

        TTxRemoveRowVersions(const TRowVersion& lower, const TRowVersion& upper)
            : Lower(lower)
            , Upper(upper)
        { }

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            txc.DB.RemoveRowVersions(TRowsModel::TableId, Lower, Upper);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    void DoVersionedRows(EVariant variant)
    {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;
        policy->InMemForceSizeToSnapshot = 256 * 1024;

        env.SendSync(rows.MakeScheme(std::move(policy)));
        env.SendSync(new NFake::TEvExecute{ new TTxUpdateSchema(variant) });

        // Insert some rows, which will be immediately compacted
        env.SendSync(new NFake::TEvExecute{ new TTxGenerateRows });
        env.WaitFor<NFake::TEvCompacted>();

        // Verify max versions first
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion::Max(), { { 1000000, 1000512 }, { 2000000, 2000512 } }) });
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(4, 40), { { 1000000, 1000512 }, { 2000000, 2000512 } }) });

        // Verify versions where second batch of rows is inserted
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(4, 39), { { 2000000, 2000512 } }) });
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(3, 30), { { 2000000, 2000512 } }) });

        // Verify versions where everything is erased
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(3, 29), { }) });
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(2, 20), { }) });

        // Verify versions where first batch of rows is inserted
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(2, 19), { { 1000000, 1000512 } }) });
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(1, 10), { { 1000000, 1000512 } }) });

        // Verify versions where nothing existed yet
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(1, 9), { }) });

        // Insert some more rows, which will be immediately compacted
        env.SendSync(new NFake::TEvExecute{ new TTxGenerateMoreRows });
        env.WaitFor<NFake::TEvCompacted>();

        // Force a full compaction of a table
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // Verify max versions first
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion::Max(), { { 3000000, 3000512 } }) });
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(6, 60), { { 3000000, 3000512 } }) });

        // Verify point where all rows exist
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(6, 59), { { 1000000, 1000512 }, { 2000000, 2000512 }, { 3000000, 3000512 } }) });

        // Verify an older point again (recompacted)
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(2, 19), { { 1000000, 1000512 } }) });
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(1, 10), { { 1000000, 1000512 } }) });

        // Remove all row versions except the most recent one
        env.SendSync(new NFake::TEvExecute{ new TTxRemoveRowVersions(
            TRowVersion(0, 50), TRowVersion(6, 60)) });

        // Force a full compaction so old versions are actually deleted
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // Verify max versions first
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion::Max(), { { 3000000, 3000512 } }) });
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(6, 60), { { 3000000, 3000512 } }) });

        // Verify the lowest point in our deleted range
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(0, 50), { { 3000000, 3000512 } }) });

        // Verify that below that we don't have any data
        env.SendSync(new NFake::TEvExecute{ new TTxVerifyRows(
            TRowVersion(0, 49), { }) });

        // If we didn't crash, the the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(TestVersionedRows) {
        DoVersionedRows(EVariant::Normal);
    }

    Y_UNIT_TEST(TestVersionedRowsSmallBlobs) {
        DoVersionedRows(EVariant::SmallBlobs);
    }

    Y_UNIT_TEST(TestVersionedRowsLargeBlobs) {
        DoVersionedRows(EVariant::LargeBlobs);
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorVersionedLargeBlobs) {

    using NTable::EReady;

    enum : ui32 {
        TableId = 101,
        KeyColumnId = 1,
        ValueColumnId = 2,
        CounterColumnId = 3,
    };

    struct TTxInitSchema : public ITransaction {
        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            using namespace NTable::NPage;

            TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;
            policy->InMemForceSizeToSnapshot = 256 * 1024;

            // Values bigger than 128 bytes stored in large blobs
            txc.DB.Alter()
                .AddTable("test", TableId)
                .AddColumn(TableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false)
                .AddColumn(TableId, "value", ValueColumnId, NScheme::TString::TypeId, false)
                .AddColumn(TableId, "counter", CounterColumnId, NScheme::TInt64::TypeId, false)
                .AddColumnToKey(TableId, KeyColumnId)
                .SetCompactionPolicy(TableId, *policy)
                .SetFamilyBlobs(TableId, 0, -1, 128);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxAddRows : public ITransaction {

        TTxAddRows(ui64 key, ui64 counter, ui64 rows, ui32 bytes, TRowVersion writeVersion)
            : Key(key), Counter(counter), Rows(rows), Bytes(bytes), WriteVersion(writeVersion)
        {}

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            for (; Rows > 0; Key++, Rows--) {
                const auto key = NScheme::TInt64::TInstance(Key);

                TString str = Bytes ? TString(Bytes, (char)Key) : "value";
                const auto val = NScheme::TString::TInstance(str);
                const auto counter = NScheme::TInt64::TInstance(Counter);
                NTable::TUpdateOp ops[2] = {
                    { ValueColumnId, NTable::ECellOp::Set, val },
                    { CounterColumnId, NTable::ECellOp::Set, counter },
                };

                txc.DB.Update(TableId, NTable::ERowOp::Upsert, { key }, ops, WriteVersion);
            }

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        ui64 Key = 0;
        ui64 Counter = 0;
        ui64 Rows = 0;
        const ui32 Bytes;
        const TRowVersion WriteVersion;
    };

    struct TTxUpdateCounter : public ITransaction {

        TTxUpdateCounter(ui64 key, ui64 counter, ui64 rows, TRowVersion writeVersion)
            : Key(key), Counter(counter), Rows(rows), WriteVersion(writeVersion)
        {}

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            for (; Rows > 0; Key++, Rows--) {
                const auto key = NScheme::TInt64::TInstance(Key);
                const auto counter = NScheme::TInt64::TInstance(Counter);
                NTable::TUpdateOp ops[1] = {
                    { CounterColumnId, NTable::ECellOp::Set, counter },
                };

                txc.DB.Update(TableId, NTable::ERowOp::Upsert, { key }, ops, WriteVersion);
            }

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        ui64 Key = 0;
        ui64 Counter = 0;
        ui64 Rows = 0;
        const TRowVersion WriteVersion;
    };

    Y_UNIT_TEST(TestMultiVersionCompactionLargeBlobs) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

        // Add some initial rows
        env.SendSync(new NFake::TEvExecute{ new TTxAddRows(1, 0, 5, 256, TRowVersion(1, 1)) });

        // Force a full compaction of a table
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // Update a counter for some rows
        env.SendSync(new NFake::TEvExecute{ new TTxUpdateCounter(2, 1, 1, TRowVersion(2, 1)) });
        env.SendSync(new NFake::TEvExecute{ new TTxUpdateCounter(4, 1, 1, TRowVersion(3, 1)) });

        // Force a full compaction of a table
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // Rewrite some initial rows
        env.SendSync(new NFake::TEvExecute{ new TTxAddRows(2, 2, 3, 256, TRowVersion(4, 1)) });

        // Force a full compaction of a table
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // The bug is fixed if we didn't crash
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorKeepEraseMarkers) {

    enum class ESchemaVariant {
        KeepEraseMarkers,
        DoNotKeepEraseMarkers,
    };

    struct TTxUpdateSchema : public ITransaction {
        const ESchemaVariant Variant;

        TTxUpdateSchema(ESchemaVariant variant)
            : Variant(variant)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            using namespace NTable::NPage;

            const auto& scheme = txc.DB.GetScheme();

            TCompactionPolicy policy = *scheme.GetTableInfo(TRowsModel::TableId)->CompactionPolicy;

            switch (Variant) {
                case ESchemaVariant::KeepEraseMarkers:
                    policy.KeepEraseMarkers = true;
                    break;
                case ESchemaVariant::DoNotKeepEraseMarkers:
                    policy.KeepEraseMarkers = false;
                    break;
            }

            txc.DB.Alter().SetCompactionPolicy(TRowsModel::TableId, policy);

            return true;
        }

        void Complete(const TActorContext &ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxGenerateRows : public ITransaction {
        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            {
                i64 key = 100;
                const auto keyValue = NScheme::TInt64::TInstance(key);

                TString str = "key100value";
                const auto valValue = NScheme::TString::TInstance(str);

                NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, valValue };

                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { keyValue }, { ops });
            }

            {
                i64 key = 200;
                const auto keyValue = NScheme::TInt64::TInstance(key);

                TString str = "key200value";
                const auto valValue = NScheme::TString::TInstance(str);

                NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, valValue };

                // Erase, then update
                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Erase, { keyValue }, { });
                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { keyValue }, { ops });
            }

            {
                i64 key = 300;
                const auto keyValue = NScheme::TInt64::TInstance(key);

                TString str = "key300value";
                const auto valValue = NScheme::TString::TInstance(str);

                NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, valValue };

                // Update, then erase
                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { keyValue }, { ops });
                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Erase, { keyValue }, { });
            }

            {
                i64 key = 400;
                const auto keyValue = NScheme::TInt64::TInstance(key);

                // Only erase
                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Erase, { keyValue }, { });
            }

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxCheckRows : public ITransaction {
        TString& Data;

        TTxCheckRows(TString& data)
            : Data(data)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TStringBuilder builder;

            i64 keyId;
            TVector<NTable::TTag> tags;
            tags.push_back(TRowsModel::ColumnValueId);
            TVector<TRawTypeValue> key;
            key.emplace_back(&keyId, sizeof(keyId), NScheme::TTypeInfo(NScheme::TInt64::TypeId));

            for (keyId = 100; keyId <= 400; keyId += 100) {
                NTable::TRowState row;
                auto ready = txc.DB.Select(TRowsModel::TableId, key, tags, row);
                if (ready == NTable::EReady::Page) {
                    return false;
                }

                TString value;
                DbgPrintValue(value, row.Get(0), NScheme::TTypeInfo(NScheme::TString::TypeId));

                builder << "Key " << keyId << " = " << row.GetRowState()
                    << " value = " << NTable::ECellOp(row.GetCellOp(0)) << " " << value << Endl;
            }

            Data = builder;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    Y_UNIT_TEST(TestKeepEraseMarkers) {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;

        env.SendSync(rows.MakeScheme(std::move(policy)));
        env.SendSync(new NFake::TEvExecute{ new TTxUpdateSchema(ESchemaVariant::KeepEraseMarkers) });

        // Insert some test rows
        env.SendSync(new NFake::TEvExecute{ new TTxGenerateRows });

        // Test initially data is as expected
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 100 = Upsert value = Set key100value\n"
                "Key 200 = Reset value = Set key200value\n"
                "Key 300 = Erase value = Empty NULL\n"
                "Key 400 = Erase value = Empty NULL\n");
        }

        // Force a full compaction of a table
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // Test data is as expected after compaction
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 100 = Upsert value = Set key100value\n"
                "Key 200 = Reset value = Set key200value\n"
                "Key 300 = Erase value = Empty NULL\n"
                "Key 400 = Erase value = Empty NULL\n");
        }

        // Update schema and recompact
        env.SendSync(new NFake::TEvExecute{ new TTxUpdateSchema(ESchemaVariant::DoNotKeepEraseMarkers) });
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // Test data is as expected after compaction
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 100 = Upsert value = Set key100value\n"
                "Key 200 = Reset value = Set key200value\n"
                "Key 300 = Absent value = Empty NULL\n"
                "Key 400 = Absent value = Empty NULL\n");
        }
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorMoveTableData) {

    struct TTxInitSchema : public ITransaction {
        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TCompactionPolicy policy;

            policy.KeepEraseMarkers = true;
            CreateTable(txc, TRowsModel::TableId + 0, policy);

            policy.KeepEraseMarkers = false;
            CreateTable(txc, TRowsModel::TableId + 1, policy);

            return true;
        }

        void CreateTable(TTransactionContext& txc, ui32 tableId, const TCompactionPolicy& policy) {
            if (txc.DB.GetScheme().GetTableInfo(tableId))
                return;

            txc.DB.Alter()
                .AddTable("test" + ToString(ui32(tableId)), tableId)
                .AddColumn(tableId, "key", TRowsModel::ColumnKeyId, NScheme::TInt64::TypeId, false)
                .AddColumn(tableId, "value", TRowsModel::ColumnValueId, NScheme::TString::TypeId, false)
                .AddColumnToKey(tableId, TRowsModel::ColumnKeyId)
                .SetCompactionPolicy(tableId, policy);
        }

        void Complete(const TActorContext &ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxGenerateRows : public ITransaction {
        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            // Main table, update and erase some keys
            for (i64 key = 1; key <= 10; key += 3) {
                const bool makeUpdate = (key % 2) == 1;
                const auto keyValue = NScheme::TInt64::TInstance(key);

                if (makeUpdate) {
                    TString str = TStringBuilder() << "key" << key << "update";
                    const auto valValue = NScheme::TString::TInstance(str);

                    NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, valValue };

                    txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { keyValue }, { ops });
                } else {
                    txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Erase, { keyValue }, { });
                }
            }

            // Shadow table, generate some keys and values
            for (i64 key = 1; key <= 10; ++key) {
                const auto keyValue = NScheme::TInt64::TInstance(key);

                TString str = TStringBuilder() << "key" << key << "base";
                const auto valValue = NScheme::TString::TInstance(str);

                NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, valValue };

                txc.DB.Update(TRowsModel::TableId + 1, NTable::ERowOp::Upsert, { keyValue }, { ops });
            }

            return true;
        }

        void Complete(const TActorContext &ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    class TSnapshotMoveContext : public NFake::TDummySnapshotContext {
    public:
        static TIntrusivePtr<TSnapshotMoveContext> Create(ui32 src, ui32 dst) {
            return new TSnapshotMoveContext(src, dst);
        }

        NFake::TEvExecute* Start() {
            return new NFake::TEvExecute{ new TTxPrepare(this) };
        }

    private:
        TSnapshotMoveContext(ui32 src, ui32 dst)
            : SourceTableId(src)
            , TargetTableId(dst)
        { }

        TConstArrayRef<ui32> TablesToSnapshot() const override {
            return { &SourceTableId, 1 };
        }

        NFake::TEvExecute* OnFinished() override {
            return new NFake::TEvExecute{ new TTxFinished(this) };
        }

    private:
        struct TTxPrepare : public ITransaction {
            TTxPrepare(TSnapshotMoveContext* self)
                : Self(self)
            { }

            bool Execute(TTransactionContext& txc, const TActorContext&) override
            {
                txc.Env.MakeSnapshot(Self);
                return true;
            }

            void Complete(const TActorContext&) override
            {
                // nothing
            }

        private:
            TSnapshotMoveContext* Self;
        };

        struct TTxFinished : public ITransaction {
            TTxFinished(TSnapshotMoveContext* self)
                : Self(self)
            { }

            bool Execute(TTransactionContext& txc, const TActorContext&) override
            {
                const auto& scheme = txc.DB.GetScheme();
                const ui32 src = Self->SourceTableId;
                const ui32 dst = Self->TargetTableId;

                // Alter destination and stop keeping erase markers
                {
                    TCompactionPolicy policy = *scheme.GetTableInfo(dst)->CompactionPolicy;
                    Y_ABORT_UNLESS(policy.KeepEraseMarkers);
                    policy.KeepEraseMarkers = false;
                    txc.DB.Alter().SetCompactionPolicy(dst, policy);
                }

                // Move and drop the snapshot
                txc.Env.MoveSnapshot(*Self, src, dst);
                txc.Env.DropSnapshot(Self);

                return true;
            }

            void Complete(const TActorContext &ctx) override
            {
                ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            }

        private:
            TSnapshotMoveContext* Self;
        };

    private:
        const ui32 SourceTableId;
        const ui32 TargetTableId;
    };

    struct TTxCheckRows : public ITransaction {
        TString& Data;

        TTxCheckRows(TString& data)
            : Data(data)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TStringBuilder builder;

            TVector<NTable::TTag> tags;
            tags.push_back(TRowsModel::ColumnKeyId);
            tags.push_back(TRowsModel::ColumnValueId);

            NTable::EReady ready;
            auto it = txc.DB.IterateRange(TRowsModel::TableId, { }, tags);

            while ((ready = it->Next(NTable::ENext::All)) != NTable::EReady::Gone) {
                if (ready == NTable::EReady::Page) {
                    return false;
                }

                const auto& row = it->Row();

                TString key;
                DbgPrintValue(key, row.Get(0), NScheme::TTypeInfo(NScheme::TUint64::TypeId));

                TString value;
                DbgPrintValue(value, row.Get(1), NScheme::TTypeInfo(NScheme::TString::TypeId));

                builder << "Key " << key << " = " << row.GetRowState()
                    << " value = " << NTable::ECellOp(row.GetCellOp(1)) << " " << value << Endl;
            }

            Data = builder;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    Y_UNIT_TEST(TestMoveSnapshot) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::TABLET_FLATBOOT, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });
        env.SendSync(new NFake::TEvExecute{ new TTxGenerateRows });

        if (false) {
            // Force full compaction of the main table
            env.SendSync(new NFake::TEvCompact(TRowsModel::TableId + 0));
            env.WaitFor<NFake::TEvCompacted>();

            // Force full compaction of the shadow table
            env.SendSync(new NFake::TEvCompact(TRowsModel::TableId + 1));
            env.WaitFor<NFake::TEvCompacted>();
        }

        // Move data from shadow to main table
        auto move = TSnapshotMoveContext::Create(TRowsModel::TableId + 1, TRowsModel::TableId + 0);
        env.SendSync(move->Start());

        // Check resulting data
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set key1update\n"
                "Key 2 = Upsert value = Set key2base\n"
                "Key 3 = Upsert value = Set key3base\n"
                "Key 4 = Erase value = Empty NULL\n"
                "Key 5 = Upsert value = Set key5base\n"
                "Key 6 = Upsert value = Set key6base\n"
                "Key 7 = Upsert value = Set key7update\n"
                "Key 8 = Upsert value = Set key8base\n"
                "Key 9 = Upsert value = Set key9base\n"
                "Key 10 = Erase value = Empty NULL\n");
        }

        // Restart tablet so it can boot with moved data
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // Check data after restart
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) }, /* retry = */ true);
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set key1update\n"
                "Key 2 = Upsert value = Set key2base\n"
                "Key 3 = Upsert value = Set key3base\n"
                "Key 4 = Erase value = Empty NULL\n"
                "Key 5 = Upsert value = Set key5base\n"
                "Key 6 = Upsert value = Set key6base\n"
                "Key 7 = Upsert value = Set key7update\n"
                "Key 8 = Upsert value = Set key8base\n"
                "Key 9 = Upsert value = Set key9base\n"
                "Key 10 = Erase value = Empty NULL\n");
        }

        // Force full compaction of the main table
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // Check resulting data
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set key1update\n"
                "Key 2 = Upsert value = Set key2base\n"
                "Key 3 = Upsert value = Set key3base\n"
                "Key 5 = Upsert value = Set key5base\n"
                "Key 6 = Upsert value = Set key6base\n"
                "Key 7 = Upsert value = Set key7update\n"
                "Key 8 = Upsert value = Set key8base\n"
                "Key 9 = Upsert value = Set key9base\n");
        }

        // Restart tablet one more time
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // Check resulting data after restart
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) }, /* retry = */ true);
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set key1update\n"
                "Key 2 = Upsert value = Set key2base\n"
                "Key 3 = Upsert value = Set key3base\n"
                "Key 5 = Upsert value = Set key5base\n"
                "Key 6 = Upsert value = Set key6base\n"
                "Key 7 = Upsert value = Set key7update\n"
                "Key 8 = Upsert value = Set key8base\n"
                "Key 9 = Upsert value = Set key9base\n");
        }
    }

    struct TTxDropTable : public ITransaction {
        const ui32 TableId;

        explicit TTxDropTable(ui32 tableId)
            : TableId(tableId)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            txc.DB.Alter().DropTable(TableId);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxCheckStep : public ITransaction {
        ui32& Step;

        explicit TTxCheckStep(ui32& step)
            : Step(step)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            Step = txc.Step;
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }
    };

    Y_UNIT_TEST(TestMoveSnapshotFollower) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::TABLET_FLATBOOT, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        env.FireDummyFollower(1);

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });
        env.SendSync(new NFake::TEvExecute{ new TTxGenerateRows });

        // Move data from shadow to main table and drop shadow table
        auto move = TSnapshotMoveContext::Create(TRowsModel::TableId + 1, TRowsModel::TableId + 0);
        env.SendSync(move->Start());
        env.SendSync(new NFake::TEvExecute{ new TTxDropTable(TRowsModel::TableId + 1) });

        // Find the last completed leader step
        ui32 leaderStep;
        env.SendSync(new NFake::TEvExecute{ new TTxCheckStep(leaderStep) });
        --leaderStep;

        // Wait for that step to be synced to follower
        while (true) {
            ui32 followerStep;
            env.SendFollowerSync(new NFake::TEvExecute{ new TTxCheckStep(followerStep) });
            if (followerStep >= leaderStep) {
                break;
            }
        }

        // Check resulting data on a follower
        {
            TString data;
            env.SendFollowerSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set key1update\n"
                "Key 2 = Upsert value = Set key2base\n"
                "Key 3 = Upsert value = Set key3base\n"
                "Key 4 = Erase value = Empty NULL\n"
                "Key 5 = Upsert value = Set key5base\n"
                "Key 6 = Upsert value = Set key6base\n"
                "Key 7 = Upsert value = Set key7update\n"
                "Key 8 = Upsert value = Set key8base\n"
                "Key 9 = Upsert value = Set key9base\n"
                "Key 10 = Erase value = Empty NULL\n");
        }

        // Force full compaction of the main table
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // Find the last completed leader step
        env.SendSync(new NFake::TEvExecute{ new TTxCheckStep(leaderStep) });
        --leaderStep;

        // Wait for that step to be synced to follower
        while (true) {
            ui32 followerStep;
            env.SendFollowerSync(new NFake::TEvExecute{ new TTxCheckStep(followerStep) });
            if (followerStep >= leaderStep) {
                break;
            }
        }

        // Check resulting data on a follower
        {
            TString data;
            env.SendFollowerSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set key1update\n"
                "Key 2 = Upsert value = Set key2base\n"
                "Key 3 = Upsert value = Set key3base\n"
                "Key 5 = Upsert value = Set key5base\n"
                "Key 6 = Upsert value = Set key6base\n"
                "Key 7 = Upsert value = Set key7update\n"
                "Key 8 = Upsert value = Set key8base\n"
                "Key 9 = Upsert value = Set key9base\n");
        }
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorFollower) {

    struct TTxCheckStep : public ITransaction {
        ui32& Step;

        explicit TTxCheckStep(ui32& step)
            : Step(step)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            Step = txc.Step;
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }
    };

    struct TTxWriteRow : public ITransaction {
        i64 Key;
        ui32& Step;

        explicit TTxWriteRow(i64 key, ui32& step)
            : Key(key)
            , Step(step)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            Step = txc.Step;

            const auto key = NScheme::TInt64::TInstance(Key);

            TString str = TStringBuilder() << "key" << Key << "value";
            const auto val = NScheme::TString::TInstance(str);
            NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, val };

            txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { key }, { ops });
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }
    };

    struct TTxCheckRows : public ITransaction {
        TString& Data;

        TTxCheckRows(TString& data)
            : Data(data)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TStringBuilder builder;

            TVector<NTable::TTag> tags;
            tags.push_back(TRowsModel::ColumnKeyId);
            tags.push_back(TRowsModel::ColumnValueId);

            NTable::EReady ready;
            auto it = txc.DB.IterateRange(TRowsModel::TableId, { }, tags);

            while ((ready = it->Next(NTable::ENext::All)) != NTable::EReady::Gone) {
                if (ready == NTable::EReady::Page) {
                    return false;
                }

                const auto& row = it->Row();

                TString key;
                DbgPrintValue(key, row.Get(0), NScheme::TTypeInfo(NScheme::TUint64::TypeId));

                TString value;
                DbgPrintValue(value, row.Get(1), NScheme::TTypeInfo(NScheme::TString::TypeId));

                builder << "Key " << key << " = " << row.GetRowState()
                    << " value = " << NTable::ECellOp(row.GetCellOp(1)) << " " << value << Endl;
            }

            Data = builder;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    Y_UNIT_TEST(BasicFollowerRead) {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        env.FireDummyFollower(1);

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;

        env.SendSync(rows.MakeScheme(std::move(policy)));

        // Insert some test rows (without waiting for completion)
        ui32 lastLeaderStep = 0;
        for (i64 key = 1; key <= 5; ++key) {
            env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, lastLeaderStep) });
        }

        // Wait for follower to sync with the last leader step
        while (true) {
            ui32 followerStep = 0;
            env.SendFollowerSync(new NFake::TEvExecute{ new TTxCheckStep(followerStep) });
            if (followerStep >= lastLeaderStep) {
                break;
            }
        }

        // Test read from follower
        {
            TString data;
            env.SendFollowerSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set key1value\n"
                "Key 2 = Upsert value = Set key2value\n"
                "Key 3 = Upsert value = Set key3value\n"
                "Key 4 = Upsert value = Set key4value\n"
                "Key 5 = Upsert value = Set key5value\n");
        }
    }

    struct TFollowerEarlyRebootObserver {
        using EEventAction = TTestActorRuntimeBase::EEventAction;

        enum class EState {
            WaitForAttach,
            WaitForSnapshot,
            BlockSnapshotCommit,
            Idle,
            WaitForSecondAttach,
        };

        TActorId Edge;
        bool Debug = false;
        TTestActorRuntimeBase::TEventObserver PrevObserverFunc;

        EState State = EState::WaitForAttach;
        TActorId FollowerTabletActor;
        TActorId LeaderTabletActor;
        ui32 SnapshotStep = 0;
        THolder<IEventHandle> SnapshotCommitResult;
        bool Detached = false;

        void Install(TTestActorRuntimeBase& runtime) {
            PrevObserverFunc = runtime.SetObserverFunc([this, &runtime](auto& ev) {
                return OnEvent(runtime, ev);
            });
        }

        void Uninstall(TTestActorRuntimeBase& runtime) {
            runtime.SetObserverFunc(PrevObserverFunc);
            PrevObserverFunc = { };
        }

        EEventAction OnEvent(
                TTestActorRuntimeBase& ctx,
                TAutoPtr<IEventHandle>& ev)
        {
            switch (ev->GetTypeRewrite()) {
                HFuncCtx(TEvTablet::TEvFollowerAttach, Handle, ctx);
                HFuncCtx(TEvTablet::TEvFollowerDetach, Handle, ctx);
                HFuncCtx(TEvTablet::TEvFollowerUpdate, Handle, ctx);
                HFuncCtx(TEvTablet::TEvCommit, Handle, ctx);
                HFuncCtx(TEvTabletBase::TEvWriteLogResult, Handle, ctx);
                default:
                    return PrevObserverFunc(ev);
            }

            return ev ? EEventAction::PROCESS : EEventAction::DROP;
        }

        void Handle(TEvTablet::TEvFollowerAttach::TPtr& ev, TTestActorRuntimeBase& ctx) {
            if (State == EState::WaitForAttach) {
                if (Debug) {
                    Cerr << "See first follower attach, waiting for snapshot" << Endl;
                }
                // On attach we kill the follower
                FollowerTabletActor = ev->Sender;
                LeaderTabletActor = ev->GetRecipientRewrite();
                // We will be blocking the next snapshot
                State = EState::WaitForSnapshot;
            } else if (State == EState::WaitForSecondAttach) {
                if (Debug) {
                    Cerr << "See second follower attach, unblocking result for first snapshot" << Endl;
                }
                State = EState::Idle;
                UnblockSnapshot(ctx);
            }
        }

        void Handle(TEvTablet::TEvFollowerDetach::TPtr&, TTestActorRuntimeBase& ctx) {
            if (Debug) {
                Cerr << "See follower detach, waking up" << Endl;
            }
            ctx.Send(new IEventHandle(Edge, Edge, new TEvents::TEvWakeup()), 0, true);
            Detached = true;
        }

        void Handle(TEvTablet::TEvFollowerUpdate::TPtr& ev, TTestActorRuntimeBase&) {
            ui32 step = ev->Get()->Record.GetStep();
            bool snapshot = ev->Get()->Record.GetIsSnapshot();
            if (Debug) {
                Cerr << "See follower update, step=" << step << " snapshot=" << snapshot << Endl;
            }
        }

        void Handle(TEvTablet::TEvCommit::TPtr& ev, TTestActorRuntimeBase& ctx) {
            auto* msg = ev->Get();
            if (Debug) {
                Cerr << "See commit for step=" << msg->Step << " IsSnapshot=" << msg->IsSnapshot << Endl;
            }
            if (State == EState::WaitForSnapshot && msg->IsSnapshot) {
                SnapshotStep = msg->Step;
                State = EState::BlockSnapshotCommit;
                // Send poison pill via actor system
                if (Debug) {
                    Cerr << "...killing the follower tablet" << Endl;
                }
                ctx.Send(new IEventHandle(FollowerTabletActor, LeaderTabletActor, new TEvents::TEvPoison()), 0, true);
            }
        }

        void Handle(TEvTabletBase::TEvWriteLogResult::TPtr& ev, TTestActorRuntimeBase&) {
            auto* msg = ev->Get();
            const auto& logid = msg->EntryId;
            if (Debug) {
                Cerr << "See commit result for step=" << logid.Step() << " cookie=" << logid.Cookie() << Endl;
            }
            if (State == EState::BlockSnapshotCommit && logid.Cookie() == 0 && logid.Step() == SnapshotStep) {
                if (Debug) {
                    Cerr << "...blocking commit result for first snapshot" << Endl;
                }
                SnapshotCommitResult.Reset(ev.Release());
                State = EState::Idle;
            }
        }

        void ReadyForSecondAttach() {
            Y_ABORT_UNLESS(State == EState::Idle);
            Y_ABORT_UNLESS(SnapshotCommitResult);
            State = EState::WaitForSecondAttach;
        }

        void UnblockSnapshot(TTestActorRuntimeBase& runtime) {
            Y_ABORT_UNLESS(SnapshotCommitResult);
            runtime.Send(SnapshotCommitResult.Release(), 0, true);
        }
    };

    // Regression test for KIKIMR-7745
    Y_UNIT_TEST(FollowerEarlyRebootHoles) {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;

        env.SendSync(rows.MakeScheme(std::move(policy)));

        TFollowerEarlyRebootObserver observer;
        observer.Edge = env.Edge;
        //observer.Debug = true;
        observer.Install(env.Env);

        // Start follower (it will die almost immediately)
        env.FireDummyFollower(1, false);
        env.WaitForGone();
        env.WaitForWakeUp();

        // Insert some test rows (without waiting for completion)
        ui32 lastLeaderStep = 0;
        for (i64 key = 1; key <= 5; ++key) {
            env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, lastLeaderStep) });
        }

        // Start another follower
        observer.ReadyForSecondAttach();
        env.FireDummyFollower(1);

        // Add one more commit (follower step is only updated on rollup)
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(6, lastLeaderStep) });

        // Wait for follower to sync with the last leader step
        while (true) {
            ui32 followerStep = 0;
            env.SendFollowerSync(new NFake::TEvExecute{ new TTxCheckStep(followerStep) });
            if (followerStep >= lastLeaderStep) {
                break;
            }
        }

        // Test read from follower
        {
            TString data;
            env.SendFollowerSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set key1value\n"
                "Key 2 = Upsert value = Set key2value\n"
                "Key 3 = Upsert value = Set key3value\n"
                "Key 4 = Upsert value = Set key4value\n"
                "Key 5 = Upsert value = Set key5value\n"
                "Key 6 = Upsert value = Set key6value\n");
        }

        observer.Uninstall(env.Env);
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorRejectProbability) {

    struct TTxMultiSchema : public ITransaction {
        TIntrusiveConstPtr<TCompactionPolicy> Policy;

        explicit TTxMultiSchema(const TIntrusiveConstPtr<TCompactionPolicy>& policy)
            : Policy(policy)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            for (ui32 tableId = 201; tableId <= 208; ++tableId) {
                txc.DB.Alter()
                    .AddTable("test" + ToString(ui32(tableId)), tableId)
                    .AddColumn(tableId, "key", TRowsModel::ColumnKeyId, NScheme::TInt64::TypeId, false)
                    .AddColumn(tableId, "value", TRowsModel::ColumnValueId, NScheme::TString::TypeId, false)
                    .AddColumnToKey(tableId, TRowsModel::ColumnKeyId)
                    .SetCompactionPolicy(tableId, *Policy);
            }

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxWrite8MB : public ITransaction {
        i64 Key;
        ui32 TableId;

        explicit TTxWrite8MB(i64 key, ui32 tableId = TRowsModel::TableId)
            : Key(key)
            , TableId(tableId)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            const auto key = NScheme::TInt64::TInstance(Key);

            TString str(size_t(8 * 1024 * 1024), 'x');
            const auto val = NScheme::TString::TInstance(str);
            NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, val };

            txc.DB.Update(TableId, NTable::ERowOp::Upsert, { key }, { ops });
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }
    };

    Y_UNIT_TEST(MaxedOutRejectProbability) {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;

        env.SendSync(rows.MakeScheme(std::move(policy)));

        float rejectProbability;
        env.SendSync(new NFake::TEvCall{ [&](auto* executor, const auto& ctx) {
            // Write 64MB
            for (i64 key = 1; key <= 8; ++key) {
                executor->Execute(new TTxWrite8MB(key), ctx);
            }
            rejectProbability = executor->GetRejectProbability();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        UNIT_ASSERT_C(rejectProbability >= 1.0f, "unexpected reject probability: " << rejectProbability);
    }

    Y_UNIT_TEST(SomeRejectProbability) {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;

        env.SendSync(rows.MakeScheme(std::move(policy)));

        float rejectProbability;
        env.SendSync(new NFake::TEvCall{ [&](auto* executor, const auto& ctx) {
            // Write 48MB
            for (i64 key = 1; key <= 6; ++key) {
                executor->Execute(new TTxWrite8MB(key), ctx);
            }
            rejectProbability = executor->GetRejectProbability();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        UNIT_ASSERT_C(rejectProbability > 0.0f && rejectProbability < 1.0f,
                "unexpected reject probability: " << rejectProbability);
    }

    Y_UNIT_TEST(ZeroRejectProbability) {
        TMyEnvBase env;
        TRowsModel rows;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;

        env.SendSync(rows.MakeScheme(std::move(policy)));

        float rejectProbability;
        env.SendSync(new NFake::TEvCall{ [&](auto* executor, const auto& ctx) {
            // Write 32MB
            for (i64 key = 1; key <= 4; ++key) {
                executor->Execute(new TTxWrite8MB(key), ctx);
            }
            rejectProbability = executor->GetRejectProbability();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        UNIT_ASSERT_C(rejectProbability <= 0.0f,
                "unexpected reject probability: " << rejectProbability);
    }

    Y_UNIT_TEST(ZeroRejectProbabilityMultipleTables) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;

        env.SendSync(new NFake::TEvExecute{ new TTxMultiSchema(policy) });

        float rejectProbability;
        env.SendSync(new NFake::TEvCall{ [&](auto* executor, const auto& ctx) {
            // Write 64MB, but to multiple tables
            for (i64 key = 1; key <= 8; ++key) {
                executor->Execute(new TTxWrite8MB(key, ui32(200 + key)), ctx);
            }
            rejectProbability = executor->GetRejectProbability();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        UNIT_ASSERT_C(rejectProbability <= 0.0f,
                "unexpected reject probability: " << rejectProbability);
    }

}

Y_UNIT_TEST_SUITE(TFlatTableCold) {

    struct TTxDummy : public ITransaction {
        bool Execute(TTransactionContext&, const TActorContext&) override {
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxMakeSnapshot : public ITransaction {
        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TIntrusivePtr<TTableSnapshotContext> snapContext =
                new TTestTableSnapshotContext({TRowsModel::TableId});
            txc.Env.MakeSnapshot(snapContext);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxBorrowSnapshot : public ITransactionWithExecutor {
        TTxBorrowSnapshot(TString& snapBody, TIntrusivePtr<TTableSnapshotContext> snapContext, ui64 targetTabletId)
            : SnapBody(snapBody)
            , SnapContext(std::move(snapContext))
            , TargetTabletId(targetTabletId)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            SnapBody = Executor->BorrowSnapshot(TRowsModel::TableId, *SnapContext, { }, { }, TargetTabletId);
            txc.Env.DropSnapshot(SnapContext);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        TString& SnapBody;
        TIntrusivePtr<TTableSnapshotContext> SnapContext;
        const ui64 TargetTabletId;
    };

    struct TTxInitColdSchema : public ITransaction {
        TTxInitColdSchema(TIntrusiveConstPtr<TCompactionPolicy> policy)
            : Policy(std::move(policy))
        { }

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            if (txc.DB.GetScheme().GetTableInfo(TRowsModel::TableId))
                return true;

            txc.DB.Alter()
                .AddTable("test" + ToString(ui32(TRowsModel::TableId)), TRowsModel::TableId)
                .AddColumn(TRowsModel::TableId, "key", TRowsModel::ColumnKeyId, NScheme::TInt64::TypeId, false)
                .AddColumn(TRowsModel::TableId, "value", TRowsModel::ColumnValueId, NScheme::TString::TypeId, false)
                .AddColumnToKey(TRowsModel::TableId, TRowsModel::ColumnKeyId)
                .SetCompactionPolicy(TRowsModel::TableId, *Policy)
                .SetColdBorrow(TRowsModel::TableId, true);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

        TIntrusiveConstPtr<TCompactionPolicy> Policy;
    };

    struct TTxLoanSnapshot : public ITransaction {
        TTxLoanSnapshot(TString snapBody)
            : SnapBody(snapBody)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            txc.Env.LoanTable(TRowsModel::TableId, SnapBody);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        TString SnapBody;
    };

    struct TTxCheckOnlyColdParts : public ITransaction {
        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            auto parts = txc.DB.GetTableParts(TRowsModel::TableId);
            auto coldParts = txc.DB.GetTableColdParts(TRowsModel::TableId);
            Y_ABORT_UNLESS(parts.empty());
            Y_ABORT_UNLESS(!coldParts.empty());
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxCheckNoColdParts : public ITransaction {
        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            auto coldParts = txc.DB.GetTableColdParts(TRowsModel::TableId);
            Y_ABORT_UNLESS(coldParts.empty());
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    Y_UNIT_TEST(ColdBorrowScan) {
        TMyEnvBase env;
        TRowsModel rows;

        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        // Start the first tablet
        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        // Init schema
        {
            TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy();
            env.SendSync(rows.MakeScheme(std::move(policy)));
        }

        // Insert 100 rows
        Cerr << "...inserting rows" << Endl;
        env.SendSync(rows.MakeRows(100, 0, 100));

        // Force a full compaction of a table
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        Cerr << "...making snapshot" << Endl;
        env.SendSync(new NFake::TEvExecute{ new TTxMakeSnapshot });
        Cerr << "...waiting for snapshot to complete" << Endl;
        auto evSnapshot = env.GrabEdgeEvent<TEvTestFlatTablet::TEvSnapshotComplete>();
        Cerr << "...borrowing snapshot" << Endl;
        TString snapBody;
        env.SendSync(new NFake::TEvExecute{ new TTxBorrowSnapshot(snapBody, evSnapshot->Get()->SnapContext, env.Tablet + 1) });

        // Stop the source tablet
        Cerr << "...stopping the source tablet" << Endl;
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.WaitForGone();

        // Start the destination tablet
        Cerr << "...starting the destination tablet" << Endl;
        ++env.Tablet;
        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        // Init destination schema with a cold table flag enabled
        {
            TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy();
            env.SendSync(new NFake::TEvExecute{ new TTxInitColdSchema(std::move(policy)) });
        }

        // Loan the source table
        Cerr << "...loaning snapshot" << Endl;
        env.SendSync(new NFake::TEvExecute{ new TTxLoanSnapshot(snapBody) });

        Cerr << "...checking table only has cold parts" << Endl;
        env.SendSync(new NFake::TEvExecute{ new TTxCheckOnlyColdParts });

        // We must see all 100 rows
        Cerr << "...starting scan" << Endl;
        env.SendAsync(new TEvTestFlatTablet::TEvQueueScan(rows.Rows()));
        env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();

        for (ui32 attempt = 1; attempt <= 3; ++attempt) {
            // Restart tablet, so cold tables are loaded at boot time
            Cerr << "...restarting tablet, iteration " << attempt << Endl;
            env.SendSync(new TEvents::TEvPoison, false, true);
            env.WaitForGone();
            env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
                return new TTestFlatTablet(env.Edge, tablet, info);
            });
            env.WaitForWakeUp();

            Cerr << "...checking table only has cold parts" << Endl;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckOnlyColdParts }, /* retry */ true);

            // We must see all 100 rows
            Cerr << "...starting scan" << Endl;
            env.SendAsync(new TEvTestFlatTablet::TEvQueueScan(rows.Rows()));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        // Erase 50 rows
        Cerr << "...erasing rows" << Endl;
        env.SendSync(rows.RowTo(51).MakeErase(50, 50));

        // We must see 50 rows
        Cerr << "...starting scan" << Endl;
        env.SendAsync(new TEvTestFlatTablet::TEvQueueScan(50));
        env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();

        // Force a full compaction of a table
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        Cerr << "...checking table has no cold parts" << Endl;
        env.SendSync(new NFake::TEvExecute{ new TTxCheckNoColdParts });

        // We must still see 50 rows
        Cerr << "...starting scan" << Endl;
        env.SendAsync(new TEvTestFlatTablet::TEvQueueScan(50));
        env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();

        Cerr << "...restarting tablet after compaction" << Endl;
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.WaitForGone();
        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        Cerr << "...checking table has no cold parts" << Endl;
        env.SendSync(new NFake::TEvExecute{ new TTxCheckNoColdParts }, /* retry */ true);

        // We must still see 50 rows
        Cerr << "...starting scan" << Endl;
        env.SendAsync(new TEvTestFlatTablet::TEvQueueScan(50));
        env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
    }
}

Y_UNIT_TEST_SUITE(TFlatTableLongTx) {

    enum : ui32  {
        TableId = 101,
        KeyColumnId = 1,
        ValueColumnId = 2,
        Value2ColumnId = 3,
    };

    struct TTxInitSchema : public ITransaction {
        TTxInitSchema(TIntrusiveConstPtr<TCompactionPolicy> policy = nullptr)
            : Policy(std::move(policy))
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            if (txc.DB.GetScheme().GetTableInfo(TableId))
                return true;

            txc.DB.Alter()
                .AddTable("test" + ToString(ui32(TableId)), TableId)
                .AddColumn(TableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false)
                .AddColumn(TableId, "value", ValueColumnId, NScheme::TString::TypeId, false)
                .AddColumn(TableId, "value2", Value2ColumnId, NScheme::TString::TypeId, false)
                .AddColumnToKey(TableId, KeyColumnId);

            if (Policy) {
                txc.DB.Alter().SetCompactionPolicy(TableId, *Policy);
            }

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

        const TIntrusiveConstPtr<TCompactionPolicy> Policy;
    };

    struct TTxCommitLongTx : public ITransaction {
        ui64 TxId;
        TRowVersion RowVersion;

        explicit TTxCommitLongTx(ui64 txId, TRowVersion rowVersion = TRowVersion::Min())
            : TxId(txId)
            , RowVersion(rowVersion)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            txc.DB.CommitTx(TableId, TxId, RowVersion);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }
    };

    template<ui32 ColumnId>
    struct TTxWriteRow : public ITransaction {
        i64 Key;
        TString Value;
        ui64 TxId;

        explicit TTxWriteRow(i64 key, TString value, ui64 txId = 0)
            : Key(key)
            , Value(std::move(value))
            , TxId(txId)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            const auto key = NScheme::TInt64::TInstance(Key);

            const auto val = NScheme::TString::TInstance(Value);
            NTable::TUpdateOp ops{ ColumnId, NTable::ECellOp::Set, val };

            if (TxId == 0) {
                txc.DB.Update(TableId, NTable::ERowOp::Upsert, { key }, { ops });
            } else {
                txc.DB.UpdateTx(TableId, NTable::ERowOp::Upsert, { key }, { ops }, TxId);
            }
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }
    };

    struct TTxCheckRows : public ITransaction {
        TString& Data;

        TTxCheckRows(TString& data)
            : Data(data)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TStringBuilder builder;

            TVector<NTable::TTag> tags;
            tags.push_back(KeyColumnId);
            tags.push_back(ValueColumnId);
            tags.push_back(Value2ColumnId);

            NTable::EReady ready;
            auto it = txc.DB.IterateRange(TableId, { }, tags);

            while ((ready = it->Next(NTable::ENext::All)) != NTable::EReady::Gone) {
                if (ready == NTable::EReady::Page) {
                    return false;
                }

                const auto& row = it->Row();

                TString key;
                DbgPrintValue(key, row.Get(0), NScheme::TTypeInfo(NScheme::TUint64::TypeId));

                TString value;
                DbgPrintValue(value, row.Get(1), NScheme::TTypeInfo(NScheme::TString::TypeId));

                TString value2;
                DbgPrintValue(value2, row.Get(2), NScheme::TTypeInfo(NScheme::TString::TypeId));

                builder << "Key " << key << " = " << row.GetRowState()
                    << " value = " << NTable::ECellOp(row.GetCellOp(1)) << " " << value
                    << " value2 = " << NTable::ECellOp(row.GetCellOp(2)) << " " << value2 << Endl;
            }

            Data = builder;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxCheckRowsReadTx : public ITransaction {
        TString& Data;
        const ui64 TxId;

        TTxCheckRowsReadTx(TString& data, ui64 txId)
            : Data(data)
            , TxId(txId)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TStringBuilder builder;

            TVector<NTable::TTag> tags;
            tags.push_back(KeyColumnId);
            tags.push_back(ValueColumnId);
            tags.push_back(Value2ColumnId);

            NTable::EReady ready;
            auto it = txc.DB.IterateRange(TableId, { }, tags,
                TRowVersion::Max(),
                MakeIntrusive<NTable::TSingleTransactionMap>(TxId, TRowVersion::Min()));

            while ((ready = it->Next(NTable::ENext::All)) != NTable::EReady::Gone) {
                if (ready == NTable::EReady::Page) {
                    return false;
                }

                const auto& row = it->Row();

                TString key;
                DbgPrintValue(key, row.Get(0), NScheme::TTypeInfo(NScheme::TUint64::TypeId));

                TString value;
                DbgPrintValue(value, row.Get(1), NScheme::TTypeInfo(NScheme::TString::TypeId));

                TString value2;
                DbgPrintValue(value2, row.Get(2), NScheme::TTypeInfo(NScheme::TString::TypeId));

                builder << "Key " << key << " = " << row.GetRowState()
                    << " value = " << NTable::ECellOp(row.GetCellOp(1)) << " " << value
                    << " value2 = " << NTable::ECellOp(row.GetCellOp(2)) << " " << value2 << Endl;
            }

            Data = builder;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxCheckRowsUncommitted : public ITransaction {
        TString& Data;

        TTxCheckRowsUncommitted(TString& data)
            : Data(data)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TStringBuilder builder;

            TVector<NTable::TTag> tags;
            tags.push_back(KeyColumnId);
            tags.push_back(ValueColumnId);
            tags.push_back(Value2ColumnId);

            NTable::EReady ready;
            auto it = txc.DB.IterateRange(TableId, { }, tags);

            while ((ready = it->Next(NTable::ENext::Uncommitted)) != NTable::EReady::Gone) {
                if (ready == NTable::EReady::Page) {
                    return false;
                }

                for (;;) {
                    const auto& row = it->Row();

                    TString key;
                    DbgPrintValue(key, row.Get(0), NScheme::TTypeInfo(NScheme::TUint64::TypeId));

                    TString value;
                    DbgPrintValue(value, row.Get(1), NScheme::TTypeInfo(NScheme::TString::TypeId));

                    TString value2;
                    DbgPrintValue(value2, row.Get(2), NScheme::TTypeInfo(NScheme::TString::TypeId));

                    builder << "Key " << key << " = " << row.GetRowState()
                        << " value = " << NTable::ECellOp(row.GetCellOp(1)) << " " << value
                        << " value2 = " << NTable::ECellOp(row.GetCellOp(2)) << " " << value2;
                    if (it->IsUncommitted()) {
                        builder << " txId " << it->GetUncommittedTxId();
                    }
                    builder << Endl;

                    if (!it->IsUncommitted()) {
                        break;
                    }

                    ready = it->SkipUncommitted();
                    if (ready == NTable::EReady::Page) {
                        return false;
                    }
                    if (ready == NTable::EReady::Gone) {
                        break;
                    }
                }
            }

            Data = builder;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxMakeSnapshot : public ITransaction {
        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TIntrusivePtr<TTableSnapshotContext> snapContext =
                new TTestTableSnapshotContext({TableId});
            txc.Env.MakeSnapshot(snapContext);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxBorrowSnapshot : public ITransactionWithExecutor {
        TTxBorrowSnapshot(TString& snapBody, TIntrusivePtr<TTableSnapshotContext> snapContext, ui64 targetTabletId)
            : SnapBody(snapBody)
            , SnapContext(std::move(snapContext))
            , TargetTabletId(targetTabletId)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            SnapBody = Executor->BorrowSnapshot(TableId, *SnapContext, { }, { }, TargetTabletId);
            txc.Env.DropSnapshot(SnapContext);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        TString& SnapBody;
        TIntrusivePtr<TTableSnapshotContext> SnapContext;
        const ui64 TargetTabletId;
    };

    struct TTxLoanSnapshot : public ITransaction {
        TTxLoanSnapshot(TString snapBody)
            : SnapBody(snapBody)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            txc.Env.LoanTable(TableId, SnapBody);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        TString SnapBody;
    };

    Y_UNIT_TEST(MemTableLongTx) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "foo") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "bar") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(3, "abc", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(1, "def", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "ghi", 123) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Empty NULL\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set bar\n");
        }

        env.SendSync(new NFake::TEvExecute{ new TTxCommitLongTx(123) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }
    }

    Y_UNIT_TEST(CompactUncommittedLongTx) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "foo") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "bar") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(3, "abc", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(1, "def", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "ghi", 123) });

        // Force a full compaction of a table
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Empty NULL\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set bar\n");
        }

        env.SendSync(new NFake::TEvExecute{ new TTxCommitLongTx(123) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        // Force a full compaction of a table
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }
    }

    Y_UNIT_TEST(CompactCommittedLongTx) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "foo") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "bar") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(3, "abc", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(1, "def", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "ghi", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxCommitLongTx(123) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        // Force a full compaction of a table
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }
    }

    Y_UNIT_TEST(CompactedLongTxRestart) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "foo") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "bar") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(3, "abc", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(1, "def", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "ghi", 123) });

        // Force a mem table compaction
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId, true));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Empty NULL\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set bar\n");
        }

        env.SendSync(new NFake::TEvExecute{ new TTxCommitLongTx(123) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        // Force a mem table compaction
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId, true));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        Cerr << "...restarting tablet after compaction" << Endl;
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.WaitForGone();
        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) }, /* retry */ true);
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }
    }

    Y_UNIT_TEST(CompactMultipleChanges) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "foo") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "bar") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(3, "abc", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(1, "def", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(2, "ghi", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "xyz", 234) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "jkl", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "mno", 123) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRowsUncommitted(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set jkl value2 = Empty NULL txId 123\n"
                "Key 1 = Upsert value = Set xyz value2 = Empty NULL txId 234\n"
                "Key 1 = Upsert value = Empty NULL value2 = Set def txId 123\n"
                "Key 1 = Upsert value = Set foo value2 = Empty NULL\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set mno txId 123\n"
                "Key 2 = Upsert value = Set ghi value2 = Empty NULL txId 123\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set bar\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL txId 123\n");
        }

        // Force a mem table compaction
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId, true));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRowsUncommitted(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set jkl value2 = Set def txId 123\n"
                "Key 1 = Upsert value = Set xyz value2 = Empty NULL txId 234\n"
                "Key 1 = Upsert value = Set foo value2 = Empty NULL\n"
                "Key 2 = Upsert value = Set ghi value2 = Set mno txId 123\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set bar\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL txId 123\n");
        }
    }

    Y_UNIT_TEST(LongTxBorrow) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "foo") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "bar") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(3, "abc", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(1, "def", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "ghi", 234) });

        // Force a mem table compaction
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId, true));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Empty NULL\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set bar\n");
        }

        env.SendSync(new NFake::TEvExecute{ new TTxCommitLongTx(123) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set bar\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        Cerr << "...making snapshot with concurrent commit" << Endl;
        {
            TVector<THolder<NTabletFlatExecutor::ITransaction>> txs;
            txs.emplace_back(new TTxMakeSnapshot);
            txs.emplace_back(new TTxCommitLongTx(234));
            env.SendAsync(new NFake::TEvExecute{ std::move(txs) });
        }
        env.WaitForWakeUp(2);
        Cerr << "...waiting for snapshot to complete" << Endl;
        auto evSnapshot = env.GrabEdgeEvent<TEvTestFlatTablet::TEvSnapshotComplete>();
        Cerr << "...borrowing snapshot" << Endl;
        TString snapBody;
        env.SendSync(new NFake::TEvExecute{ new TTxBorrowSnapshot(snapBody, evSnapshot->Get()->SnapContext, env.Tablet + 1) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        // Stop the source tablet
        Cerr << "...stopping the source tablet" << Endl;
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.WaitForGone();

        // Start the destination tablet
        Cerr << "...starting the destination tablet" << Endl;
        ++env.Tablet;
        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        Cerr << "...initializing destination schema" << Endl;
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });

        // Loan the source table
        Cerr << "...loaning snapshot" << Endl;
        env.SendSync(new NFake::TEvExecute{ new TTxLoanSnapshot(snapBody) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set bar\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        env.SendSync(new NFake::TEvExecute{ new TTxCommitLongTx(234) });

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        Cerr << "...restarting destination tablet" << Endl;
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.WaitForGone();
        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) }, /* retry */ true);
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        Cerr << "...restarting source tablet" << Endl;
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.WaitForGone();
        --env.Tablet;
        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) }, /* retry */ true);
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }
    }

    Y_UNIT_TEST(MemTableLongTxRead) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "foo") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "bar") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(3, "abc", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(1, "def", 123) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "ghi", 123) });

        // We should see our own uncommitted changes
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRowsReadTx(data, 123) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }

        // Others shouldn't see these changes yet
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Empty NULL\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set bar\n");
        }

        env.SendSync(new NFake::TEvExecute{ new TTxCommitLongTx(123) });

        // Once committed everyone will see these changes
        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Set def\n"
                "Key 2 = Upsert value = Empty NULL value2 = Set ghi\n"
                "Key 3 = Upsert value = Set abc value2 = Empty NULL\n");
        }
    }

} // Y_UNIT_TEST_SUITE(TFlatTableLongTx)

Y_UNIT_TEST_SUITE(TFlatTableLongTxAndBlobs) {

    enum : ui32  {
        TableId = 101,
        KeyColumnId = 1,
        Value1ColumnId = 2,
        Value2ColumnId = 3,
        Value3ColumnId = 4,
    };

    struct TTxInitSchema : public ITransaction {
        TTxInitSchema(TIntrusiveConstPtr<TCompactionPolicy> policy = nullptr)
            : Policy(std::move(policy))
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            if (txc.DB.GetScheme().GetTableInfo(TableId))
                return true;

            txc.DB.Alter()
                .AddTable("test" + ToString(ui32(TableId)), TableId)
                .AddColumn(TableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false)
                .AddColumn(TableId, "value1", Value1ColumnId, NScheme::TString::TypeId, false)
                .AddColumn(TableId, "value2", Value2ColumnId, NScheme::TString::TypeId, false)
                .AddColumn(TableId, "value3", Value3ColumnId, NScheme::TString::TypeId, false)
                .AddColumnToKey(TableId, KeyColumnId);

            if (Policy) {
                txc.DB.Alter().SetCompactionPolicy(TableId, *Policy);
            }

            txc.DB.Alter().SetRedo(/* external */ 32);
            txc.DB.Alter().SetFamilyBlobs(TableId, 0, /* outer */ 16, /* external */ 32);

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

        const TIntrusiveConstPtr<TCompactionPolicy> Policy;
    };

    struct TTxDisableBlobs : public ITransaction {
        TTxDisableBlobs() = default;

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            txc.DB.Alter().SetFamilyBlobs(TableId, 0, -1, -1);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxWriteManyDeltas : public ITransaction {
        explicit TTxWriteManyDeltas(size_t count, size_t valueSize, i64 key = 123, ui64 baseTxId = 1)
            : Key(key)
            , Count(count)
            , ValueSize(valueSize)
            , BaseTxId(baseTxId)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            const auto key = NScheme::TInt64::TInstance(Key);

            TString baseValue(ValueSize, 'x');
            for (size_t index = 0; index < Count; ++index) {
                ui64 txId = BaseTxId + index;
                ui32 columnId = Value1ColumnId + (txId - 1) % 3;

                TString valueContent = TStringBuilder() << baseValue << txId;
                const auto val = NScheme::TString::TInstance(valueContent);
                NTable::TUpdateOp ops{ columnId, NTable::ECellOp::Set, val };

                txc.DB.UpdateTx(TableId, NTable::ERowOp::Upsert, { key }, { ops }, txId);
            }

            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }

        const i64 Key;
        const size_t Count;
        const size_t ValueSize;
        const ui64 BaseTxId;
    };

    struct TTxCommitManyDeltas : public ITransaction {
        explicit TTxCommitManyDeltas(size_t count, ui64 baseTxId = 1, TRowVersion baseVersion = TRowVersion(1, 1))
            : Count(count)
            , BaseTxId(baseTxId)
            , BaseVersion(baseVersion)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            for (size_t index = 0; index < Count; ++index) {
                ui64 txId = BaseTxId + index;
                TRowVersion rowVersion(BaseVersion.Step, BaseVersion.TxId + index);
                txc.DB.CommitTx(TableId, txId, rowVersion);
            }

            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }

        const size_t Count;
        const ui64 BaseTxId;
        const TRowVersion BaseVersion;
    };

    void DoValueSize(size_t valueSize) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;
        policy->ReadAheadLoThreshold = 512;
        policy->ReadAheadHiThreshold = 1024;
        policy->MinDataPageSize = 1024;
        policy->MinBTreeIndexNodeSize = 128;

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(policy) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteManyDeltas(1000, valueSize) });

        // Force a full compaction of a table
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        env.SendSync(new NFake::TEvExecute{ new TTxDisableBlobs });
        env.SendSync(new NFake::TEvExecute{ new TTxCommitManyDeltas(1000) });

        // Force a full compaction of a table
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        // TODO: check results
    }

    Y_UNIT_TEST(SmallValues) {
        DoValueSize(1);
    }

    Y_UNIT_TEST(OuterBlobValues) {
        DoValueSize(16);
    }

    Y_UNIT_TEST(ExternalBlobValues) {
        DoValueSize(32);
    }

} // Y_UNIT_TEST_SUITE(TFlatTableLongTxAndBlobs)

Y_UNIT_TEST_SUITE(TFlatTableSnapshotWithCommits) {

    struct TTxMakeSnapshotAndWrite : public ITransaction {
        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TIntrusivePtr<TTableSnapshotContext> snapContext =
                new TTestTableSnapshotContext({TRowsModel::TableId});
            txc.Env.MakeSnapshot(snapContext);

            for (i64 keyValue = 101; keyValue <= 104; ++keyValue) {
                const auto key = NScheme::TInt64::TInstance(keyValue);

                TString str = "value";
                const auto val = NScheme::TString::TInstance(str);
                NTable::TUpdateOp ops{ TRowsModel::ColumnValueId, NTable::ECellOp::Set, val };

                txc.DB.Update(TRowsModel::TableId, NTable::ERowOp::Upsert, { key }, { ops });
            }

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxBorrowSnapshot : public ITransactionWithExecutor {
        TTxBorrowSnapshot(TString& snapBody, TIntrusivePtr<TTableSnapshotContext> snapContext, ui64 targetTabletId)
            : SnapBody(snapBody)
            , SnapContext(std::move(snapContext))
            , TargetTabletId(targetTabletId)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            SnapBody = Executor->BorrowSnapshot(TRowsModel::TableId, *SnapContext, { }, { }, TargetTabletId);
            txc.Env.DropSnapshot(SnapContext);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        TString& SnapBody;
        TIntrusivePtr<TTableSnapshotContext> SnapContext;
        const ui64 TargetTabletId;
    };

    struct TTxCheckRows : public ITransaction {
        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            i64 keyId;
            TVector<NTable::TTag> tags;
            tags.push_back(TRowsModel::ColumnValueId);
            TVector<TRawTypeValue> key;
            key.emplace_back(&keyId, sizeof(keyId), NScheme::TTypeInfo(NScheme::TInt64::TypeId));

            for (keyId = 1; keyId <= 104; ++keyId) {
                NTable::TRowState row;
                auto ready = txc.DB.Select(TRowsModel::TableId, key, tags, row);
                if (ready == NTable::EReady::Page)
                    return false;
                Y_VERIFY_S(ready == NTable::EReady::Data, "Failed to find key " << keyId);
                Y_ABORT_UNLESS(row.GetRowState() == NTable::ERowOp::Upsert);
                TStringBuf selected = row.Get(0).AsBuf();
                TString expected = "value";
                Y_ABORT_UNLESS(selected == expected);
            }

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    Y_UNIT_TEST(SnapshotWithCommits) {
        TMyEnvBase env;
        TRowsModel rows;

        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        // Start the first tablet
        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();

        // Init schema
        {
            TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy();
            env.SendSync(rows.MakeScheme(std::move(policy)));
        }

        // Insert 100 rows
        Cerr << "...inserting rows" << Endl;
        env.SendSync(rows.MakeRows(100, 0, 100));

        Cerr << "...making snapshot and writing to table" << Endl;
        env.SendSync(new NFake::TEvExecute{ new TTxMakeSnapshotAndWrite });
        Cerr << "...waiting for snapshot to complete" << Endl;
        auto evSnapshot = env.GrabEdgeEvent<TEvTestFlatTablet::TEvSnapshotComplete>();
        Cerr << "...borrowing snapshot" << Endl;
        TString snapBody;
        env.SendSync(new NFake::TEvExecute{ new TTxBorrowSnapshot(snapBody, evSnapshot->Get()->SnapContext, env.Tablet + 1) });

        // Check all added rows one-by-one
        Cerr << "...checking rows" << Endl;
        env.SendSync(new NFake::TEvExecute{ new TTxCheckRows });

        for (int i = 0; i < 2; ++i) {
            Cerr << "...restarting tablet" << Endl;
            env.SendSync(new TEvents::TEvPoison, false, true);
            env.WaitForGone();
            env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
                return new TTestFlatTablet(env.Edge, tablet, info);
            });
            env.WaitForWakeUp();

            // Check all added rows one-by-one
            Cerr << "...checking rows" << Endl;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows }, /* retry */ true);
        }
    }

} // Y_UNIT_TEST_SUITE(TFlatTableSnapshotWithCommits)

Y_UNIT_TEST_SUITE(TFlatTableExecutorIndexLoading) {

    struct TTxCalculateReadSize : public ITransaction {
        ui32 Attempt = 0;
        TVector<ui64>& ReadSizes;
        ui64 MinKey, MaxKey;
        
        TTxCalculateReadSize(TVector<ui64>& readSizes, ui64 minKey, ui64 maxKey)
            : ReadSizes(readSizes)
            , MinKey(minKey)
            , MaxKey(maxKey)
        {
            ReadSizes.clear();
        }


        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            UNIT_ASSERT_LE(++Attempt, 10);

            const auto minKey = NScheme::TInt64::TInstance(MinKey);
            const auto maxKey = NScheme::TInt64::TInstance(MaxKey);
            const TVector<NTable::TTag> tags{ { TRowsModel::ColumnKeyId, TRowsModel::ColumnValueId } };

            auto sizeEnv = txc.DB.CreateSizeEnv();
            txc.DB.CalculateReadSize(sizeEnv, TRowsModel::TableId, { minKey }, { maxKey }, tags, 0, 0, 0);
            ReadSizes.push_back(sizeEnv.GetSize());

            return txc.DB.Precharge(TRowsModel::TableId,  { minKey }, { maxKey }, tags, 0, 0, 0);
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxPrechargeAndSeek : public ITransaction {
        ui32 Attempt = 0;
        bool Pinned = false;

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            UNIT_ASSERT_LE(++Attempt, 10);

            const auto key = NScheme::TInt64::TInstance(100);
            const TVector<NTable::TTag> tags{ { TRowsModel::ColumnKeyId, TRowsModel::ColumnValueId } };

            // no we should have 2 data pages without index, it's ~20*1024 bytes
            // (index size is ~ bytes)
            auto precharged = txc.DB.Precharge(TRowsModel::TableId, { key }, { key }, tags, 0, 0, 0);
            if (!precharged) {
                return false;
            }

            NTable::TRowState row;
            auto ready = txc.DB.Select(TRowsModel::TableId, { key }, tags, row);
            if (ready == NTable::EReady::Page) {
                return false;
            }

            if (!Pinned) {
                // needed for legal restart
                txc.RequestMemory(1);
                // Postpone will pin all touched data
                Pinned = true;
                return false;
            } else {
                return true;
            }
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    void ZeroSharedCache(TMyEnvBase &env) {
        env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(0));
    }

    Y_UNIT_TEST(CalculateReadSize_FlatIndex) {
        TMyEnvBase env;
        TRowsModel rows;
        const ui32 rowsCount = 1024;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), false));

        env.SendSync(rows.MakeRows(rowsCount, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        TVector<ui64> sizes;
        
        env.SendSync(new NFake::TEvExecute{ new TTxCalculateReadSize(sizes, 0, 1) });
        UNIT_ASSERT_VALUES_EQUAL(sizes, (TVector<ui64>{20566, 20566}));

        env.SendSync(new NFake::TEvExecute{ new TTxCalculateReadSize(sizes, 100, 200) });
        UNIT_ASSERT_VALUES_EQUAL(sizes, (TVector<ui64>{1048866, 1048866}));

        env.SendSync(new NFake::TEvExecute{ new TTxCalculateReadSize(sizes, 300, 700) });
        UNIT_ASSERT_VALUES_EQUAL(sizes, (TVector<ui64>{4133766, 4133766}));
    }

    Y_UNIT_TEST(CalculateReadSize_BTreeIndex) {
        TMyEnvBase env;
        TRowsModel rows;
        const ui32 rowsCount = 1024;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();
        ZeroSharedCache(env);

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.MakeRows(rowsCount, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        TVector<ui64> sizes;
        
        env.SendSync(new NFake::TEvExecute{ new TTxCalculateReadSize(sizes, 0, 1) });
        UNIT_ASSERT_VALUES_EQUAL(sizes, (TVector<ui64>{0, 0, 0, 0, 20566, 20566}));

        env.SendSync(new NFake::TEvExecute{ new TTxCalculateReadSize(sizes, 100, 200) });
        UNIT_ASSERT_VALUES_EQUAL(sizes, (TVector<ui64>{0, 0, 0, 0, 1048866, 1048866}));

        env.SendSync(new NFake::TEvExecute{ new TTxCalculateReadSize(sizes, 300, 700) });
        UNIT_ASSERT_VALUES_EQUAL(sizes, (TVector<ui64>{0, 0, 0, 0, 4133766, 4133766}));
    }

    Y_UNIT_TEST(PrechargeAndSeek_FlatIndex) {
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);

        appData.ResourceProfiles = new TResourceProfiles;
        TResourceProfiles::TResourceProfile profile;
        profile.SetTabletType(NKikimrTabletBase::TTabletTypes::Unknown);
        profile.SetName("zero");
        profile.SetInitialTxMemory(0);

        // 2 data pages ~2*10*1024 bytes
        // flat index size is ~256*1024 bytes shouldn't be included
        profile.SetTxMemoryLimit(22*1024);

        appData.ResourceProfiles->AddProfile(profile);

        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(rows.MakeScheme(new TCompactionPolicy()));

        env.SendSync(rows.MakeRows(10*1024, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        env.SendSync(new NFake::TEvExecute(new NTestSuiteTFlatTableExecutorResourceProfile::TTxSetResourceProfile("zero")));
        env.SendSync(new NFake::TEvExecute{ new TTxPrechargeAndSeek() });

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // should have the same behaviour
        env.SendSync(new NFake::TEvExecute{ new TTxPrechargeAndSeek() }, true);

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(PrechargeAndSeek_BTreeIndex) {
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);

        appData.ResourceProfiles = new TResourceProfiles;
        TResourceProfiles::TResourceProfile profile;
        profile.SetTabletType(NKikimrTabletBase::TTabletTypes::Unknown);
        profile.SetName("zero");
        profile.SetInitialTxMemory(0);

        // 2 data pages ~2*10*1024 bytes
        // 3 b-tree index nodes ~3*7*1024 bytes
        profile.SetTxMemoryLimit((20 + 21)*1024);

        appData.ResourceProfiles->AddProfile(profile);

        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.MakeRows(10*1024, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        env.SendSync(new NFake::TEvExecute(new NTestSuiteTFlatTableExecutorResourceProfile::TTxSetResourceProfile("zero")));
        env.SendSync(new NFake::TEvExecute{ new TTxPrechargeAndSeek() });

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // should have the same behaviour
        env.SendSync(new NFake::TEvExecute{ new TTxPrechargeAndSeek() }, true);

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(Scan_FlatIndex) {
        TMyEnvBase env;
        TRowsModel rows;
        const ui32 rowsCount = 1024;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);

        env->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), false));

        env.SendSync(rows.MakeRows(rowsCount, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        { // no read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {1, 1};
            queueScan->ExpectedPageFaults = 1025;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // small read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            queueScan->ExpectedPageFaults = 171;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // infinite read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {Max<ui64>(), Max<ui64>()};
            env.SendAsync(std::move(queueScan));
            queueScan->ExpectedPageFaults = 2;
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        for (ui64 leadKey = 1; ; leadKey += rowsCount / 10) {
            ui64 expectedRowsCount = rowsCount >= leadKey ? rowsCount - leadKey + 1 : 0;
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(expectedRowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            TVector<TCell> leadKey_ = {TCell::Make(leadKey)};
            queueScan->LeadKey = leadKey_;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
            if (!expectedRowsCount) {
                break;
            }
        }

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(Scan_BTreeIndex) {
        TMyEnvBase env;
        TRowsModel rows;
        const ui32 rowsCount = 1024;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);

        env->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();
        ZeroSharedCache(env);

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.MakeRows(rowsCount, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        { // no read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {1, 1};
            queueScan->ExpectedPageFaults = 1028;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // small read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            queueScan->ExpectedPageFaults = 189;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // infinite read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {Max<ui64>(), Max<ui64>()};
            queueScan->ExpectedPageFaults = 5;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        for (ui64 leadKey = 1; ; leadKey += rowsCount / 10) {
            ui64 expectedRowsCount = rowsCount >= leadKey ? rowsCount - leadKey + 1 : 0;
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(expectedRowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            TVector<TCell> leadKey_ = {TCell::Make(leadKey)};
            queueScan->LeadKey = leadKey_;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
            if (!expectedRowsCount) {
                break;
            }
        }

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(Scan_History_FlatIndex) {
        TMyEnvBase env;
        TRowsModel rows;
        const ui32 rowsCount = 1024;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);

        env->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), false));

        env.SendSync(rows.RowTo(1).VersionTo(TRowVersion(1, 10)).MakeRows(rowsCount, 10*1024));
        env.SendSync(rows.RowTo(1).VersionTo(TRowVersion(2, 20)).MakeRows(rowsCount, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        { // no read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount, TRowVersion(2, 0));
            queueScan->ReadAhead = {1, 1};
            env.SendAsync(std::move(queueScan));
            queueScan->ExpectedPageFaults = 2050;
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // small read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount, TRowVersion(2, 0));
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            queueScan->ExpectedPageFaults = 173;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // infinite read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount, TRowVersion(2, 0));
            queueScan->ReadAhead = {Max<ui64>(), Max<ui64>()};
            queueScan->ExpectedPageFaults = 4;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        for (ui64 leadKey = 1; ; leadKey += rowsCount / 10) {
            ui64 expectedRowsCount = rowsCount >= leadKey ? rowsCount - leadKey + 1 : 0;
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(expectedRowsCount, TRowVersion(2, 0));
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            TVector<TCell> leadKey_ = {TCell::Make(leadKey)};
            queueScan->LeadKey = leadKey_;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
            if (!expectedRowsCount) {
                break;
            }
        }

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(Scan_History_BTreeIndex) {
        TMyEnvBase env;
        TRowsModel rows;
        const ui32 rowsCount = 1024;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);

        env->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();
        ZeroSharedCache(env);

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.RowTo(1).VersionTo(TRowVersion(1, 10)).MakeRows(rowsCount, 10*1024));
        env.SendSync(rows.RowTo(1).VersionTo(TRowVersion(2, 20)).MakeRows(rowsCount, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        { // no read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount, TRowVersion(2, 0));
            queueScan->ReadAhead = {1, 1};
            queueScan->ExpectedPageFaults = 2056;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // small read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount, TRowVersion(2, 0));
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            queueScan->ExpectedPageFaults = 194;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // infinite read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount, TRowVersion(2, 0));
            queueScan->ReadAhead = {Max<ui64>(), Max<ui64>()};
            queueScan->ExpectedPageFaults = 10;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        for (ui64 leadKey = 1; ; leadKey += rowsCount / 10) {
            ui64 expectedRowsCount = rowsCount >= leadKey ? rowsCount - leadKey + 1 : 0;
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(expectedRowsCount, TRowVersion(2, 0));
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            TVector<TCell> leadKey_ = {TCell::Make(leadKey)};
            queueScan->LeadKey = leadKey_;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
            if (!expectedRowsCount) {
                break;
            }
        }

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(Scan_Groups_FlatIndex) {
        TMyEnvBase env;
        TRowsModel rows;
        const ui32 rowsCount = 1024;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);

        env->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), true));

        env.SendSync(rows.MakeRows(rowsCount, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        { // no read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {1, 1};
            queueScan->ExpectedPageFaults = 1029;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // small read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            queueScan->ExpectedPageFaults = 173;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // infinite read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {Max<ui64>(), Max<ui64>()};
            queueScan->ExpectedPageFaults = 4;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        for (ui64 leadKey = 1; ; leadKey += rowsCount / 10) {
            ui64 expectedRowsCount = rowsCount >= leadKey ? rowsCount - leadKey + 1 : 0;
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(expectedRowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            TVector<TCell> leadKey_ = {TCell::Make(leadKey)};
            queueScan->LeadKey = leadKey_;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
            if (!expectedRowsCount) {
                break;
            }
        }

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(Scan_Groups_BTreeIndex) {
        TMyEnvBase env;
        TRowsModel rows;
        const ui32 rowsCount = 1024;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);

        env->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();
        ZeroSharedCache(env);

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy), true));

        env.SendSync(rows.MakeRows(rowsCount, 10*1024));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        { // no read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {1, 1};
            queueScan->ExpectedPageFaults = 1032;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // small read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            queueScan->ExpectedPageFaults = 191;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // infinite read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {Max<ui64>(), Max<ui64>()};
            queueScan->ExpectedPageFaults = 7;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        for (ui64 leadKey = 1; ; leadKey += rowsCount / 10) {
            ui64 expectedRowsCount = rowsCount >= leadKey ? rowsCount - leadKey + 1 : 0;
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(expectedRowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            TVector<TCell> leadKey_ = {TCell::Make(leadKey)};
            queueScan->LeadKey = leadKey_;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
            if (!expectedRowsCount) {
                break;
            }
        }

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(Scan_Groups_BTreeIndex_Empty) {
        TMyEnvBase env;
        TRowsModel rows;
        const ui32 rowsCount = 10;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);

        env->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });
        env.WaitForWakeUp();
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), true));

        env.SendSync(rows.MakeRows(rowsCount, 10));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        { // no read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {1, 1};
            queueScan->ExpectedPageFaults = 2;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        { // read ahead
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(rowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            queueScan->ExpectedPageFaults = 2;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
        }

        for (ui64 leadKey = 1; ; leadKey++) {
            ui64 expectedRowsCount = rowsCount >= leadKey ? rowsCount - leadKey + 1 : 0;
            auto queueScan = new TEvTestFlatTablet::TEvQueueScan(expectedRowsCount);
            queueScan->ReadAhead = {5*10*1024, 10*10*1024};
            TVector<TCell> leadKey_ = {TCell::Make(leadKey)};
            queueScan->LeadKey = leadKey_;
            queueScan->ExpectedPageFaults = expectedRowsCount ? 2 : 0;
            env.SendAsync(std::move(queueScan));
            env.WaitFor<TEvTestFlatTablet::TEvScanFinished>();
            if (!expectedRowsCount) {
                break;
            }
        }

        // If we didn't crash, then assume the test succeeded
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorStickyPages) {
    using EReady = NTable::EReady;
    using ENext = NTable::ENext;

    struct TTxFullScan : public ITransaction {
        int& FailedAttempts;

        TTxFullScan(int& failedAttempts)
            : FailedAttempts(failedAttempts)
        {
            FailedAttempts = 0;
        }

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            TVector<NTable::TTag> tags{ { TRowsModel::ColumnKeyId, TRowsModel::ColumnValueId } };

            auto iter = txc.DB.IterateRange(TRowsModel::TableId, { }, tags, {2, 0});
            while (iter->Next(ENext::Data) == EReady::Data) {
                // iterate over all rows
            }

            if (iter->Last() != EReady::Page) {
                return true;
            }
            FailedAttempts++;
            return false;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxKeepFamilyInMemory : public ITransaction {
        ui32 Family;

        TTxKeepFamilyInMemory(ui32 family)
            : Family(family)
        { }

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            using namespace NTable::NPage;

            txc.DB.Alter().SetFamily(TRowsModel::TableId, Family, ECache::Ever, ECodec::Plain);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxAddFamily : public ITransaction {
        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            using namespace NTable::NPage;

            txc.DB.Alter()
                .SetFamily(TRowsModel::TableId, TRowsModel::AltFamilyId, ECache::None, ECodec::Plain)
                .AddColumnToFamily(TRowsModel::TableId, TRowsModel::ColumnValueId, TRowsModel::AltFamilyId);

            return true;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };
    
    void ZeroSharedCache(TMyEnvBase &env) {
        env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(0));
    }

    Y_UNIT_TEST(TestNonSticky_FlatIndex) {
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy()));

        // 10 history pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 10 data pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 20); // 20 data pages, 2 index pages are sticky until restart

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 22); // 20 data pages and 2 index pages

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 20); // 20 data pages, 2 index pages are sticky again
    }

    Y_UNIT_TEST(TestNonSticky_BTreeIndex) {
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy()));

        // 10 history pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 10 data pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 22); // 20 data pages, 2 index nodes

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 22); // 20 data pages, 2 index nodes

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 22); // 20 data pages, 2 index nodes
    }

    Y_UNIT_TEST(TestSticky) {
        TMyEnvBase env;
        TRowsModel rows;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy()));
        env.SendSync(new NFake::TEvExecute{ new TTxKeepFamilyInMemory(0) });

        // 10 history pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 10 data pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // should have the same behaviour
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);
    }

    Y_UNIT_TEST(TestNonStickyGroup_FlatIndex) {
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), true));

        // 1 historic[0] + 10 historic[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 1 groups[0] + 10 groups[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 12); // 1 groups[0], 1 historic[0], 10 historic[1] pages, 3 index pages are sticky until restart

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 15); // 1 groups[0], 1 historic[0], 10 historic[1] pages, 3 index pages

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 12); // 1 groups[0], 1 historic[0], 10 historic[1] pages, 3 index pages are sticky again
    }

    Y_UNIT_TEST(TestNonStickyGroup_BTreeIndex) {
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), true));

        // 1 historic[0] + 10 historic[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 1 groups[0] + 10 groups[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 13); // index root nodes, 1 groups[0], 1 historic[0], 10 historic[1] pages

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 13); // index root nodes, 1 groups[0], 1 historic[0], 10 historic[1] pages

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 13); // index root nodes, 1 groups[0], 1 historic[0], 10 historic[1] pages
    }

    Y_UNIT_TEST(TestStickyMain) {
        TMyEnvBase env;
        TRowsModel rows;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), true));
        env.SendSync(new NFake::TEvExecute{ new TTxKeepFamilyInMemory(0) });

        // 1 historic[0] + 10 historic[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 1 groups[0] + 10 groups[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 10); // 10 historic[1] pages

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // should have the same behaviour
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 10);
    }

    Y_UNIT_TEST(TestStickyAlt_FlatIndex) {
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), true));

        env.SendSync(new NFake::TEvExecute{ new TTxKeepFamilyInMemory(TRowsModel::AltFamilyId) });

        // 1 historic[0] + 10 historic[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 1 groups[0] + 10 groups[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 2); // 1 groups[0], 1 historic[0], 3 index pages are sticky until restart

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 5); // 1 groups[0], 1 historic[0], 3 index pages

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 2); // 1 groups[0], 1 historic[0], 3 index pages are sticky again
    }

    Y_UNIT_TEST(TestStickyAlt_BTreeIndex) {
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), true));

        env.SendSync(new NFake::TEvExecute{ new TTxKeepFamilyInMemory(TRowsModel::AltFamilyId) });

        // 1 historic[0] + 10 historic[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 1 groups[0] + 10 groups[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 3); // index root nodes, 1 groups[0], 1 historic[0]

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 3); // index root nodes, 1 groups[0], 1 historic[0]

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 3); // index root nodes, 1 groups[0], 1 historic[0]
    }

    Y_UNIT_TEST(TestStickyAll) {
        TMyEnvBase env;
        TRowsModel rows;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy(), true));
        env.SendSync(new NFake::TEvExecute{ new TTxKeepFamilyInMemory(0) });
        env.SendSync(new NFake::TEvExecute{ new TTxKeepFamilyInMemory(TRowsModel::AltFamilyId) });

        // 1 historic[0] + 10 historic[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 1 groups[0] + 10 groups[1] pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // should have the same behaviour
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);
    }

    Y_UNIT_TEST(TestAlterAddFamilySticky) {
        TMyEnvBase env;
        TRowsModel rows;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy()));

        // 10 historic[0] pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 10 groups[0] pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // add family, old parts won't have it
        env.SendSync(new NFake::TEvExecute{ new TTxAddFamily() });
        env.SendSync(new NFake::TEvExecute{ new TTxKeepFamilyInMemory(0) });
        env.SendSync(new NFake::TEvExecute{ new TTxKeepFamilyInMemory(TRowsModel::AltFamilyId) });

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_GE(failedAttempts, 20); // old parts aren't sticky before restart

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0); // all old columns were made sticky, load them on start
    }

    Y_UNIT_TEST(TestAlterAddFamilyPartiallySticky) {
        TMyEnvBase env;
        TRowsModel rows;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        ZeroSharedCache(env);

        env.SendSync(rows.MakeScheme(new TCompactionPolicy()));

        // 10 historic[0] pages
        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(70, 950));
        
        // 10 groups[0] pages
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(70, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // add family, old parts won't have it
        env.SendSync(new NFake::TEvExecute{ new TTxAddFamily() });
        env.SendSync(new NFake::TEvExecute{ new TTxKeepFamilyInMemory(0) });

        int failedAttempts = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) });
        UNIT_ASSERT_GE(failedAttempts, 20); // old parts aren't sticky before restart

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0); // if at least one family of a group is for memory load it 
    }
}

Y_UNIT_TEST_SUITE(TFlatTableExecutorBTreeIndex) {
    using EReady = NTable::EReady;
    using ENext = NTable::ENext;

    struct TTxFullScan : public ITransaction {
        int& ReadRows;
        int& FailedAttempts;
        
        TTxFullScan(int& readRows, int& failedAttempts)
            : ReadRows(readRows)
            , FailedAttempts(failedAttempts)
        {
            ReadRows = 0;
            FailedAttempts = 0;
        }

        bool Execute(TTransactionContext &txc, const TActorContext &) override
        {
            TVector<NTable::TTag> tags{ { TRowsModel::ColumnKeyId, TRowsModel::ColumnValueId } };

            auto iter = txc.DB.IterateRange(TRowsModel::TableId, { }, tags, {2, 0});
            
            ReadRows = 0;
            while (iter->Next(ENext::Data) == EReady::Data) {
                ReadRows++;
            }

            if (iter->Last() != EReady::Page) {
                return true;
            }

            FailedAttempts++;
            return false;
        }

        void Complete(const TActorContext &ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    Y_UNIT_TEST(EnableLocalDBBtreeIndex_Default) { // uses b-tree index
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        UNIT_ASSERT_VALUES_EQUAL(appData.FeatureFlags.HasEnableLocalDBBtreeIndex(), false);
        UNIT_ASSERT_VALUES_EQUAL(appData.FeatureFlags.HasEnableLocalDBFlatIndex(), false);
        auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());
        int readRows = 0, failedAttempts = 0;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(1000, 950));
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(1000, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // all pages are always kept in shared cache (except flat index)
        UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 334);

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // after restart we have no pages in private cache
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 330);
    }

    Y_UNIT_TEST(EnableLocalDBBtreeIndex_True) { // uses b-tree index
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());
        int readRows = 0, failedAttempts = 0;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(1000, 950));
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(1000, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // all pages are always kept in shared cache (except flat index)
        UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 334);

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // after restart we have no pages in private cache
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 330);
    }

    Y_UNIT_TEST(EnableLocalDBBtreeIndex_False) { // uses flat index
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());
        int readRows = 0, failedAttempts = 0;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(1000, 950));
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(1000, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // all pages are always kept in shared cache
        UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 290);

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // after restart we have no pages in private cache
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 288);
    }

    Y_UNIT_TEST(EnableLocalDBBtreeIndex_True_EnableLocalDBFlatIndex_False) { // uses b-tree index
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);
        auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());
        int readRows = 0, failedAttempts = 0;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(1000, 950));
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(1000, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // all pages are always kept in shared cache
        UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 334);

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // after restart we have no pages in private cache
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 330);
    }

    Y_UNIT_TEST(EnableLocalDBBtreeIndex_False_EnableLocalDBFlatIndex_False) { // uses flat index
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(false);
        auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());
        int readRows = 0, failedAttempts = 0;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(1000, 950));
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(1000, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // all pages are always kept in shared cache
        UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 290);

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // after restart we have no pages in private cache
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 288);
    }

    Y_UNIT_TEST(EnableLocalDBBtreeIndex_True_TurnOff) { // uses b-tree index at first
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);
        auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());
        int readRows = 0, failedAttempts = 0;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(1000, 950));
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(1000, 950));
        
        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // all pages are always kept in shared cache (except flat index)
        UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 334);

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 0);

        // restart tablet, turn off setting
        env.SendSync(new TEvents::TEvPoison, false, true);
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // after restart we have no pages in private cache
        // but use only flat index
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 288);
    }

    Y_UNIT_TEST(EnableLocalDBBtreeIndex_True_Generations) { // uses b-tree index
        TMyEnvBase env;
        TRowsModel rows;

        auto &appData = env->GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(true);
        auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());
        int readRows = 0, failedAttempts = 0;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        auto policy = MakeIntrusive<TCompactionPolicy>();
        policy->MinBTreeIndexNodeSize = 128;
        policy->Generations.push_back({100 * 1024, 2, 2, 200 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), false});
        for (auto& gen : policy->Generations) {
            gen.ExtraCompactionPercent = 0;
            gen.ExtraCompactionMinSize = 0;
            gen.ExtraCompactionExpPercent = 0;
            gen.ExtraCompactionExpMaxSize = 0;
            gen.UpliftPartSize = 0;
        }
        env.SendSync(rows.MakeScheme(std::move(policy)));

        env.SendSync(rows.VersionTo(TRowVersion(1, 10)).RowTo(0).MakeRows(1000, 950));
        env.SendSync(rows.VersionTo(TRowVersion(2, 20)).RowTo(0).MakeRows(1000, 950));

        env.SendSync(new NFake::TEvCompact(TRowsModel::TableId));
        env.WaitFor<NFake::TEvCompacted>();

        // gen 0 data pages are always kept in shared cache
        // b-tree index pages are always kept in shared cache
        UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 48);

        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 286);

        // restart tablet
        env.SendSync(new TEvents::TEvPoison, false, true);
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // after restart we have no pages in private cache
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(readRows, failedAttempts) }, true);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(failedAttempts, 330);
    }

}

Y_UNIT_TEST_SUITE(TFlatTableExecutorReboot) {
    Y_UNIT_TEST(TestSchemeGcAfterReassign) {
        TMyEnvBase env;
        TRowsModel data;

        env->SetLogPriority(NKikimrServices::TABLET_FLATBOOT, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });

        env.WaitForWakeUp();

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy();

        env.SendSync(data.MakeScheme(std::move(policy)));
        env.SendSync(data.MakeRows(250));
        bool wasGc = false;
        bool timeToStop = false;

        env.Env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage) {
                auto* event = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                if (event->Channel == 1) {
                    wasGc = true;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        env.SendSync(new TEvents::TEvPoison, false, true);
        IActor *tabletActor = nullptr; // save tablet to get its actor id and avoid using tablet resolver which has outdated info
        while (true) {
            struct TReassignedStarter : NFake::TStarter {
                NFake::TStorageInfo* MakeTabletInfo(ui64 tablet) noexcept override {
                    auto *info = TStarter::MakeTabletInfo(tablet);
                    info->Channels[1].History.emplace_back(3, 3);
                    return info;
                }
            };
            TReassignedStarter starter;
            env.FireTablet(env.Edge, env.Tablet, [&env, &tabletActor](const TActorId &tablet, TTabletStorageInfo *info) mutable {
                return tabletActor = new TTestFlatTablet(env.Edge, tablet, info);
            }, 0, &starter);
            env.WaitForWakeUp();
            env.SendEv(tabletActor->SelfId(), new TEvTestFlatTablet::TEvQueueScan(data.Rows(), true));
            env.WaitForWakeUp();
            env.SendEv(tabletActor->SelfId(), new TEvTestFlatTablet::TEvStartQueuedScan());
            TAutoPtr<IEventHandle> handle;
            env->GrabEdgeEventRethrow<TEvTestFlatTablet::TEvScanFinished>(handle);
            env.SendEv(tabletActor->SelfId(), new TEvents::TEvPoison);
            env.WaitForGone();
            // do 1 more iter after gc happened
            if (timeToStop) {
                break;
            }
            timeToStop = wasGc;
        }
    }
}

Y_UNIT_TEST_SUITE(TCutTabletHistory) {
    Y_UNIT_TEST(TestCutTabletHistory) {
        struct TTestStarter : NFake::TStarter {
            NFake::TStorageInfo* MakeTabletInfo(ui64 tablet) noexcept override {
                auto *info = TStarter::MakeTabletInfo(tablet);
                info->Channels[1].History.emplace_back(1, 0);
                info->Channels[1].History.emplace_back(2, 1);
                return info;
            }
        };

        TMyEnvBase env;
        TRowsModel data;
        bool wasCutHistory = false;
        env.Env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTablet::EvCutTabletHistory) {
                auto* event = ev->Get<TEvTablet::TEvCutTabletHistory>();
                UNIT_ASSERT_VALUES_EQUAL(event->Record.GetChannel(), 1);
                UNIT_ASSERT_LE(event->Record.GetFromGeneration(), 1);
                UNIT_ASSERT_LE(event->Record.GetGroupID(), 1);
                wasCutHistory = true;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        });

        env.WaitForWakeUp();

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy();

        env.SendSync(data.MakeScheme(std::move(policy)));
        env.SendSync(data.MakeRows(250));

        env.SendSync(new TEvents::TEvPoison, false, true);

        TTestStarter starter;
        env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TTestFlatTablet(env.Edge, tablet, info);
        }, 0, &starter);

        while (!wasCutHistory) {
            env.Env.DispatchEvents({});
        }

    }
}

} // namespace NTabletFlatExecutor
} // namespace NKikimr
