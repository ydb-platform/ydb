#include "flat_cxx_database.h"
#include "flat_executor_recovery.h"
#include "flat_executor_ut_common.h"

#include <ydb/core/testlib/actors/block_events.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

struct TSchema : NIceDb::Schema {
    struct Data : Table<1> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
        struct Value : Column<2, NScheme::NTypeIds::Uint32> { };
        struct BinaryValue : Column<3, NScheme::NTypeIds::String> { };
        struct DefaultValue : Column<4, NScheme::NTypeIds::Uint32> { static constexpr ui32 Default = 42; };
        struct NoBackupColumn : Column<5, NScheme::NTypeIds::Uint32> { using BackupPolicy = NoBackup; };

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value, BinaryValue, DefaultValue, NoBackupColumn>;
    };

    struct CompositePKData : Table<3> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
        struct SubKey : Column<2, NScheme::NTypeIds::Uint64> { };
        struct Value : Column<3, NScheme::NTypeIds::Uint32> { };

        using TKey = TableKey<Key, SubKey>;
        using TColumns = TableColumns<Key, SubKey, Value>;
    };

    struct NoBackupTable : Table<4> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
        struct Value : Column<2, NScheme::NTypeIds::Uint32> { };

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;

        using BackupPolicy = NoBackup;
    };

    using TTables = SchemaTables<Data, CompositePKData, NoBackupTable>;
}; // TSchema

struct TNewColumnSchema : NIceDb::Schema {
    struct Data : Table<1> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
        struct Value : Column<2, NScheme::NTypeIds::Uint32> { };
        struct BinaryValue : Column<3, NScheme::NTypeIds::String> { };
        struct DefaultValue : Column<4, NScheme::NTypeIds::Uint32> { static constexpr ui32 Default = 42; };
        struct NoBackupColumn : Column<5, NScheme::NTypeIds::Uint32> { using BackupPolicy = NoBackup; };

        struct NewColumn : Column<100, NScheme::NTypeIds::Uint32> { };

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value, BinaryValue, DefaultValue, NoBackupColumn, NewColumn>;
    };

    struct CompositePKData : Table<3> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
        struct SubKey : Column<2, NScheme::NTypeIds::Uint64> { };
        struct Value : Column<3, NScheme::NTypeIds::Uint32> { };

        using TKey = TableKey<Key, SubKey>;
        using TColumns = TableColumns<Key, SubKey, Value>;
    };

    struct NoBackupTable : Table<4> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
        struct Value : Column<2, NScheme::NTypeIds::Uint32> { };

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;

        using BackupPolicy = NoBackup;
    };

    using TTables = SchemaTables<Data, CompositePKData, NoBackupTable>;
}; // TNewColumnSchema

template<typename T>
struct TTxInitSchema : public ITransaction {
    const TActorId Owner;

    TTxInitSchema(TActorId owner) : Owner(owner) {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb(txc.DB).Materialize<T>();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
}; // TTxInitSchema

struct TTxInitSchemaWithMigration : public ITransaction {
    const TActorId Owner;

    TTxInitSchemaWithMigration(TActorId owner) : Owner(owner) {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Materialize<TSchema>();
        db.Table<TSchema::Data>().Key(1)
            .Update<TSchema::Data::Value>(42);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
}; // TTxInitSchemaWithMigration

struct TTxWriteValue : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui32 Value;

    TTxWriteValue(TActorId owner, ui64 key, ui32 value)
        : Owner(owner)
        , Key(key)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TSchema::Data>().Key(Key)
            .Update<TSchema::Data::Value>(Value);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TTxWriteValue

struct TTxWriteRange : public ITransaction {
    const TActorId Owner;
    const ui64 KeyStart;
    const ui64 KeyEnd;
    const ui32 Value;

    TTxWriteRange(TActorId owner, ui64 keyStart, ui64 keyEnd, ui32 value)
        : Owner(owner)
        , KeyStart(keyStart)
        , KeyEnd(keyEnd)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        for (ui64 key = KeyStart; key < KeyEnd; ++key) {
            db.Table<TSchema::Data>().Key(key)
                .Update<TSchema::Data::Value>(Value);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TTxWriteRange

struct TTxWriteBinaryValue : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const TString Value;

    TTxWriteBinaryValue(TActorId owner, ui64 key, const TString& value)
        : Owner(owner)
        , Key(key)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TSchema::Data>().Key(Key)
            .Update<TSchema::Data::BinaryValue>(Value);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TTxWriteBinaryValue

struct TTxWriteCompositePK : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui64 SubKey;
    const ui32 Value;

    TTxWriteCompositePK(TActorId owner, ui64 key, ui64 subKey, ui32 value)
        : Owner(owner)
        , Key(key)
        , SubKey(subKey)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TSchema::CompositePKData>().Key(Key, SubKey)
            .Update<TSchema::CompositePKData::Value>(Value);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TTxWriteCompositePK

struct TTxEraseRow : public ITransaction {
    const TActorId Owner;
    const ui64 Key;

    TTxEraseRow(TActorId owner, ui64 key)
        : Owner(owner)
        , Key(key)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TSchema::Data>().Key(Key).Delete();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TTxEraseRow

struct TxWriteDefaultValue : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const std::optional<ui32> Value;

    TxWriteDefaultValue(TActorId owner, ui64 key, std::optional<ui32> value)
        : Owner(owner)
        , Key(key)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        if (Value) {
            db.Table<TSchema::Data>().Key(Key)
                .Update<TSchema::Data::DefaultValue>(*Value);
        } else {
            db.Table<TSchema::Data>().Key(Key)
                .UpdateToNull<TSchema::Data::DefaultValue>();
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TxWriteDefaultValue

struct TxWriteTwoColumns : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui32 Value;
    const TString BinaryValue;

    TxWriteTwoColumns(TActorId owner, ui64 key, ui32 value, const TString& binaryValue)
        : Owner(owner)
        , Key(key)
        , Value(value)
        , BinaryValue(binaryValue)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TSchema::Data>().Key(Key)
            .Update<TSchema::Data::Value, TSchema::Data::BinaryValue>(Value, BinaryValue);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TxWriteTwoColumns

struct TxWriteTwoColumnsNoBackupColumn : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui32 Value;
    const ui32 NoBackupColumnValue;

    TxWriteTwoColumnsNoBackupColumn(TActorId owner, ui64 key, ui32 value, ui32 noBackupColumnValue)
        : Owner(owner)
        , Key(key)
        , Value(value)
        , NoBackupColumnValue(noBackupColumnValue)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TSchema::Data>().Key(Key)
            .Update<TSchema::Data::Value, TSchema::Data::NoBackupColumn>(Value, NoBackupColumnValue);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TxWriteTwoColumnsNoBackupColumn

struct TTxReplaceRow : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui32 Value;

    TTxReplaceRow(TActorId owner, ui64 key, ui32 value)
        : Owner(owner)
        , Key(key)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        txc.DB.Update(
            TSchema::Data::TableId,
            NTable::ERowOp::Reset,
            { NScheme::TUint64::TInstance(Key) },
            { NIceDb::TUpdateOp(TSchema::Data::Value::ColumnId, NTable::ECellOp::Set, NScheme::TUint32::TInstance(Value)) }
        );

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TTxReplaceRow

struct TxWriteNewColumn : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui32 Value;

    TxWriteNewColumn(TActorId owner, ui64 key, ui32 value)
        : Owner(owner)
        , Key(key)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TNewColumnSchema::Data>().Key(Key)
            .Update<TNewColumnSchema::Data::NewColumn>(Value);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TxWriteNewColumn

struct TTxWriteNoBackupTable : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui32 Value;

    TTxWriteNoBackupTable(TActorId owner, ui64 key, ui32 value)
        : Owner(owner)
        , Key(key)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TSchema::NoBackupTable>().Key(Key)
            .Update<TSchema::NoBackupTable::Value>(Value);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TTxWriteNoBackupTable

struct TTxWriteNoBackupColumn : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui32 Value;

    TTxWriteNoBackupColumn(TActorId owner, ui64 key, ui32 value)
        : Owner(owner)
        , Key(key)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TSchema::Data>().Key(Key)
            .Update<TSchema::Data::NoBackupColumn>(Value);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TTxWriteNoBackupColumn

template <typename TTable>
struct TTxCountRows : public ITransaction {
    enum EEv {
        EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

    struct TEvResult : public TEventLocal<TEvResult, EEv::EvResult> {
        TEvResult(ui64 count)
            : Count(count)
        {}

        ui64 Count;
    };

    const TActorId Owner;
    ui64 Count = 0;

    TTxCountRows(TActorId owner)
        : Owner(owner)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        Count = 0;

        NIceDb::TNiceDb db(txc.DB);

        auto rowSet = db.Table<TTable>().All().Select();
        if (!rowSet.IsReady()) {
            return false;
        }
        while (!rowSet.EndOfSet()) {
            ++Count;
            if (!rowSet.Next()) {
                return false;
            }
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new TEvResult(Count));
    }
}; // TTxCountRows

template<typename TTable, typename TColumn>
struct TTxReadValue : public ITransaction {
    enum EEv {
        EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

    struct TEvResult : public TEventLocal<TEvResult, EEv::EvResult> {
        TEvResult(ui32 value)
            : Value(value)
        {}

        ui32 Value;
    };

    const TActorId Owner;
    const ui64 Key;
    ui32 Value = 0;

    TTxReadValue(TActorId owner, ui64 key)
        : Owner(owner)
        , Key(key)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        auto row = db.Table<TTable>().Key(Key).Select();
        if (!row.IsReady()) {
            return false;
        }
        if (!row.IsValid()) {
            return false;
        }
        Value = row.template GetValueOrDefault<TColumn>();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new TEvResult(Value));
    }
}; // TTxReadValue

struct TTxReadBinaryValue : public ITransaction {
    enum EEv {
        EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

    struct TEvResult : public TEventLocal<TEvResult, EEv::EvResult> {
        TEvResult(const TString& value)
            : Value(value)
        {}

        TString Value;
    };

    const TActorId Owner;
    const ui64 Key;
    TString Value;

    TTxReadBinaryValue(TActorId owner, ui64 key)
        : Owner(owner)
        , Key(key)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        auto row = db.Table<TSchema::Data>().Key(Key).Select();
        if (!row.IsReady()) {
            return false;
        }
        if (!row.IsValid()) {
            return false;
        }
        Value = row.GetValue<TSchema::Data::BinaryValue>();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new TEvResult(Value));
    }
}; // TTxReadBinaryValue

struct TTxReadCompositePK : public ITransaction {
    enum EEv {
        EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

    struct TEvResult : public TEventLocal<TEvResult, EEv::EvResult> {
        TEvResult(ui32 value)
            : Value(value)
        {}

        ui32 Value;
    };

    const TActorId Owner;
    const ui64 Key;
    const ui64 SubKey;
    ui32 Value = 0;

    TTxReadCompositePK(TActorId owner, ui64 key, ui64 subKey)
        : Owner(owner)
        , Key(key)
        , SubKey(subKey)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        auto row = db.Table<TSchema::CompositePKData>().Key(Key, SubKey).Select();
        if (!row.IsReady()) {
            return false;
        }
        if (!row.IsValid()) {
            return false;
        }
        Value = row.GetValueOrDefault<TSchema::CompositePKData::Value>();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new TEvResult(Value));
    }
}; // TTxReadCompositePK

struct TRecoveryStarter : public NFake::TStarter {
    using TBase = NFake::TStarter;

    NFake::TStorageInfo* MakeTabletInfo(ui64 tablet, ui32 channelsCount) override {
        auto* info = TBase::MakeTabletInfo(tablet, channelsCount);
        info->BootType = ETabletBootType::Recovery;
        return info;
    }
}; // TRecoveryStarter

struct TEnv : public TMyEnvBase {
    TEnv()
        : TMyEnvBase()
    {
        Env.SetLogPriority(NKikimrServices::LOCAL_DB_BACKUP, NActors::NLog::PRI_TRACE);
        Env.GetAppData().SystemTabletBackupConfig.MutableFilesystem()->SetPath(Env.GetTempDir());
    }

    template<typename TSchema>
    class TDummy : public NFake::TDummy {
        using TBase = NFake::TDummy;
    public:
        using TBase::TBase;

        TIntrusiveConstPtr<NTable::TBackupExclusion> BackupExclusion() const override {
            return NIceDb::GenerateBackupExclusion<TSchema>();
        }
    };

    void FireDummyTablet(ui32 flags = 0) override
    {
        FireTablet(Edge, Tablet, [this, &flags](const TActorId &tablet, TTabletStorageInfo *info) {
            return new TDummy<TSchema>(tablet, info, Edge, flags);
        });

        WaitFor<NFake::TEvReady>();
    }

    template<typename T = TSchema>
    void InitSchema() {
        SendSync(new NFake::TEvExecute{ new TTxInitSchema<T>(Edge) });
    }

    void InitSchemaWithMigration() {
        SendSync(new NFake::TEvExecute{ new TTxInitSchemaWithMigration(Edge) });
    }

    void WriteValue(ui64 key, ui32 value) {
        SendAsync(new NFake::TEvExecute{ new TTxWriteValue(Edge, key, value) });
        WaitFor<NFake::TEvResult>();
    }

    void EraseRow(ui64 key) {
        SendAsync(new NFake::TEvExecute{ new TTxEraseRow(Edge, key) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteRange(ui64 keyStart, ui64 keyEnd, ui32 value) {
        SendAsync(new NFake::TEvExecute{ new TTxWriteRange(Edge, keyStart, keyEnd, value) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteBinaryValue(ui64 key, const TString& value) {
        SendAsync(new NFake::TEvExecute{ new TTxWriteBinaryValue(Edge, key, value) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteCompositePK(ui64 key, ui64 subKey, ui32 value) {
        SendAsync(new NFake::TEvExecute{ new TTxWriteCompositePK(Edge, key, subKey, value) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteDefaultValue(ui64 key, std::optional<ui32> value) {
        SendAsync(new NFake::TEvExecute{ new TxWriteDefaultValue(Edge, key, value) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteTwoColumns(ui64 key, ui32 value, const TString& binaryValue) {
        SendAsync(new NFake::TEvExecute{ new TxWriteTwoColumns(Edge, key, value, binaryValue) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteTwoColumnsNoBackupColumn(ui64 key, ui32 value, ui32 noBackupColumnValue) {
        SendAsync(new NFake::TEvExecute{ new TxWriteTwoColumnsNoBackupColumn(Edge, key, value, noBackupColumnValue) });
        WaitFor<NFake::TEvResult>();
    }

    void ReplaceRow(ui64 key, ui32 value) {
        SendAsync(new NFake::TEvExecute{ new TTxReplaceRow(Edge, key, value) });
        WaitFor<NFake::TEvResult>();
    }

    void WaitChangelogFlush() {
        Cerr << "...waiting changelog flush" << Endl;
        Env.AdvanceCurrentTime(TDuration::Seconds(5));
        Env.SimulateSleep(TDuration::Seconds(1));
    }

    void WriteNewColumn(ui64 key, ui32 value) {
        SendAsync(new NFake::TEvExecute{ new TxWriteNewColumn(Edge, key, value) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteNoBackupTable(ui64 key, ui32 value) {
        SendAsync(new NFake::TEvExecute{ new TTxWriteNoBackupTable(Edge, key, value) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteNoBackupColumn(ui64 key, ui32 value) {
        SendAsync(new NFake::TEvExecute{ new TTxWriteNoBackupColumn(Edge, key, value) });
        WaitFor<NFake::TEvResult>();
    }

    template<typename TTable>
    ui64 CountRows() {
        using TTxCountRows = TTxCountRows<TTable>;

        SendAsync(new NFake::TEvExecute{ new TTxCountRows(Edge) });

        TAutoPtr<IEventHandle> handle;
        Env.GrabEdgeEventRethrow<typename TTxCountRows::TEvResult>(handle);

        return handle->Get<typename TTxCountRows::TEvResult>()->Count;
    }

    template<typename TColumn>
    ui32 ReadValue(ui64 key) {
        using TTxReadValue = TTxReadValue<TSchema::Data, TColumn>;

        SendAsync(new NFake::TEvExecute{ new TTxReadValue(Edge, key) });

        TAutoPtr<IEventHandle> handle;
        Env.GrabEdgeEventRethrow<typename TTxReadValue::TEvResult>(handle);

        return handle->Get<typename TTxReadValue::TEvResult>()->Value;
    }

    TString ReadBinaryValue(ui64 key) {
        SendAsync(new NFake::TEvExecute{ new TTxReadBinaryValue(Edge, key) });

        TAutoPtr<IEventHandle> handle;
        Env.GrabEdgeEventRethrow<TTxReadBinaryValue::TEvResult>(handle);

        return handle->Get<TTxReadBinaryValue::TEvResult>()->Value;
    }

    ui32 ReadNewColumn(ui64 key) {
        using TTxReadValue = TTxReadValue<TNewColumnSchema::Data, TNewColumnSchema::Data::NewColumn>;

        SendAsync(new NFake::TEvExecute{ new TTxReadValue(Edge, key) });

        TAutoPtr<IEventHandle> handle;
        Env.GrabEdgeEventRethrow<TTxReadValue::TEvResult>(handle);

        return handle->Get<TTxReadValue::TEvResult>()->Value;
    }

    ui32 ReadCompositePK(ui64 key, ui64 subKey) {
        SendAsync(new NFake::TEvExecute{ new TTxReadCompositePK(Edge, key, subKey) });

        TAutoPtr<IEventHandle> handle;
        Env.GrabEdgeEventRethrow<TTxReadCompositePK::TEvResult>(handle);

        return handle->Get<TTxReadCompositePK::TEvResult>()->Value;
    }

    void RestartTabletInRecoveryMode()
    {
        SendSync(new TEvents::TEvPoison, false, true);

        TRecoveryStarter starter;
        FireTablet(Edge, Tablet, &NRecovery::CreateRecoveryShard, 0, &starter);

        // Wait for connectivity
        Env.ConnectToPipe(Tablet, Edge, 0, PipeCfgRetries());
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletPipe::EvServerConnected);
        Env.DispatchEvents(options);
    }

    void RestoreBackup(const TString& backupPath, ui32 TestTabletFlags) {
        Cerr << "...restarting dummy tablet in recovery mode" << Endl;
        RestartTabletInRecoveryMode();

        Cerr << "...restoring backup" << Endl;
        SendAsync(new NRecovery::TEvRestoreBackup(backupPath));

        TAutoPtr<IEventHandle> handle;
        auto result = Env.GrabEdgeEventRethrow<NRecovery::TEvRestoreCompleted>(handle);
        if (result->Error) {
            Cerr << "...restore has error: " << result->Error << Endl;
        }
        UNIT_ASSERT_C(result->Success, "Restore completed with unexpected result, error: " << result->Error);

        Cerr << "...restarting tablet in normal mode" << Endl;
        RestartTablet(TestTabletFlags);
    }

    void RestoreLastBackup(ui32 TestTabletFlags) {
        auto tabletIdDir = TFsPath(Env.GetTempDir())
            .Child("dummy")
            .Child(ToString(Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        RestoreBackup(genDirs.back(), TestTabletFlags);
    }
}; // TEnv

Y_UNIT_TEST_SUITE(Backup) {
    ui32 TestTabletFlags = ui32(NFake::TDummy::EFlg::Backup)
        | ui32(NFake::TDummy::EFlg::Comp)
        | ui32(NFake::TDummy::EFlg::Vac);

    Y_UNIT_TEST(GenerationDirs) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...restarting tablet again" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto dummyDir = TFsPath(env->GetTempDir()).Child("dummy");
        UNIT_ASSERT_C(dummyDir.Exists(), "Tablet type dir isn't created");

        auto tabletIdDir = dummyDir.Child(ToString(env.Tablet));
        UNIT_ASSERT_C(tabletIdDir.Exists(), "Tablet ID dir isn't created");

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        UNIT_ASSERT_C(genDirs.size() == 3, "Every restart must create new generation dir");
    }

    Y_UNIT_TEST(SnapshotIOError) {
        TEnv env;

        TFsPath(env->GetTempDir()).Child("dummy").MkDir(S_IRUSR);
        env->GetAppData().FeatureFlags.SetEnableTabletRestartOnUnhandledExceptions(true);

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);

        Cerr << "...waiting tablet death" << Endl;
        env.WaitForGone(); // crash on IO error
    }

    Y_UNIT_TEST(EmptyData) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto snapshotDir = genDirs.back().Child("snapshot");
        UNIT_ASSERT_C(snapshotDir.Exists(), "Snapshot dir isn't created");

        TVector<TFsPath> tables;
        snapshotDir.List(tables);

        std::erase_if(tables, [](const TFsPath& path) {
            return path.Basename() == "schema.json";
        });

        UNIT_ASSERT(tables.size() == 3);

        for (const auto& table : tables) {
            TString content = TFileInput(table).ReadAll();
            UNIT_ASSERT(content.empty());
        }

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        TString content = TFileInput(changelog).ReadAll();
        UNIT_ASSERT(content.empty());

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 0);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::CompositePKData>(), 0);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(SnapshotData) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...writing two columns" << Endl;
        env.WriteValue(1, 10);
        env.WriteBinaryValue(1, "abcdef");

        Cerr << "...writing two columns simultaneously" << Endl;
        env.WriteTwoColumns(2, 20, "abcdef");

        Cerr << "...erasing row" << Endl;
        env.WriteValue(3, 30);
        env.EraseRow(3);

        Cerr << "...replacing row" << Endl;
        env.WriteDefaultValue(4, 40);
        env.ReplaceRow(4, 40);

        Cerr << "...writing different values in one column" << Endl;
        env.WriteValue(5, 10);
        env.WriteValue(5, 50);

        Cerr << "...writing composite primary key" << Endl;
        env.WriteCompositePK(1, 2, 10);

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto snapshotDir = genDirs.back().Child("snapshot");
        UNIT_ASSERT_C(snapshotDir.Exists(), "Snapshot dir isn't created");

        TVector<TFsPath> tables;
        snapshotDir.List(tables);

        std::erase_if(tables, [](const TFsPath& path) {
            return path.Basename() == "schema.json";
        });

        UNIT_ASSERT(tables.size() == 3);

        {
            auto table = snapshotDir.Child("Data.json");
            UNIT_ASSERT_C(table.Exists(), "Data table isn't created");
            TString content = TFileInput(table).ReadAll();
            UNIT_ASSERT_VALUES_EQUAL(
                content,
                R"({"Key":1,"Value":10,"BinaryValue":"YWJjZGVm","DefaultValue":null})""\n"
                R"({"Key":2,"Value":20,"BinaryValue":"YWJjZGVm","DefaultValue":null})""\n"
                R"({"Key":4,"Value":40,"BinaryValue":null,"DefaultValue":null})""\n"
                R"({"Key":5,"Value":50,"BinaryValue":null,"DefaultValue":null})""\n"
            );
        }

        {
            auto table = snapshotDir.Child("CompositePKData.json");
            UNIT_ASSERT_C(table.Exists(), "CompositePKData table isn't created");
            TString content = TFileInput(table).ReadAll();
            UNIT_ASSERT_VALUES_EQUAL(content, R"({"Key":1,"SubKey":2,"Value":10})""\n");
        }

        {
            auto table = snapshotDir.Child("NoBackupTable.json");
            UNIT_ASSERT_C(table.Exists(), "NoBackupTable table isn't created");
            TString content = TFileInput(table).ReadAll();
            UNIT_ASSERT(content.empty());
        }

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 4);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::CompositePKData>(), 1);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(1), 10);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadBinaryValue(1), "abcdef");

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(2), 20);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadBinaryValue(2), "abcdef");

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(4), 40);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::DefaultValue>(4), 42);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(5), 50);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadCompositePK(1, 2), 10);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(SnapshotLargeData) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...writing large data" << Endl;
        env.WriteRange(0, 1'000'000, 42); // 1 million rows

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto snapshotDir = genDirs.back().Child("snapshot");
        UNIT_ASSERT_C(snapshotDir.Exists(), "Snapshot dir isn't created");

        auto table = snapshotDir.Child("Data.json");
        UNIT_ASSERT_C(table.Exists(), "Data table isn't created");

        TString content = TFileInput(table).ReadAll();
        auto lines = StringSplitter(content).Split('\n').SkipEmpty();
        UNIT_ASSERT_VALUES_EQUAL(lines.Count(), 1'000'000);

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 1'000'000);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(0), 42);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(999'999), 42);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(SnapshotSchema) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto snapshotDir = genDirs.back().Child("snapshot");
        UNIT_ASSERT_C(snapshotDir.Exists(), "Snapshot dir isn't created");

        TVector<TFsPath> schemaFiles;
        snapshotDir.List(schemaFiles);

        std::erase_if(schemaFiles, [](const TFsPath& path) {
            return path.Basename() != "schema.json";
        });

        UNIT_ASSERT(schemaFiles.size() == 1);

        auto schema = snapshotDir.Child("schema.json");
        UNIT_ASSERT_C(schema.Exists(), "Schema file isn't created");

        TString content = TFileInput(schema).ReadAll();
        UNIT_ASSERT_C(content.Contains("\"table_name\":\"Data\""), "Data table isn't in schema");
        UNIT_ASSERT_C(content.Contains("\"table_name\":\"CompositePKData\""), "CompositePKData table isn't in schema");
        UNIT_ASSERT_C(content.Contains("\"table_name\":\"NoBackupTable\""), "NoBackupTable table isn't in schema");

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 0);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::CompositePKData>(), 0);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::NoBackupTable>(), 0);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(ChangelogData) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...writing two columns" << Endl;
        env.WriteValue(1, 10);
        env.WriteBinaryValue(1, "abcdef");

        Cerr << "...writing two columns simultaneously" << Endl;
        env.WriteTwoColumns(2, 20, "abcdef");

        Cerr << "...erasing row" << Endl;
        env.WriteValue(3, 30);
        env.EraseRow(3);

        Cerr << "...replacing row" << Endl;
        env.WriteDefaultValue(4, 40);
        env.ReplaceRow(4, 40);

        Cerr << "...writing different values in one column" << Endl;
        env.WriteValue(5, 10);
        env.WriteValue(5, 50);

        Cerr << "...writing composite primary key" << Endl;
        env.WriteCompositePK(1, 2, 10);

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        env.WaitChangelogFlush();

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        TString content = TFileInput(changelog).ReadAll();
        UNIT_ASSERT_VALUES_EQUAL(
            content,
            R"({"step":4,"data_changes":[{"table":"Data","op":"upsert","Key":1,"Value":10}]})""\n"
            R"({"step":5,"data_changes":[{"table":"Data","op":"upsert","Key":1,"BinaryValue":"YWJjZGVm"}]})""\n"

            R"({"step":6,"data_changes":[{"table":"Data","op":"upsert","Key":2,"Value":20,"BinaryValue":"YWJjZGVm"}]})""\n"

            R"({"step":7,"data_changes":[{"table":"Data","op":"upsert","Key":3,"Value":30}]})""\n"
            R"({"step":8,"data_changes":[{"table":"Data","op":"erase","Key":3}]})""\n"

            R"({"step":9,"data_changes":[{"table":"Data","op":"upsert","Key":4,"DefaultValue":40}]})""\n"
            R"({"step":10,"data_changes":[{"table":"Data","op":"replace","Key":4,"Value":40}]})""\n"

            R"({"step":11,"data_changes":[{"table":"Data","op":"upsert","Key":5,"Value":10}]})""\n"
            R"({"step":12,"data_changes":[{"table":"Data","op":"upsert","Key":5,"Value":50}]})""\n"

            R"({"step":13,"data_changes":[{"table":"CompositePKData","op":"upsert","Key":1,"SubKey":2,"Value":10}]})""\n"
        );

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 4);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::CompositePKData>(), 1);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(1), 10);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadBinaryValue(1), "abcdef");

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(2), 20);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadBinaryValue(2), "abcdef");

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(4), 40);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::DefaultValue>(4), 42);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(5), 50);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadCompositePK(1, 2), 10);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(ChangelogLargeData) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...writing large data in one commit" << Endl;
        env.WriteRange(0, 1'000'000, 42); // 1 million rows

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        env.WaitChangelogFlush();

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        TString content = TFileInput(changelog).ReadAll();
        NJson::TJsonValue json;
        NJson::ReadJsonTree(content, &json);

        UNIT_ASSERT_VALUES_EQUAL(json["step"].GetInteger(), 4);
        UNIT_ASSERT_VALUES_EQUAL(json["data_changes"].GetArray().size(), 1'000'000);

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 1'000'000);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(0), 42);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(999'999), 42);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(ChangelogManyCommits) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...writing data in many commits" << Endl;
        TString data(1'000, 'a'); // make commit large enough
        for (int i = 0; i < 1'000; ++i) {
            env.WriteBinaryValue(i, data);
        }

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        env.WaitChangelogFlush();

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        TString content = TFileInput(changelog).ReadAll();
        auto lines = StringSplitter(content).Split('\n').SkipEmpty();
        UNIT_ASSERT_VALUES_EQUAL(lines.Count(), 1'000);

        // Check state before and after restore
        auto assertState = [&env, &data]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 1'000);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadBinaryValue(0), data);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadBinaryValue(999), data);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(ChangelogSchema) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        env.WaitChangelogFlush();

        TString content = TFileInput(changelog).ReadAll();
        NJson::TJsonValue json;
        NJson::ReadJsonTree(content, &json);

        UNIT_ASSERT_VALUES_EQUAL(json["step"].GetInteger(), 2);
        UNIT_ASSERT_C(json.Has("schema_changes"), "Schema changes must be in changelog");
        UNIT_ASSERT_C(!json.Has("data_changes"), "Unexpected data changes in changelog");

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 0);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::CompositePKData>(), 0);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::NoBackupTable>(), 0);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(ChangelogSchemaAndData) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema with migration" << Endl;
        env.InitSchemaWithMigration();

        Cerr << "...initing new schema without restart" << Endl;
        env.InitSchema<TNewColumnSchema>();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        env.WaitChangelogFlush();

        TString content = TFileInput(changelog).ReadAll();
        auto lines = StringSplitter(content).Split('\n').SkipEmpty().ToList<TString>();

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);

        NJson::TJsonValue json0;
        NJson::ReadJsonTree(lines[0], &json0);
        UNIT_ASSERT_VALUES_EQUAL(json0["step"].GetInteger(), 2);
        UNIT_ASSERT_C(json0.Has("schema_changes"), "Schema changes must be in changelog");
        UNIT_ASSERT_C(json0.Has("data_changes"), "Data changes must be in changelog");

        NJson::TJsonValue json1;
        NJson::ReadJsonTree(lines[1], &json1);
        UNIT_ASSERT_VALUES_EQUAL(json1["step"].GetInteger(), 3);
        UNIT_ASSERT_C(json1.Has("schema_changes"), "Schema changes must be in changelog");
        UNIT_ASSERT_C(!json1.Has("data_changes"), "Unexpected data changes in changelog");

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 1);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::CompositePKData>(), 0);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::NoBackupTable>(), 0);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(1), 42);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(ChangelogSchemaNewColumn) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema<TSchema>();

        Cerr << "...writing data" << Endl;
        env.WriteValue(1, 10);

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema with new column" << Endl;
        env.InitSchema<TNewColumnSchema>();

        Cerr << "...writing data to new column" << Endl;
        env.WriteNewColumn(1, 20);

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        env.WaitChangelogFlush();

        TString content = TFileInput(changelog).ReadAll();
        auto lines = StringSplitter(content).Split('\n').SkipEmpty().ToList<TString>();

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);

        NJson::TJsonValue json;
        NJson::ReadJsonTree(lines[0], &json);
        UNIT_ASSERT_VALUES_EQUAL(json["step"].GetInteger(), 4);
        UNIT_ASSERT_C(json.Has("schema_changes"), "Schema changes must be in changelog");
        UNIT_ASSERT_C(!json.Has("data_changes"), "Unexpected data changes in changelog");

        UNIT_ASSERT_VALUES_EQUAL(
            lines[1],
            R"({"step":5,"data_changes":[{"table":"Data","op":"upsert","Key":1,"NewColumn":20}]})"
        );

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TNewColumnSchema::Data>(), 1);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TNewColumnSchema::CompositePKData>(), 0);
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TNewColumnSchema::NoBackupTable>(), 0);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadNewColumn(1), 20);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(ChangelogTornWrite) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        {
            TFile changelogFile(changelog, OpenExisting | RdWr);
            TString tornWrite = R"({"step":4,"data_changes":[{"table":"Data","op":"upsert","Key":1,"Value":10}]})""\n"
                                R"({"step":5,"data_changes":[{"table":"Data","op":"upsert","Key":2,"Value":20}]})""\n"
                                R"({"step":6,"data_changes":[{"table":"Data","op":"upsert","Key":3)";
            changelogFile.Write(tornWrite.data(), tornWrite.size());
            changelogFile.Flush();
        }

        env.RestoreBackup(genDirs.back(), TestTabletFlags);
        UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 2);
    }

    Y_UNIT_TEST(ExcludeTablet) {
        TEnv env;

        env->GetAppData().SystemTabletBackupConfig.AddExcludeTabletIds(env.Tablet);

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);

        Cerr << "...sleeping while tablet is doing some work" << Endl;
        env->SimulateSleep(TDuration::Seconds(1));

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);

        Cerr << "...sleeping while tablet is doing some work" << Endl;
        env->SimulateSleep(TDuration::Seconds(1));

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        UNIT_ASSERT_C(!tabletIdDir.Exists(), "Tablet is not excluded from backup");
    }

    Y_UNIT_TEST(RecoveryModeKeepsData) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...writing three rows" << Endl;
        env.WriteValue(1, 10);
        env.WriteValue(2, 10);
        env.WriteValue(3, 10);

        UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 3);

        Cerr << "...restarting dummy tablet in recovery mode" << Endl;
        env.RestartTabletInRecoveryMode();

        Cerr << "...restarting tablet in normal mode" << Endl;
        env.RestartTablet(TestTabletFlags);

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 3);
    }

    Y_UNIT_TEST(NoBackupTable) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...writing data to NoBackupTable" << Endl;
        env.WriteNoBackupTable(1, 10);
        env.WriteNoBackupTable(2, 10);

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        env.WaitChangelogFlush();

        TString changelogContent = TFileInput(changelog).ReadAll();
        auto changelogLines = StringSplitter(changelogContent).Split('\n').SkipEmpty();
        UNIT_ASSERT_VALUES_EQUAL_C(changelogLines.Count(), 1, "Must contain only schema changes commit");
        UNIT_ASSERT_C(changelogContent.Contains(R"("table_name":"NoBackupTable")"), "Changelog doesn't contain NoBackupTable schema changes");

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        genDirs.clear();
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto snapshotDir = genDirs.back().Child("snapshot");
        UNIT_ASSERT_C(snapshotDir.Exists(), "Snapshot dir isn't created");

        auto noBackupTable = snapshotDir.Child("NoBackupTable.json");
        UNIT_ASSERT_C(noBackupTable.Exists(), "NoBackupTable table isn't created");
        TString noBackupTableContent = TFileInput(noBackupTable).ReadAll();
        UNIT_ASSERT(noBackupTableContent.empty());

        auto schema = snapshotDir.Child("schema.json");
        UNIT_ASSERT_C(schema.Exists(), "Schema file isn't created");
        auto schemaContent = TFileInput(schema).ReadAll();
        UNIT_ASSERT_C(schemaContent.Contains(R"("table_name":"NoBackupTable")"), "Snapshot doesn't contain NoBackupTable schema");

        // Before restore
        UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::NoBackupTable>(), 2);

        // Restore first backup
        env.RestoreBackup(genDirs.front(), TestTabletFlags);
        UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::NoBackupTable>(), 0);

        // Restore last backup
        env.RestoreBackup(genDirs.back(), TestTabletFlags);
        UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::NoBackupTable>(), 0);
    }

    Y_UNIT_TEST(NoBackupColumn) {
        TEnv env;

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...writing data to NoBackupColumn" << Endl;
        env.WriteValue(1, 10);
        env.WriteNoBackupColumn(1, 10);
        env.WriteTwoColumnsNoBackupColumn(2, 20, 20);

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto changelog = genDirs.back().Child("changelog.json");
        UNIT_ASSERT_C(changelog.Exists(), "Changelog file isn't created");

        env.WaitChangelogFlush();

        TString changelogContent = TFileInput(changelog).ReadAll();
        auto changelogLines = StringSplitter(changelogContent).Split('\n').SkipEmpty().ToList<TString>();
        UNIT_ASSERT_VALUES_EQUAL_C(changelogLines.size(), 3, "Must contain only three commits");
        UNIT_ASSERT_C(
            changelogLines[0].Contains(R"("column_name":"NoBackupColumn")"),
            "Changelog doesn't contain NoBackupColumn schema changes");
        UNIT_ASSERT_VALUES_EQUAL(
            changelogLines[1],
            R"({"step":3,"data_changes":[{"table":"Data","op":"upsert","Key":1,"Value":10}]})"
        );
        UNIT_ASSERT_VALUES_EQUAL(
            changelogLines[2],
            R"({"step":5,"data_changes":[{"table":"Data","op":"upsert","Key":2,"Value":20}]})"
        );

        Cerr << "...restarting tablet" << Endl;
        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        genDirs.clear();
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto snapshotDir = genDirs.back().Child("snapshot");
        UNIT_ASSERT_C(snapshotDir.Exists(), "Snapshot dir isn't created");

        auto table = snapshotDir.Child("Data.json");
        UNIT_ASSERT_C(table.Exists(), "Data table isn't created");
        TString tableContent = TFileInput(table).ReadAll();
        UNIT_ASSERT_VALUES_EQUAL(
            tableContent,
            R"({"Key":1,"Value":10,"BinaryValue":null,"DefaultValue":null})""\n"
            R"({"Key":2,"Value":20,"BinaryValue":null,"DefaultValue":null})""\n"
        );

        auto schema = snapshotDir.Child("schema.json");
        UNIT_ASSERT_C(schema.Exists(), "Schema file isn't created");

        auto schemaContent = TFileInput(schema).ReadAll();
        UNIT_ASSERT_C(schemaContent.Contains(R"("column_name":"NoBackupColumn")"), "Snapshot doesn't contain NoBackupColumn schema");

        // Before restore
        UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(1), 10);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(2), 20);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::NoBackupColumn>(1), 10);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::NoBackupColumn>(2), 20);

        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 2);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(1), 10);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::Value>(2), 20);

            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::NoBackupColumn>(1), 0);
            UNIT_ASSERT_VALUES_EQUAL(env.ReadValue<TSchema::Data::NoBackupColumn>(2), 0);
        };

        // Restore first backup
        env.RestoreBackup(genDirs.front(), TestTabletFlags);
        assertState();

        // Restore last backup
        env.RestoreBackup(genDirs.back(), TestTabletFlags);
        assertState();
    }

    Y_UNIT_TEST(NewSnapshotChangelogSize) {
        TEnv env;

        // Small limit
        env->GetAppData().SystemTabletBackupConfig.SetNewBackupChangelogMinBytes(100);

        Cerr << "...starting tablet" << Endl;
        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...initing schema" << Endl;
        env.InitSchema();

        Cerr << "...writing much data to changelog" << Endl;
        env.WriteRange(0, 1000, 10);

        Cerr << "...waiting for new snapshot" << Endl;
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        Cerr << "...writing little bit more data to changelog" << Endl;
        env.WriteValue(1000, 10);

        env.WaitChangelogFlush();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> backupDirs;
        tabletIdDir.List(backupDirs);

        UNIT_ASSERT_VALUES_EQUAL(backupDirs.size(), 2);

        // Check state before and after restore
        auto assertState = [&env]() {
            UNIT_ASSERT_VALUES_EQUAL(env.CountRows<TSchema::Data>(), 1001);
        };

        assertState();
        env.RestoreLastBackup(TestTabletFlags);
        assertState();
    }
}

} // namespace NKikimr::NTabletFlatExecutor::NBackup
