#include "flat_executor_ut_common.h"

#include <flat_cxx_database.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

struct TSchema : NIceDb::Schema {
    struct Data : Table<1> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
        struct Value : Column<2, NScheme::NTypeIds::Uint32> { };

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    struct BinaryData : Table<2> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
        struct Value : Column<2, NScheme::NTypeIds::String> { };

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    struct CompositePKData : Table<3> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
        struct SubKey : Column<2, NScheme::NTypeIds::Uint64> { };
        struct Value : Column<3, NScheme::NTypeIds::Uint32> { };

        using TKey = TableKey<Key, SubKey>;
        using TColumns = TableColumns<Key, SubKey, Value>;
    };

    using TTables = SchemaTables<Data, BinaryData, CompositePKData>;
}; // TSchema

struct TTxInitSchema : public ITransaction {
    const TActorId Owner;

    TTxInitSchema(TActorId owner) : Owner(owner) {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb(txc.DB).Materialize<TSchema>();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
}; // TTxInitSchema

struct TTxWriteDataRow : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui32 Value;

    TTxWriteDataRow(TActorId owner, ui64 key, ui32 value)
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
}; // TTxWriteDataRow

struct TTxWriteBinaryDataRow : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const TString Value;

    TTxWriteBinaryDataRow(TActorId owner, ui64 key, const TString& value)
        : Owner(owner)
        , Key(key)
        , Value(value)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<TSchema::BinaryData>().Key(Key)
            .Update<TSchema::BinaryData::Value>(Value);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
}; // TTxWriteBinaryDataRow

struct TTxWriteCompositePKDataRow : public ITransaction {
    const TActorId Owner;
    const ui64 Key;
    const ui64 SubKey;
    const ui32 Value;

    TTxWriteCompositePKDataRow(TActorId owner, ui64 key, ui64 subKey, ui32 value)
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
}; // TTxWriteCompositePKDataRow

struct TEnv : public TMyEnvBase {
    TEnv()
        : TMyEnvBase()
    {
        Env.SetLogPriority(NKikimrServices::LOCAL_DB_BACKUP, NActors::NLog::PRI_TRACE);
        Env.GetAppData().SystemTabletBackupConfig.MutableFilesystem()->SetPath(Env.GetTempDir());
    }

    void InitSchema() {
        SendSync(new NFake::TEvExecute{ new TTxInitSchema(Edge) });
    }

    void WriteRow(ui64 key, ui32 value) {
        SendAsync(new NFake::TEvExecute{ new TTxWriteDataRow(Edge, key, value) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteBinaryRow(ui64 key, const TString& value) {
        SendAsync(new NFake::TEvExecute{ new TTxWriteBinaryDataRow(Edge, key, value) });
        WaitFor<NFake::TEvResult>();
    }

    void WriteCompositePKRow(ui64 key, ui64 subKey, ui32 value) {
        SendAsync(new NFake::TEvExecute{ new TTxWriteCompositePKDataRow(Edge, key, subKey, value) });
        WaitFor<NFake::TEvResult>();
    }

}; // TEnv

Y_UNIT_TEST_SUITE(Backup) {
    ui32 TestTabletFlags = ui32(NFake::TDummy::EFlg::Backup)
        | ui32(NFake::TDummy::EFlg::Comp)
        | ui32(NFake::TDummy::EFlg::Clean);

    Y_UNIT_TEST(FileStructure) {
        TEnv env;

        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto dummyDir = TFsPath(env->GetTempDir()).Child("Dummy");
        UNIT_ASSERT_C(dummyDir, "Tablet type dir isn't created");

        auto tabletIdDir = dummyDir.Child(ToString(env.Tablet));
        UNIT_ASSERT_C(tabletIdDir.Exists(), "Tablet ID dir isn't created");

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        UNIT_ASSERT_C(!genDirs.empty(), "Tablet generation dir isn't created");
    }

    Y_UNIT_TEST(FileStructureAfterRestarts) {
        TEnv env;

        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto dummyDir = TFsPath(env->GetTempDir()).Child("Dummy");
        UNIT_ASSERT_C(dummyDir, "Tablet type dir isn't created");

        auto tabletIdDir = dummyDir.Child(ToString(env.Tablet));
        UNIT_ASSERT_C(tabletIdDir.Exists(), "Tablet ID dir isn't created");

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        UNIT_ASSERT_C(genDirs.size() == 2, "Every restart must create new generation dir");
    }

    Y_UNIT_TEST(SnapshotIOError) {
        TEnv env;

        env->GetAppData().SystemTabletBackupConfig.MutableFilesystem()->SetPath("/dev/null");
        env->GetAppData().FeatureFlags.SetEnableTabletRestartOnUnhandledExceptions(true);

        env.FireDummyTablet(TestTabletFlags);

        env.WaitForGone(); // crash on IO error
    }

    Y_UNIT_TEST(SnapshotEmptyData) {
        TEnv env;

        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();
        env.InitSchema();

        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("Dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto snapshotDir = genDirs.rbegin()->Child("snapshot");
        UNIT_ASSERT_C(snapshotDir.Exists(), "Snapshot dir isn't created");

        TVector<TFsPath> tables;
        snapshotDir.List(tables);
        UNIT_ASSERT(tables.size() == 3);

        for (const auto& table : tables) {
            TString content = TFileInput(table).ReadAll();
            UNIT_ASSERT(content.empty());
        }
    }

    Y_UNIT_TEST(SnapshotData) {
        TEnv env;

        env.FireDummyTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();
        env.InitSchema();

        env.WriteRow(1, 10);
        env.WriteRow(2, 11);
        env.WriteBinaryRow(3, "abcdef");
        env.WriteCompositePKRow(4, 5, 100);

        env.RestartTablet(TestTabletFlags);
        env.WaitFor<NFake::TEvSnapshotBackedUp>();

        auto tabletIdDir = TFsPath(env->GetTempDir())
            .Child("Dummy")
            .Child(ToString(env.Tablet));

        TVector<TFsPath> genDirs;
        tabletIdDir.List(genDirs);

        std::sort(genDirs.begin(), genDirs.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        auto snapshotDir = genDirs.rbegin()->Child("snapshot");
        UNIT_ASSERT_C(snapshotDir.Exists(), "Snapshot dir isn't created");

        TVector<TFsPath> tables;
        snapshotDir.List(tables);
        UNIT_ASSERT(tables.size() == 3);

        std::sort(tables.begin(), tables.end(), [](const TFsPath& a, const TFsPath& b) {
            return a.Basename() < b.Basename();
        });

        {
            UNIT_ASSERT_VALUES_EQUAL(tables[0].Basename(), "BinaryData.json");
            TString content = TFileInput(tables[0]).ReadAll();
            UNIT_ASSERT_VALUES_EQUAL(content, "{\"Key\":3,\"Value\":\"YWJjZGVm\"}\n");
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(tables[1].Basename(), "CompositePKData.json");
            TString content = TFileInput(tables[1]).ReadAll();
            UNIT_ASSERT_VALUES_EQUAL(content, "{\"Key\":4,\"SubKey\":5,\"Value\":100}\n");
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(tables[2].Basename(), "Data.json");
            TString content = TFileInput(tables[2]).ReadAll();
            UNIT_ASSERT_VALUES_EQUAL(
                content,
                "{\"Key\":1,\"Value\":10}\n"
                "{\"Key\":2,\"Value\":11}\n"
            );
        }
    }
}

} // namespace NKikimr::NBackup
