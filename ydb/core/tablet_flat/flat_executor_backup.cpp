#include "flat_executor_backup.h"

#include "flat_backup.h"
#include "flat_boot_cookie.h"
#include "flat_dbase_apply.h"
#include "flat_dbase_scheme.h"
#include "flat_executor_backup_common.h"
#include "flat_redo_player.h"
#include "flat_row_state.h"
#include "flat_sausage_slicer.h"
#include "flat_update_op.h"
#include "util_deref.h"
//to trigger ci

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/io_formats/json/json.h>
#include <ydb/core/util/pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <yql/essentials/types/binary_json/read.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/protobuf/json/util.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/stream/buffer.h>
#include <util/stream/file.h>
#include <util/system/hp_timer.h>

#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::LOCAL_DB_BACKUP, LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::LOCAL_DB_BACKUP, LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::LOCAL_DB_BACKUP, LogPrefix() << stream)

namespace NKikimr::NTabletFlatExecutor::NBackup {

using namespace NTable;

namespace {

EScanStatus ToScanStatus(EStatus status) {
    switch (status) {
        case EStatus::Done:
            return EScanStatus::Done;
        case EStatus::Lost:
            return EScanStatus::Lost;
        case EStatus::Term:
            return EScanStatus::Term;
        case EStatus::StorageError:
            return EScanStatus::StorageError;
        case EStatus::Exception:
            return EScanStatus::Exception;
    }
    return EScanStatus::InProgress;
}

void WriteJson(TStringBuf in, NJson::TJsonWriter& out) {
    NJson::TJsonValue value;
    Y_ENSURE(NJson::ReadJsonTree(in, &value));
    out.Write(&value);
}

void WriteColumnToJson(const TString& columnName, NScheme::TTypeId columnType,
                       const TCell& columnData, NJson::TJsonWriter& writer)
{
    if (columnData.IsNull()) {
        writer.WriteNull(columnName);
        return;
    }

    switch (columnType) {
    case NScheme::NTypeIds::Int32:
        writer.Write(columnName, columnData.AsValue<i32>());
        break;
    case NScheme::NTypeIds::Uint32:
        writer.Write(columnName, columnData.AsValue<ui32>());
        break;
    case NScheme::NTypeIds::Int64:
        writer.Write(columnName, columnData.AsValue<i64>());
        break;
    case NScheme::NTypeIds::Uint64:
        writer.Write(columnName, columnData.AsValue<ui64>());
        break;
    case NScheme::NTypeIds::Uint8:
        writer.Write(columnName, columnData.AsValue<ui8>());
        break;
    case NScheme::NTypeIds::Int8:
        writer.Write(columnName, columnData.AsValue<i8>());
        break;
    case NScheme::NTypeIds::Int16:
        writer.Write(columnName, columnData.AsValue<i16>());
        break;
    case NScheme::NTypeIds::Uint16:
        writer.Write(columnName, columnData.AsValue<ui16>());
        break;
    case NScheme::NTypeIds::Bool:
        writer.Write(columnName, columnData.AsValue<bool>());
        break;
    case NScheme::NTypeIds::Double:
        writer.Write(columnName, columnData.AsValue<double>());
        break;
    case NScheme::NTypeIds::Float:
        writer.Write(columnName, columnData.AsValue<float>());
        break;
    case NScheme::NTypeIds::Date:
        writer.Write(columnName, columnData.AsValue<ui16>());
        break;
    case NScheme::NTypeIds::Datetime:
        writer.Write(columnName, columnData.AsValue<ui32>());
        break;
    case NScheme::NTypeIds::Timestamp:
        writer.Write(columnName, columnData.AsValue<ui64>());
        break;
    case NScheme::NTypeIds::Interval:
        writer.Write(columnName, columnData.AsValue<i64>());
        break;
    case NScheme::NTypeIds::Date32:
        writer.Write(columnName, columnData.AsValue<i32>());
        break;
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
    case NScheme::NTypeIds::Interval64:
        writer.Write(columnName, columnData.AsValue<i64>());
        break;
    case NScheme::NTypeIds::Utf8:
        writer.Write(columnName, columnData.AsBuf());
        break;
    case NScheme::NTypeIds::Json:
        writer.WriteKey(columnName);
        WriteJson(columnData.AsBuf(), writer);
        break;
    case NScheme::NTypeIds::JsonDocument:
        writer.WriteKey(columnName);
        WriteJson(NBinaryJson::SerializeToJson(columnData.AsBuf()), writer);
        break;
    case NScheme::NTypeIds::PairUi64Ui64: {
        auto pair = columnData.AsValue<std::pair<ui64, ui64>>();
        writer.WriteKey(columnName);
        writer.OpenArray();
        writer.Write(pair.first);
        writer.Write(pair.second);
        writer.CloseArray();
        break;
    }
    case NScheme::NTypeIds::String:
    default:
        writer.Write(columnName, Base64Encode(columnData.AsBuf()));
        break;
    }
}

TFsPath CreateBackupPath(TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation, ui32 step) {
    TString tabletTypeName = TTabletTypes::EType_Name(tabletType);
    NProtobufJson::ToSnakeCaseDense(&tabletTypeName);
    TString timestamp = TlsActivationContext->AsActorContext().Now().FormatGmTime("%Y%m%d%H%M%SZ");

    auto path = TFsPath(tabletTypeName)
        .Child(ToString(tabletId))
        .Child(TStringBuilder() << "backup_" << timestamp << "_g" << generation << "_s" << step);

    return path;
}

ui64 MaxBackupsLimit() {
    return AppData()->SystemTabletBackupConfig.GetMaxBackupsLimit();
}

using TGenStep = std::pair<ui32, ui32>;

std::optional<TGenStep> ParseBackupGenStep(const TString& name) {
    auto parts = StringSplitter(name).Split('_').ToList<TStringBuf>();
    if (parts.size() < 4) {
        return std::nullopt;
    }

    if (parts[0] != "backup") {
        return std::nullopt;
    }

    const auto genPart = parts[2];
    const auto stepPart = parts[3];
    if (!genPart.StartsWith('g') || !stepPart.StartsWith('s')) {
        return std::nullopt;
    }

    ui32 gen, step;
    if (!TryFromString(genPart.SubStr(1), gen) || !TryFromString(stepPart.SubStr(1), step)) {
        return std::nullopt;
    }

    return std::make_pair(gen, step);
}

ui64 NewBackupChangelogMinBytes() {
    return AppData()->SystemTabletBackupConfig.GetNewBackupChangelogMinBytes();
}

NJson::TJsonWriterConfig BackupJsonConfig() {
    auto cfg = NFormats::DefaultJsonWriterConfig();
    cfg.SetUnbuffered(true); // buffer is managed by the caller
    return cfg;
}

} // anonymous namespace

class TSnapshotWriter : public TActorBootstrapped<TSnapshotWriter>, public IActorExceptionHandler {
public:
    using TBase = TActorBootstrapped<TSnapshotWriter>;

    TStringBuilder LogPrefix() const {
        return TStringBuilder() << "[" << TabletId << ":" << Generation << ":" << Step << "] ";
    }

    struct TTableFile {
        TString Name;
        TFile File;
        TSha256Hasher Sha256;
    };

    TSnapshotWriter(TActorId owner, const TFsPath& path,
                    const THashMap<ui32, TScheme::TTableInfo>& tables,
                    TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation, ui32 step,
                    TAutoPtr<TSchemeChanges> schema, TIntrusiveConstPtr<TBackupExclusion> exclusion)
        : Owner(owner)
        , BackupPath(path)
        , SnapshotPath(path.Child("snapshot.tmp"))
        , FinalSnapshotPath(path.Child("snapshot"))
        , TabletType(tabletType)
        , TabletId(tabletId)
        , Generation(generation)
        , Step(step)
        , Schema(schema)
        , Exclusion(exclusion)
    {
        for (const auto& [tableId, table] : tables) {
            Tables.emplace(tableId, TTableFile{table.Name, {}, {}});
        }
    }

    void Bootstrap() {
        LOG_N("Starting snapshot" << " Path# " << SnapshotPath);

        DeleteOldBackups();

        try {
            SnapshotPath.MkDirs();
        } catch (const TIoException& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to create snapshot dir " << SnapshotPath << ": " << e.what());
        }

        auto schemaPath = SnapshotPath.Child("schema.json");
        try {
            SchemaFile = TFile(schemaPath, EOpenModeFlag::CreateNew | EOpenModeFlag::WrOnly);
            TStringStream stringOut;
            NProtobufJson::Proto2Json(*Schema, stringOut, {
                .EnumMode = NProtobufJson::TProto2JsonConfig::EnumName,
                .FieldNameMode = NProtobufJson::TProto2JsonConfig::FieldNameSnakeCaseDense,
                .MapAsObject = true,
            });
            SchemaFile.Write(stringOut.Data(), stringOut.Size());
            SchemaSha256.Update(stringOut.Data(), stringOut.Size());
            WrittenBytes += stringOut.Size();
            Send(Owner, new TEvSnapshotStats(stringOut.Size()));
            LOG_D("Schema written" << " Bytes# " << stringOut.Size());
        } catch (const std::exception& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to create snapshot schema file " << schemaPath << ": " << e.what());
        }

        if (Tables.empty()) {
            LOG_D("No tables to scan, finalizing");
            return Finalize();
        }

        for (auto& [tableId, table] : Tables) {
            auto tablePath = SnapshotPath.Child(table.Name + ".json");
            try {
                table.File = TFile(tablePath, EOpenModeFlag::CreateNew | EOpenModeFlag::WrOnly);
            } catch (const TIoException& e) {
                return ReplyAndDie(false, TStringBuilder() << "Failed to create table snapshot file " << tablePath << ": " << e.what());
            }

            if (Exclusion && Exclusion->HasTable(tableId)) {
                ScanDone(tableId); // empty table, no scan here
            }
        }

        Become(&TThis::StateWork);
    }

    void DeleteOldBackups() {
        try {
            const auto backupGenStep = TGenStep{Generation, Step};
    
            TVector<TFsPath> children;
            BackupPath.Parent().List(children);
    
            TVector<std::pair<TGenStep, TFsPath>> backups;
            for (const auto& child : children) {
                auto genStep = ParseBackupGenStep(child.Basename());

                // not a backup directory
                if (!genStep) {
                    continue;
                }
    
                // valid backup directory
                if (child.Child("snapshot").Exists()) {
                    backups.emplace_back(*genStep, child);
                    continue;
                }
    
                // newer backup
                if (genStep >= backupGenStep) {
                    continue;
                }
    
                LOG_N("Deleting incomplete backup" << " Path# " << child);
                child.ForceDelete();
            }

            std::sort(backups.begin(), backups.end(), [](const auto& a, const auto& b) {
                return a.first > b.first; // descending by (generation, step)
            });

            for (size_t i = MaxBackupsLimit(); i < backups.size(); ++i) {
                LOG_N("Deleting old backup" << " Path# " << backups[i].second);
                backups[i].second.ForceDelete();
            }
        } catch (const std::exception& e) {
            LOG_E("Failed to delete old backups" << " Path# " << BackupPath << " Error# " << e.what());
        }
    }

    void ReplyAndDie(bool success = true, const TString& error = "") {
        if (success) {
            Send(Owner, new TEvSnapshotCompleted(WrittenBytes));
        } else {
            LOG_E("Snapshot failed" << " Error# " << error);
            Send(Owner, new TEvSnapshotCompleted(error));
        }

        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWriteSnapshot, Handle);
        }
    }

    void Handle(TEvWriteSnapshot::TPtr& ev) {
        const auto* msg = ev->Get();
        LOG_D("Writing snapshot" << " TableId# " << msg->TableId << " Bytes# " << msg->SnapshotData.Size());

        auto it = Tables.find(msg->TableId);
        if (it == Tables.end()) {
            return ReplyAndDie(false, TStringBuilder() << "Got write snapshot for unknown table " << msg->TableId);
        }

        if (!msg->SnapshotData.Empty()) {
            try {
                it->second.File.Write(msg->SnapshotData.Data(), msg->SnapshotData.Size());
                it->second.Sha256.Update(msg->SnapshotData.Data(), msg->SnapshotData.Size());
                WrittenBytes += msg->SnapshotData.Size();
                Send(Owner, new TEvSnapshotStats(msg->SnapshotData.Size()));
            } catch (const TIoException& e) {
                return ReplyAndDie(false, TStringBuilder() << "Failed to write snapshot table data " << it->second.File.GetName() << ": " << e.what());
            }
        }

        switch (msg->ScanStatus) {
            case EScanStatus::InProgress:
                return ContinueScan(ev->Sender);
            case EScanStatus::Done:
                return ScanDone(msg->TableId);
            case EScanStatus::Lost:
                return ScanFailed(it->second.Name, "Owner entity is lost");
            case EScanStatus::Term:
                return ScanFailed(it->second.Name, "Explicit process termination by owner");
            case EScanStatus::StorageError:
                return ScanFailed(it->second.Name, "Some blob has been failed to load");
            case EScanStatus::Exception:
                return ScanFailed(it->second.Name, "Unhandled exception has happened");
        }
    }

    void ContinueScan(TActorId scan) const {
        Send(scan, new TEvWriteSnapshotAck());
    }

    void ScanDone(ui32 tableId) {
        DoneTables.insert(tableId);
        LOG_D("Table scan done" << " Done# " << DoneTables.size() << " Total# " << Tables.size());
        if (DoneTables.size() == Tables.size()) {
            return Finalize();
        }
    }

    static NJson::TJsonValue MakeFileEntry(const TString& name, TSha256Hasher& hasher) {
        NJson::TJsonValue entry;
        entry["name"] = name;
        entry["sha256"] = hasher.Final();
        return entry;
    }

    void Finalize() {
        try {
            SchemaFile.Flush();
        } catch (const TIoException& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to flush snapshot schema " << SchemaFile.GetName() << ": " << e.what());
        }

        for (auto& [_, table] : Tables) {
            try {
                table.File.Flush();
            } catch (const TIoException& e) {
                return ReplyAndDie(false, TStringBuilder() << "Failed to flush snapshot table data " << table.File.GetName() << ": " << e.what());
            }
        }

        try {
            NJson::TJsonValue manifest;
            TString tabletTypeName = TTabletTypes::EType_Name(TabletType);
            NProtobufJson::ToSnakeCaseDense(&tabletTypeName);
            manifest["tablet_type"] = tabletTypeName;
            manifest["tablet_id"] = TabletId;
            manifest["generation"] = Generation;
            manifest["step"] = Step;

            auto& files = manifest["files"];
            files.SetType(NJson::JSON_ARRAY);
            files.AppendValue(MakeFileEntry("schema.json", SchemaSha256));
            for (auto& [_, table] : Tables) {
                files.AppendValue(MakeFileEntry(table.Name + ".json", table.Sha256));
            }

            const TString manifestStr = NJson::WriteJson(manifest, /*formatOutput=*/ false);

            auto manifestPath = SnapshotPath.Child("manifest.json");
            TFile manifestFile(manifestPath, EOpenModeFlag::CreateNew | EOpenModeFlag::WrOnly);
            manifestFile.Write(manifestStr.data(), manifestStr.size());
            manifestFile.Flush();

            const TString manifestChecksum = TSha256Hasher::Hash(manifestStr);

            auto checksumPath = SnapshotPath.Child("manifest.json.sha256");
            TFile checksumFile(checksumPath, EOpenModeFlag::CreateNew | EOpenModeFlag::WrOnly);
            checksumFile.Write(manifestChecksum.data(), manifestChecksum.size());
            checksumFile.Flush();

            Send(Owner, new TEvSnapshotStats(manifestStr.size() + manifestChecksum.size()));
        } catch (const std::exception& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to write manifest: " << e.what());
        }

        try {
            TFile snapshotDir(SnapshotPath, EOpenModeFlag::RdOnly);
            snapshotDir.Flush();
        } catch (const TIoException& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to flush temporary snapshot dir " << SnapshotPath << ": " << e.what());
        }

        try {
            SnapshotPath.RenameTo(FinalSnapshotPath);
        } catch (const TIoException& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to rename snapshot " << SnapshotPath << " to " << FinalSnapshotPath << ": " << e.what());
        }

        try {
            TFile parentDir(FinalSnapshotPath.Parent(), EOpenModeFlag::RdOnly);
            parentDir.Flush();
        } catch (const TIoException& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to flush parent dir after rename " << FinalSnapshotPath.Parent() << ": " << e.what());
        }

        DeleteOldBackups();

        LOG_N("Snapshot finalized" << " Bytes# " << WrittenBytes);
        return ReplyAndDie();
    }

    void ScanFailed(const TString& tableName, const TString& error) {
        return ReplyAndDie(false, TStringBuilder() << "Snapshot scan for " << tableName << " failed: " << error);
    }

    bool OnUnhandledException(const std::exception& exc) override {
        ReplyAndDie(false, TStringBuilder() << "Unhandled exception: " << exc.what());
        return true;
    }

private:
    TActorId Owner;

    TFsPath BackupPath;
    TFsPath SnapshotPath;
    TFsPath FinalSnapshotPath;

    TTabletTypes::EType TabletType;
    ui64 TabletId;
    ui32 Generation;
    ui32 Step;

    THashMap<ui32, TTableFile> Tables;
    THashSet<ui32> DoneTables;

    TFile SchemaFile;
    TSha256Hasher SchemaSha256;
    TAutoPtr<TSchemeChanges> Schema;

    TIntrusiveConstPtr<TBackupExclusion> Exclusion;

    ui64 WrittenBytes = 0;
};

class TBackupSnapshotScan : public IScan, public TActor<TBackupSnapshotScan> {
public:
    TBackupSnapshotScan(TActorId snapshotWriter, ui32 tableId, const THashMap<ui32, TColumn>& columns,
                        TIntrusiveConstPtr<TBackupExclusion> exclusion)
        : TActor(&TThis::StateWork)
        , SnapshotWriter(snapshotWriter)
        , TableId(tableId)
        , Columns(columns)
        , Exclusion(exclusion)
    {}

    void Describe(IOutputStream& o) const override {
        o << "BackupSnapshotScan";
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) override {
        TlsActivationContext->AsActorContext().RegisterWithSameMailbox(this);

        Driver = driver;
        Scheme = scheme;

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64) override {
        lead.To(Scheme->Tags(), {}, ESeek::Lower);
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell>, const TRow& row) override {
        TBufferOutput out(Buffer);

        NJson::TJsonWriter writer(&out, BackupJsonConfig());
        writer.OpenMap();

        for (const auto& info : Scheme->Cols) {
            const auto& column = Columns.at(info.Tag);
            if (Exclusion && Exclusion->HasColumn(TableId, column.Id)) {
                continue;
            }

            const auto& cell = row.Get(info.Pos);

            try {
                WriteColumnToJson(column.Name, column.PType.GetTypeId(), cell, writer);
            } catch (const std::exception& e) {
                TString value;
                DbgPrintValue(value, cell, column.PType);
        
                throw yexception() << "Failed to write column to JSON: " << e.what()
                    << " Column# " << column.Name
                    << " Type# " << NScheme::TypeName(column.PType.GetTypeId(), "")
                    << " Value# " << value;
            }
        }

        writer.CloseMap();
        out << '\n';

        if (Buffer.Size() >= 1_MB) {
            SendBuffer();
        }

        return MaybeContinue();
    }

    void Handle(TEvWriteSnapshotAck::TPtr&) {
        InFlight = false;
        Driver->Touch(MaybeContinue());
    }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        SendBuffer(ToScanStatus(status));
        PassAway();
        return nullptr;
    }

    EScan Exhausted() override {
        return EScan::Final;
    }

    void SendBuffer(EScanStatus status = EScanStatus::InProgress) {
        InFlight = true;
        Send(SnapshotWriter, new TEvWriteSnapshot(TableId, std::move(Buffer), status));
    }

    EScan MaybeContinue() const {
        if (!InFlight) {
            return EScan::Feed;
        }  else {
            return EScan::Sleep;
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWriteSnapshotAck, Handle);
        }
    }

private:
    IDriver* Driver = nullptr;
    TIntrusiveConstPtr<TScheme> Scheme;

    TActorId SnapshotWriter;
    ui32 TableId;
    THashMap<ui32, TColumn> Columns;
    TIntrusiveConstPtr<TBackupExclusion> Exclusion;

    TBuffer Buffer;
    bool InFlight = false;
};

class TChangelogSerializer {
public:
    using TKeys = TArrayRef<const TRawTypeValue>;
    using TOps = TArrayRef<const TUpdateOp>;

    TChangelogSerializer(NJson::TJsonWriter& writer, const TScheme& schema,
                         TIntrusiveConstPtr<TBackupExclusion> exclusion,
                         const std::function<void()>& beginCommit)
        : Writer(writer)
        , Schema(schema)
        , Exclusion(exclusion)
        , BeginCommit(beginCommit)
    {}

    bool NeedIn(ui32) noexcept
    {
        return true;
    }

    void DoBegin(ui32, ui32, ui64, ui64)
    {
        // ignore
    }

    void DoAnnex(TArrayRef<const TStdPad<NPageCollection::TGlobId>>)
    {
        Y_TABLET_ERROR("Annex is unsupported");
    }

    void BeginChanges() {
        if (!HasChanges) {
            HasChanges = true;
            Writer.WriteKey("data_changes");
            Writer.OpenArray();
        }
    }

    bool NoOps(ui32 tid, TOps ops) const {
        for (const auto& op : ops) {
            if (!Exclusion || !Exclusion->HasColumn(tid, op.Tag)) {
                return false;
            }
        }
        return true;
    }

    void DoUpdate(ui32 tid, ERowOp rop, TKeys key, TOps ops, TRowVersion)
    {
        if (Exclusion && Exclusion->HasTable(tid)) {
            return;
        }

        if (NoOps(tid, ops) && !TCellOp::HaveNoOps(rop)) {
            return; // ignore data changes that contain only excluded columns
        }

        BeginCommit();
        BeginChanges();
        Writer.OpenMap();

        const auto& table = Schema.Tables.at(tid);
        Writer.Write("table", table.Name);

        Writer.WriteKey("op");
        switch (rop) {
            case ERowOp::Absent:
                Y_TABLET_ERROR("Row op is absent");
                break;
            case ERowOp::Upsert:
                Writer.Write("upsert");
                break;
            case ERowOp::Erase:
                Writer.Write("erase");
                break;
            case ERowOp::Reset:
                Writer.Write("replace");
                break;
        }

        for (size_t i = 0; i < table.KeyColumns.size(); ++i) {
            ui32 columnId = table.KeyColumns[i];
            const auto& column = table.Columns.at(columnId);
            WriteColumnToJson(column.Name, column.PType.GetTypeId(), key[i].AsRef(), Writer);
        }

        for (const auto& op : ops) {
            const ui32 columnId = op.Tag;
            if (Exclusion && Exclusion->HasColumn(table.Id, columnId)) {
                continue;
            }

            const auto& column = table.Columns.at(columnId);
            switch (ECellOp(op.Op)) {
                case ECellOp::Empty:
                    Y_TABLET_ERROR("Cell op is empty");
                    break;
                case ECellOp::Set:
                    WriteColumnToJson(column.Name, column.PType.GetTypeId(), op.AsCell(), Writer);
                    break;
                case ECellOp::Null:
                case ECellOp::Reset:
                    WriteColumnToJson(column.Name, column.PType.GetTypeId(), column.Null, Writer);
                    break;
            }
        }

        Writer.CloseMap();
    }

    void DoUpdateTx(ui32, ERowOp, TKeys, TOps, ui64)
    {
        Y_TABLET_ERROR("UpdateTx is unsupported");
    }

    void DoCommitTx(ui32, ui64, TRowVersion)
    {
        Y_TABLET_ERROR("CommitTx is unsupported");
    }

    void DoRemoveTx(ui32, ui64)
    {
        Y_TABLET_ERROR("RemoveTx is unsupported");
    }

    void DoFlush(ui32, ui64, TEpoch)
    {
        // ignore
    }

    void DoLockRowTx(ui32, ELockMode, TKeys, ui64)
    {
        // ignore
    }

    void Finalize()
    {
        if (HasChanges) {
            Writer.CloseArray();
        }
    }

private:
    NJson::TJsonWriter& Writer;
    const TScheme& Schema;
    TIntrusiveConstPtr<TBackupExclusion> Exclusion;

    bool HasChanges = false;
    std::function<void()> BeginCommit;
};

class TChangelogWriter : public TActorBootstrapped<TChangelogWriter>, public IActorExceptionHandler {
    struct TEvPrivate {
        enum EEv {
            EvFlush = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE));

        struct TEvFlush : TEventLocal<TEvFlush, EvFlush> {
            TEvFlush(ui64 cookie)
                : Cookie(cookie)
            {}

            ui64 Cookie;
        };
    };
public:
    TChangelogWriter(TActorId owner, const TFsPath& path, const TScheme& schema,
                     TIntrusiveConstPtr<TBackupExclusion> exclusion,
                     ui64 tabletId, ui32 generation, ui32 step)
        : Owner(owner)
        , ChangelogPath(path.Child("changelog.json"))
        , ChangelogChecksumPath(path.Child("changelog.json.sha256"))
        , Schema(schema)
        , Exclusion(exclusion)
        , TabletId(tabletId)
        , Generation(generation)
        , Step(step)
    {}

    TStringBuilder LogPrefix() const {
        return TStringBuilder() << "[" << TabletId << ":" << Generation << ":" << Step << "] ";
    }

    void Bootstrap() {
        LOG_N("Starting changelog" << " Path# " << ChangelogPath);

        try {
            ChangelogPath.Parent().MkDirs();
            ChangelogFile = TFile(ChangelogPath, EOpenModeFlag::CreateNew | EOpenModeFlag::WrOnly);
        } catch (const TIoException& e) {
            return ReplyAndDie(TStringBuilder() << "Failed to create changelog file " << ChangelogPath << ": " << e.what());
        }

        try {
            WriteChangelogChecksum();
        } catch (const std::exception& e) {
            return ReplyAndDie(TStringBuilder() << "Failed to write changelog checksum " << ChangelogChecksumPath << ": " << e.what());
        }

        Become(&TThis::StateWork);
        Schedule(TDuration::Seconds(5), new TEvPrivate::TEvFlush(++ExpectedFlushCookie));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWriteChangelog, Handle);
            hFunc(TEvPrivate::TEvFlush, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, FlushAndDie);
            hFunc(TEvSnapshotCompleted, Handle);
        }
    }

    void Handle(TEvWriteChangelog::TPtr& ev) {
        size_t changesStart = Buffer.Size();
        TBufferOutput out(Buffer);
        NJson::TJsonWriter writer(&out, BackupJsonConfig());

        const auto* msg = ev->Get();
        const ui64 msgSize = msg->GetTotalSize();
        LOG_D("Writing changelog" << " Step# " << msg->Step << " Bytes# " << msgSize);

        TString dataUpdate;
        TString schemeUpdate;

        if (!msg->EmbeddedLogBody.empty()) {
            if (!msg->References.empty()) {
                return ReplyAndDie("There are both embedded log body and references");
            }
            dataUpdate = msg->EmbeddedLogBody;
        } else {
            for (const auto& ref : msg->References) {
                const TLogoBlobID& id = ref.Id;
                const TString& body = ref.Buffer;

                const NBoot::TCookie cookie(id.Cookie());
                if (cookie.Type() != NBoot::TCookie::EType::Log) {
                    continue; // skip
                }

                switch (cookie.Index()) {
                case NBoot::TCookie::EIdx::RedoLz4:
                    if (dataUpdate)
                        dataUpdate.append(body);
                    else
                        dataUpdate = body;
                    break;
                case NBoot::TCookie::EIdx::Alter:
                    if (schemeUpdate)
                        schemeUpdate.append(body);
                    else
                        schemeUpdate = body;
                    break;
                default:
                    continue; // skip
                }
            }
        }

        // Commit could be not included in backup
        bool hasCommit = false;
        auto beginCommit = [&](){
            if (!hasCommit) {
                hasCommit = true;
                writer.OpenMap();
                writer.Write("step", msg->Step);
            }
        };

        if (schemeUpdate) {
            beginCommit();

            TSchemeChanges changes;
            bool parseOk = ParseFromStringNoSizeLimit(changes, schemeUpdate);
            if (!parseOk) {
                return ReplyAndDie("Can't parse scheme update from proto");
            }

            TSchemeModifier modifier(Schema);
            modifier.Apply(changes);

            writer.WriteKey("schema_changes");
            writer.OpenArray();

            for (const auto& rec : changes.GetDelta()) {
                NJson::TJsonValue value;
                NProtobufJson::Proto2Json(rec, value, {
                    .EnumMode = NProtobufJson::TProto2JsonConfig::EnumName,
                    .FieldNameMode = NProtobufJson::TProto2JsonConfig::FieldNameSnakeCaseDense,
                    .MapAsObject = true,
                });
                writer.Write(value);
            }
            writer.CloseArray();
        }

        if (dataUpdate) {
            try {
                dataUpdate = NPageCollection::TSlicer::Lz4()->Decode(dataUpdate);
                TChangelogSerializer serializer(writer, Schema, Exclusion, beginCommit);
                NRedo::TPlayer<TChangelogSerializer> redoPlayer(serializer);
                redoPlayer.Replay(dataUpdate);
                serializer.Finalize();
            } catch (const std::exception& e) {
                return ReplyAndDie(TStringBuilder() << "Failed to serialize commit data: " << e.what());
            }
        }

        if (hasCommit) {
            writer.Write("prev_sha256", Checksum.Intermediate());
            writer.CloseMap();
            out << '\n';

            size_t changesSize = Buffer.Size() - changesStart;
            Checksum.Update(Buffer.data() + changesStart, changesSize);
            if (!BufferCreatedAt) {
                BufferCreatedAt = msg->CreatedAt;
            }
        }

        Send(Owner, new TEvWriteChangelogAck(msgSize));

        if (Buffer.Size() >= 1_MB) {
            Flush();
        }
    }

    void Handle(TEvSnapshotCompleted::TPtr& ev) {
        if (ev->Get()->Success) {
            SnapshotWrittenBytes = ev->Get()->WrittenBytes;
        } else {
            PassAway();
        }
    }

    void Handle(TEvPrivate::TEvFlush::TPtr& ev) {
        if (ev->Get()->Cookie == ExpectedFlushCookie) {
            Flush();
        }
    }

    bool NeedNewBackup() const {
        return SnapshotWrittenBytes.has_value()
            && WrittenBytes >= SnapshotWrittenBytes
            && WrittenBytes >= NewBackupChangelogMinBytes();
    }

    void Flush() {
        if (!Buffer.Empty()) {
            THPTimer timer;
            try {
                ChangelogFile.Write(Buffer.data(), Buffer.size());
                ChangelogFile.Flush();
                WrittenBytes += Buffer.size();
            } catch (const TIoException& e) {
                return ReplyAndDie(TStringBuilder() << "Failed to write changelog data " << ChangelogFile.GetName() << ": " << e.what());
            }
            TDuration flushLatency = TDuration::Seconds(timer.Passed());

            ui64 flushedBytes = Buffer.size();
            Buffer.Clear();

            Y_ENSURE(BufferCreatedAt);
            TDuration lag = TActivationContext::Monotonic() - *BufferCreatedAt;
            BufferCreatedAt = std::nullopt;

            LOG_D("Flushed" << " Bytes# " << flushedBytes << " TotalBytes# " << WrittenBytes << " Lag# " << lag);
            Send(Owner, new TEvChangelogStats(flushedBytes, flushLatency, lag));

            try {
                WriteChangelogChecksum();
            } catch (const std::exception& e) {
                return ReplyAndDie(TStringBuilder() << "Failed to write changelog checksum " << ChangelogChecksumPath << ": " << e.what());
            }

            if (Dying) {
                return;
            }

            if (NeedNewBackup()) {
                LOG_N("Requesting new backup" << " ChangelogBytes# " << WrittenBytes << " SnapshotBytes# " << *SnapshotWrittenBytes);
                Send(Owner, new TEvStartNewBackup());
            }
        }
        Schedule(TDuration::Seconds(5), new TEvPrivate::TEvFlush(++ExpectedFlushCookie));
    }

    void FlushAndDie() {
        Dying = true;
        Flush();
        LOG_N("Everything is flushed, shutting down");
        PassAway();
    }

    void ReplyAndDie(const TString& error) {
        if (!Dying) {
            LOG_E("Changelog failed" << " Error# " << error);
            Send(Owner, new TEvChangelogFailed(error));
            PassAway();
        }
    }

    void WriteChangelogChecksum() {
        TFsPath tmpPath(ChangelogChecksumPath.GetPath() + ".tmp");
        TFileOutput out(tmpPath);
        out.Write(Checksum.Intermediate());
        out.Flush();
        tmpPath.RenameTo(ChangelogChecksumPath);
        TFile(ChangelogChecksumPath.Parent(), EOpenModeFlag::RdOnly).Flush();
    }

    bool OnUnhandledException(const std::exception& exc) override {
        ReplyAndDie(TStringBuilder() << "Unhandled exception: " << exc.what());
        return true;
    }

private:
    TActorId Owner;

    TFsPath ChangelogPath;
    TFsPath ChangelogChecksumPath;
    TFile ChangelogFile;

    TScheme Schema;
    TIntrusiveConstPtr<TBackupExclusion> Exclusion;

    const ui64 TabletId;
    const ui32 Generation;
    const ui32 Step;

    TBuffer Buffer;
    ui64 ExpectedFlushCookie = 0;

    bool Dying = false;
    ui64 WrittenBytes = 0;
    std::optional<ui64> SnapshotWrittenBytes;

    TSha256Hasher Checksum;
    std::optional<TMonotonic> BufferCreatedAt;
};

IActor* CreateSnapshotWriter(TActorId owner, const NKikimrConfig::TSystemTabletBackupConfig& config,
                             const THashMap<ui32, TScheme::TTableInfo>& tables,
                             TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation, ui32 step,
                             TAutoPtr<TSchemeChanges> schema, TIntrusiveConstPtr<TBackupExclusion> exclusion)
{
    if (config.HasFilesystem()) {
        auto path = TFsPath(config.GetFilesystem().GetPath())
            .Child(CreateBackupPath(tabletType, tabletId, generation, step));
        return new TSnapshotWriter(owner, path, tables, tabletType, tabletId, generation, step, schema, exclusion);
    } else {
        return nullptr;
    }
}

IScan* CreateSnapshotScan(TActorId snapshotWriter, ui32 tableId, const THashMap<ui32, TColumn>& columns,
                          TIntrusiveConstPtr<TBackupExclusion> exclusion)
{
    return new TBackupSnapshotScan(snapshotWriter, tableId, columns, exclusion);
}

IActor* CreateChangelogWriter(TActorId owner, const NKikimrConfig::TSystemTabletBackupConfig& config,
                              TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation, ui32 step,
                              const TScheme& schema, TIntrusiveConstPtr<TBackupExclusion> exclusion)
{
    if (config.HasFilesystem()) {
        auto path = TFsPath(config.GetFilesystem().GetPath())
            .Child(CreateBackupPath(tabletType, tabletId, generation, step));
        return new TChangelogWriter(owner, path, schema, exclusion, tabletId, generation, step);
    } else {
        return nullptr;
    }
}

} // NKikimr::NTabletFlatExecutor::NBackup

