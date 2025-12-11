#include "flat_backup.h"
#include "flat_boot_cookie.h"
#include "flat_dbase_apply.h"
#include "flat_dbase_scheme.h"
#include "flat_executor_backup.h"
#include "flat_redo_player.h"
#include "flat_row_state.h"
#include "flat_sausage_slicer.h"
#include "flat_update_op.h"
#include "util_deref.h"

#include <ydb/core/base/appdata_fwd.h>
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

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::LOCAL_DB_BACKUP, stream)

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

void WriteJson(TStringBuf in, NJsonWriter::TBuf& out) {
    NJson::TJsonValue value;
    Y_ENSURE(NJson::ReadJsonTree(in, &value));
    out.WriteJsonValue(&value);
}

void WriteColumnToJson(const TString& columnName, NScheme::TTypeId columnType,
                       const TCell& columnData, NJsonWriter::TBuf& writer)
{
    if (columnData.IsNull()) {
        writer.WriteKey(columnName).WriteNull();
        return;
    }

    switch (columnType) {
    case NScheme::NTypeIds::Int32:
        writer.WriteKey(columnName).WriteInt(columnData.AsValue<i32>());
        break;
    case NScheme::NTypeIds::Uint32:
        writer.WriteKey(columnName).WriteULongLong(columnData.AsValue<ui32>());
        break;
    case NScheme::NTypeIds::Int64:
        writer.WriteKey(columnName).WriteLongLong(columnData.AsValue<i64>());
        break;
    case NScheme::NTypeIds::Uint64:
        writer.WriteKey(columnName).WriteULongLong(columnData.AsValue<ui64>());
        break;
    case NScheme::NTypeIds::Uint8:
        writer.WriteKey(columnName).WriteULongLong(columnData.AsValue<ui8>());
        break;
    case NScheme::NTypeIds::Int8:
        writer.WriteKey(columnName).WriteInt(columnData.AsValue<i8>());
        break;
    case NScheme::NTypeIds::Int16:
        writer.WriteKey(columnName).WriteInt(columnData.AsValue<i16>());
        break;
    case NScheme::NTypeIds::Uint16:
        writer.WriteKey(columnName).WriteULongLong(columnData.AsValue<ui16>());
        break;
    case NScheme::NTypeIds::Bool:
        writer.WriteKey(columnName).WriteBool(columnData.AsValue<bool>());
        break;
    case NScheme::NTypeIds::Double:
        writer.WriteKey(columnName).WriteDouble(columnData.AsValue<double>());
        break;
    case NScheme::NTypeIds::Float:
        writer.WriteKey(columnName).WriteFloat(columnData.AsValue<float>());
        break;
    case NScheme::NTypeIds::Date:
        writer.WriteKey(columnName).WriteString(TInstant::Days(columnData.AsValue<ui16>()).ToString());
        break;
    case NScheme::NTypeIds::Datetime:
        writer.WriteKey(columnName).WriteString(TInstant::Seconds(columnData.AsValue<ui32>()).ToString());
        break;
    case NScheme::NTypeIds::Timestamp:
        writer.WriteKey(columnName).WriteString(TInstant::MicroSeconds(columnData.AsValue<ui64>()).ToString());
        break;
    case NScheme::NTypeIds::Interval:
        writer.WriteKey(columnName).WriteLongLong(columnData.AsValue<i64>());
        break;
    case NScheme::NTypeIds::Date32:
        writer.WriteKey(columnName).WriteInt(columnData.AsValue<i32>());
        break;
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
    case NScheme::NTypeIds::Interval64:
        writer.WriteKey(columnName).WriteLongLong(columnData.AsValue<i64>());
        break;
    case NScheme::NTypeIds::Utf8:
        writer.WriteKey(columnName).WriteString(columnData.AsBuf());
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
        writer.WriteKey(columnName)
            .BeginList()
            .WriteULongLong(pair.first)
            .WriteULongLong(pair.second)
            .EndList();
        break;
    }
    case NScheme::NTypeIds::ActorId: {
        auto actorId = columnData.AsValue<TActorId>();
        writer.WriteKey(columnName).WriteString(actorId.ToString());
        break;
    }
    case NScheme::NTypeIds::String:
    default:
        writer.WriteKey(columnName).WriteString(Base64Encode(columnData.AsBuf()));
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

ui64 NewBackupChangelogMinBytes() {
    return AppData()->SystemTabletBackupConfig.GetNewBackupChangelogMinBytes();
}

} // anonymous namespace

class TSnapshotWriter : public TActorBootstrapped<TSnapshotWriter> {
public:
    using TBase = TActorBootstrapped<TSnapshotWriter>;

    struct TTableFile {
        TString Name;
        TFile File;
    };

    TSnapshotWriter(TActorId owner, const TFsPath& path,
                    const THashMap<ui32, TScheme::TTableInfo>& tables,
                    TAutoPtr<TSchemeChanges> schema, TIntrusiveConstPtr<TBackupExclusion> exclusion)
        : Owner(owner)
        , SnapshotPath(path.Child("snapshot"))
        , Schema(schema)
        , Exclusion(exclusion)
    {
        for (const auto& [tableId, table] : tables) {
            Tables.emplace(tableId, TTableFile(table.Name, {}));
        }
    }

    void Bootstrap() {
        LOG_D("Bootstrap for " << SnapshotPath);

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
            WrittenBytes += stringOut.Size();
        } catch (const TIoException& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to create snapshot schema file " << schemaPath << ": " << e.what());
        }

        if (Tables.empty()) {
            return ReplyAndDie();
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

    void ReplyAndDie(bool success = true, const TString& error = "") {
        if (success) {
            Send(Owner, new TEvSnapshotCompleted(WrittenBytes));
        } else {
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
        LOG_D("Handle " << ev->ToString());

        const auto* msg = ev->Get();
        auto it = Tables.find(msg->TableId);
        if (it == Tables.end()) {
            return ReplyAndDie(false, TStringBuilder() << "Got write snapshot for unknown table " << msg->TableId);
        }

        if (!msg->SnapshotData.Empty()) {
            try {
                it->second.File.Write(msg->SnapshotData.Data(), msg->SnapshotData.Size());
                WrittenBytes += msg->SnapshotData.Size();
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
        if (DoneTables.size() == Tables.size()) {
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

            return ReplyAndDie();
        }
    }

    void ScanFailed(const TString& tableName, const TString& error) {
        return ReplyAndDie(false, TStringBuilder() << "Snapshot scan for " << tableName << " failed: " << error);
    }

private:
    TActorId Owner;

    TFsPath SnapshotPath;

    THashMap<ui32, TTableFile> Tables;
    THashSet<ui32> DoneTables;

    TFile SchemaFile;
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

        NJsonWriter::TBuf b(NJsonWriter::HEM_RELAXED, &out);
        b.BeginObject();

        for (const auto& info : Scheme->Cols) {
            const auto& column = Columns.at(info.Tag);
            if (Exclusion && Exclusion->HasColumn(TableId, column.Id)) {
                continue;
            }

            const auto& cell = row.Get(info.Pos);
            WriteColumnToJson(column.Name, column.PType.GetTypeId(), cell, b);
        }

        b.EndObject();
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

    TChangelogSerializer(NJsonWriter::TBuf& writer, const TScheme& schema,
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
            Writer.BeginList();
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
        Writer.BeginObject();

        const auto& table = Schema.Tables.at(tid);
        Writer.WriteKey("table");
        Writer.WriteString(table.Name);

        Writer.WriteKey("op");
        switch (rop) {
            case ERowOp::Absent:
                Y_TABLET_ERROR("Row op is absent");
                break;
            case ERowOp::Upsert:
                Writer.WriteString("upsert");
                break;
            case ERowOp::Erase:
                Writer.WriteString("erase");
                break;
            case ERowOp::Reset:
                Writer.WriteString("replace");
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

        Writer.EndObject();
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
            Writer.EndList();
        }
    }

private:
    NJsonWriter::TBuf& Writer;
    const TScheme& Schema;
    TIntrusiveConstPtr<TBackupExclusion> Exclusion;

    bool HasChanges = false;
    std::function<void()> BeginCommit;
};

class TChangelogWriter : public TActorBootstrapped<TChangelogWriter> {
    struct TEvPrivate {
        enum EEv {
            EvMailboxCleaned = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvFlush,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE));

        struct TEvMailboxCleaned : TEventLocal<TEvMailboxCleaned, EvMailboxCleaned> {};
        struct TEvFlush : TEventLocal<TEvFlush, EvFlush> {
            TEvFlush(ui64 cookie)
                : Cookie(cookie)
            {}

            ui64 Cookie;
        };
    };
public:
    TChangelogWriter(TActorId owner, const TFsPath& path, const TScheme& schema,
                     TIntrusiveConstPtr<TBackupExclusion> exclusion)
        : Owner(owner)
        , ChangelogPath(path.Child("changelog.json"))
        , Schema(schema)
        , Exclusion(exclusion)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap for " << ChangelogPath);

        try {
            ChangelogPath.Parent().MkDirs();
            ChangelogFile = TFile(ChangelogPath, EOpenModeFlag::CreateNew | EOpenModeFlag::WrOnly);
        } catch (const TIoException& e) {
            return ReplyAndDie(TStringBuilder() << "Failed to create changelog file " << ChangelogPath << ": " << e.what());
        }

        Become(&TThis::StateWork);
        Schedule(TDuration::Seconds(5), new TEvPrivate::TEvFlush(++ExpectedFlushCookie));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWriteChangelog, Handle);
            hFunc(TEvPrivate::TEvFlush, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, CleanMailbox);
            cFunc(TEvPrivate::TEvMailboxCleaned::EventType, FlushAndDie);
            hFunc(TEvSnapshotCompleted, Handle);
        }
    }

    void Handle(TEvWriteChangelog::TPtr& ev) {
        LOG_D("Handle " << ev->ToString());

        TBufferOutput out(Buffer);
        NJsonWriter::TBuf b(NJsonWriter::HEM_RELAXED, &out);

        const auto* msg = ev->Get();

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
                b.BeginObject();
                b.WriteKey("step");
                b.WriteULongLong(msg->Step);
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

            b.WriteKey("schema_changes");
            b.BeginList();

            for (const auto& rec : changes.GetDelta()) {
                NJson::TJsonValue value;
                NProtobufJson::Proto2Json(rec, value, {
                    .EnumMode = NProtobufJson::TProto2JsonConfig::EnumName,
                    .FieldNameMode = NProtobufJson::TProto2JsonConfig::FieldNameSnakeCaseDense,
                    .MapAsObject = true,
                });
                b.WriteJsonValue(&value);
            }
            b.EndList();
        }

        if (dataUpdate) {
            try {
                dataUpdate = NPageCollection::TSlicer::Lz4()->Decode(dataUpdate);
                TChangelogSerializer serializer(b, Schema, Exclusion, beginCommit);
                NRedo::TPlayer<TChangelogSerializer> redoPlayer(serializer);
                redoPlayer.Replay(dataUpdate);
                serializer.Finalize();
            } catch (const std::exception& e) {
                return ReplyAndDie(TStringBuilder() << "Failed to serialize commit data: " << e.what());
            }
        }

        if (hasCommit) {
            b.EndObject();
            out << '\n';
        }

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
        LOG_D("Handle " << ev->ToString());

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
            try {
                ChangelogFile.Write(Buffer.data(), Buffer.size());
                ChangelogFile.Flush(); // TODO(pixcc): fsync on parent folder?
                WrittenBytes += Buffer.size();
            } catch (const TIoException& e) {
                return ReplyAndDie(TStringBuilder() << "Failed to write changelog data " << ChangelogFile.GetName() << ": " << e.what());
            }
            Buffer.Clear();

            if (Dying) {
                return;
            }

            if (NeedNewBackup()) {
                Send(Owner, new TEvStartNewBackup);
            }
        }
        Schedule(TDuration::Seconds(5), new TEvPrivate::TEvFlush(++ExpectedFlushCookie));
    }

    void CleanMailbox() {
        Dying = true;
        Send(SelfId(), new TEvPrivate::TEvMailboxCleaned());
    }

    void FlushAndDie() {
        Flush();
        PassAway();
    }

    void ReplyAndDie(const TString& error) {
        Send(Owner, new TEvChangelogFailed(error));
        PassAway();
    }

private:
    TActorId Owner;

    TFsPath ChangelogPath;
    TFile ChangelogFile;

    TScheme Schema;
    TIntrusiveConstPtr<TBackupExclusion> Exclusion;

    TBuffer Buffer;
    ui64 ExpectedFlushCookie = 0;

    bool Dying = false;
    ui64 WrittenBytes = 0;
    std::optional<ui64> SnapshotWrittenBytes;
};

IActor* CreateSnapshotWriter(TActorId owner, const NKikimrConfig::TSystemTabletBackupConfig& config,
                             const THashMap<ui32, TScheme::TTableInfo>& tables,
                             TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation, ui32 step,
                             TAutoPtr<TSchemeChanges> schema, TIntrusiveConstPtr<TBackupExclusion> exclusion)
{
    if (config.HasFilesystem()) {
        auto path = TFsPath(config.GetFilesystem().GetPath())
            .Child(CreateBackupPath(tabletType, tabletId, generation, step));
        return new TSnapshotWriter(owner, path, tables, schema, exclusion);
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
        return new TChangelogWriter(owner, path, schema, exclusion);
    } else {
        return nullptr;
    }
}

} // NKikimr::NTabletFlatExecutor::NBackup

