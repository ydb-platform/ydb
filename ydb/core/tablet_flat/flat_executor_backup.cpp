#include "flat_dbase_scheme.h"
#include "flat_executor_backup.h"
#include "flat_row_state.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <yql/essentials/types/binary_json/read.h>

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

}

class TSnapshotWriter : public TActorBootstrapped<TSnapshotWriter> {
public:
    using TBase = TActorBootstrapped<TSnapshotWriter>;

    struct TTableFile {
        TString Name;
        TFile File;
    };

    TSnapshotWriter(const TFsPath& path, TActorId owner,
                    const THashMap<ui32, TScheme::TTableInfo>& tables,
                    TAutoPtr<NTable::TSchemeChanges> schema)
        : SnapshotPath(path.Child("snapshot"))
        , Owner(owner)
        , Schema(schema)
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
            TUnbufferedFileOutput schemaOut(SchemaFile);
            NProtobufJson::Proto2Json(*Schema, schemaOut, {
                .EnumMode = NProtobufJson::TProto2JsonConfig::EnumName,
                .FieldNameMode = NProtobufJson::TProto2JsonConfig::FieldNameSnakeCaseDense,
                .MapAsObject = true,
            });
        } catch (const TIoException& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to create snapshot schema file " << schemaPath << ": " << e.what());
        }

        if (Tables.empty()) {
            return ReplyAndDie();
        }

        for (auto& [_, table] : Tables) {
            auto tablePath = SnapshotPath.Child(table.Name + ".json");
            try {
                table.File = TFile(tablePath, EOpenModeFlag::CreateNew | EOpenModeFlag::WrOnly);
            } catch (const TIoException& e) {
                return ReplyAndDie(false, TStringBuilder() << "Failed to create table snapshot file " << tablePath << ": " << e.what());
            }
        }

        Become(&TThis::StateWork);
    }

    void ReplyAndDie(bool success = true, const TString& error = "") {
        Send(Owner, new TEvSnapshotCompleted(success, error));
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
    TFsPath SnapshotPath;
    TActorId Owner;

    THashMap<ui32, TTableFile> Tables;
    THashSet<ui32> DoneTables;

    TFile SchemaFile;
    TAutoPtr<NTable::TSchemeChanges> Schema;
};

class TBackupSnapshotScan : public IScan, public TActor<TBackupSnapshotScan> {
public:
    TBackupSnapshotScan(TActorId snapshotWriter, ui32 tableId, const THashMap<ui32, TColumn>& columns)
        : TActor(&TThis::StateWork)
        , SnapshotWriter(snapshotWriter)
        , TableId(tableId)
        , Columns(columns)
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
            const auto& cell = row.Get(info.Pos);

            if (cell.IsNull()) {
                b.WriteKey(Columns.at(info.Tag).Name).WriteNull();
                continue;
            }

            switch (info.TypeInfo.GetTypeId()) {
            case NScheme::NTypeIds::Int32:
                b.WriteKey(Columns.at(info.Tag).Name).WriteInt(cell.AsValue<i32>());
                break;
            case NScheme::NTypeIds::Uint32:
                b.WriteKey(Columns.at(info.Tag).Name).WriteULongLong(cell.AsValue<ui32>());
                break;
            case NScheme::NTypeIds::Int64:
                b.WriteKey(Columns.at(info.Tag).Name).WriteLongLong(cell.AsValue<i64>());
                break;
            case NScheme::NTypeIds::Uint64:
                b.WriteKey(Columns.at(info.Tag).Name).WriteULongLong(cell.AsValue<ui64>());
                break;
            case NScheme::NTypeIds::Uint8:
                b.WriteKey(Columns.at(info.Tag).Name).WriteULongLong(cell.AsValue<ui8>());
                break;
            case NScheme::NTypeIds::Int8:
                b.WriteKey(Columns.at(info.Tag).Name).WriteInt(cell.AsValue<i8>());
                break;
            case NScheme::NTypeIds::Int16:
                b.WriteKey(Columns.at(info.Tag).Name).WriteInt(cell.AsValue<i16>());
                break;
            case NScheme::NTypeIds::Uint16:
                b.WriteKey(Columns.at(info.Tag).Name).WriteULongLong(cell.AsValue<ui16>());
                break;
            case NScheme::NTypeIds::Bool:
                b.WriteKey(Columns.at(info.Tag).Name).WriteBool(cell.AsValue<bool>());
                break;
            case NScheme::NTypeIds::Double:
                b.WriteKey(Columns.at(info.Tag).Name).WriteDouble(cell.AsValue<double>());
                break;
            case NScheme::NTypeIds::Float:
                b.WriteKey(Columns.at(info.Tag).Name).WriteFloat(cell.AsValue<float>());
                break;
            case NScheme::NTypeIds::Date:
                b.WriteKey(Columns.at(info.Tag).Name).WriteULongLong(cell.AsValue<ui16>());
                break;
            case NScheme::NTypeIds::Datetime:
                b.WriteKey(Columns.at(info.Tag).Name).WriteULongLong(cell.AsValue<ui32>());
                break;
            case NScheme::NTypeIds::Timestamp:
                b.WriteKey(Columns.at(info.Tag).Name).WriteULongLong(cell.AsValue<ui64>());
                break;
            case NScheme::NTypeIds::Interval:
                b.WriteKey(Columns.at(info.Tag).Name).WriteLongLong(cell.AsValue<i64>());
                break;
            case NScheme::NTypeIds::Date32:
                b.WriteKey(Columns.at(info.Tag).Name).WriteInt(cell.AsValue<i32>());
                break;
            case NScheme::NTypeIds::Datetime64:
            case NScheme::NTypeIds::Timestamp64:
            case NScheme::NTypeIds::Interval64:
                b.WriteKey(Columns.at(info.Tag).Name).WriteLongLong(cell.AsValue<i64>());
                break;
            case NScheme::NTypeIds::Utf8:
            case NScheme::NTypeIds::Json:
                b.WriteKey(Columns.at(info.Tag).Name).WriteString(cell.AsBuf());
                break;
            case NScheme::NTypeIds::JsonDocument:
                b.WriteKey(Columns.at(info.Tag).Name).WriteString(Base64Encode(NBinaryJson::SerializeToJson(cell.AsBuf())));
                break;
            case NScheme::NTypeIds::PairUi64Ui64: {
                auto pair = cell.AsValue<std::pair<ui64, ui64>>();
                b.WriteKey(Columns.at(info.Tag).Name)
                    .BeginList()
                    .WriteULongLong(pair.first)
                    .WriteULongLong(pair.second)
                    .EndList();
                break;
            }
            case NScheme::NTypeIds::ActorId: {
                auto actorId = cell.AsValue<TActorId>();
                b.WriteKey(Columns.at(info.Tag).Name).WriteString(actorId.ToString());
                break;
            }
            default:
                b.WriteKey(Columns.at(info.Tag).Name).WriteString(Base64Encode(cell.AsBuf()));
                break;
            }
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

    TBuffer Buffer;
    bool InFlight = false;
};

IActor* CreateSnapshotWriter(TActorId owner, const NKikimrConfig::TSystemTabletBackupConfig& config,
                             const THashMap<ui32, TScheme::TTableInfo>& tables,
                             TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation,
                             TAutoPtr<NTable::TSchemeChanges> schema)
{
    if (config.HasFilesystem()) {
        TString tabletTypeName = TTabletTypes::EType_Name(tabletType);
        NProtobufJson::ToSnakeCaseDense(&tabletTypeName);

        auto path = TFsPath(config.GetFilesystem().GetPath())
            .Child(tabletTypeName)
            .Child(ToString(tabletId))
            .Child("gen_" + ToString(generation));
        return new TSnapshotWriter(path, owner, tables, schema);
    } else {
        return nullptr;
    }
}

IScan* CreateSnapshotScan(TActorId snapshotWriter, ui32 tableId, const THashMap<ui32, TColumn>& columns) {
    return new TBackupSnapshotScan(snapshotWriter, tableId, columns);
}

} // NKikimr::NTabletFlatExecutor::NBackup

