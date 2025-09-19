#include "flat_dbase_scheme.h"
#include "flat_executor_backup.h"
#include "flat_row_state.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <yql/essentials/types/binary_json/read.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/stream/buffer.h>

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::LOCAL_DB_BACKUP, stream)

namespace NKikimr::NTabletFlatExecutor::NBackup {

using namespace NTable;

class TSnapshotWriter : public TActorBootstrapped<TSnapshotWriter> {
public:
    using TBase = TActorBootstrapped<TSnapshotWriter>;

    struct TTableFile {
        TString Name;
        TFile File;
    };

    TSnapshotWriter(const TFsPath& path, TActorId owner, const THashMap<ui32, TScheme::TTableInfo>& tables)
        : SnapshotPath(path.Child("snapshot"))
        , Owner(owner)
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
            hFunc(TEvCompleteSnapshot, Handle);
        }
    }

    void Handle(TEvWriteSnapshot::TPtr& ev) {
        LOG_D("Handle " << ev->ToString());

        const auto* msg = ev->Get();
        auto it = Tables.find(msg->TableId);
        if (it == Tables.end()) {
            return ReplyAndDie(false, TStringBuilder() << "Got write snapshot for unknown table " << msg->TableId);
        }

        try {
            it->second.File.Write(msg->SnapshotData.Data(), msg->SnapshotData.Size());
            it->second.File.Flush();
        } catch (const TIoException& e) {
            return ReplyAndDie(false, TStringBuilder() << "Failed to write snapshot table data " << it->second.File.GetName() << ": " << e.what());
        }
    }

    void Handle(TEvCompleteSnapshot::TPtr& ev) {
        LOG_D("Handle " << ev->ToString());

        const auto* msg = ev->Get();
        auto it = Tables.find(msg->TableId);
        if (it == Tables.end()) {
            return ReplyAndDie(false, TStringBuilder() << "Got complete snapshot for unknown table " << msg->TableId);
        }

        if (msg->Success) {
            DoneTables.insert(msg->TableId);
            if (DoneTables.size() == Tables.size()) {
                return ReplyAndDie();
            }
        } else {
            return ReplyAndDie(false, TStringBuilder() << "Snapshot scan for " << it->second.File.GetName() << " failed: " << msg->Error);
        }
    }

private:
    TFsPath SnapshotPath;
    TActorId Owner;

    THashMap<ui32, TTableFile> Tables;
    THashSet<ui32> DoneTables;
};

class TBackupSnapshotScan : public IScan {
public:
    TBackupSnapshotScan(TActorId snapshotWriter, ui32 tableId, const THashMap<ui32, TColumn>& columns)
        : SnapshotWriter(snapshotWriter)
        , TableId(tableId)
        , Columns(columns)
    {}

    void Describe(IOutputStream& o) const override {
        o << "BackupSnapshotScan";
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) override {
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

        NJsonWriter::TBuf b;
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
            case NScheme::NTypeIds::String:
            case NScheme::NTypeIds::String4k:
            case NScheme::NTypeIds::String2m:
            case NScheme::NTypeIds::Yson:
                b.WriteKey(Columns.at(info.Tag).Name).WriteString(Base64Encode(cell.AsBuf()));
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
                Y_ENSURE(false, "Unsupported type");
            }
        }

        b.EndObject();

        out << b.Str() << '\n';

        if (Buffer.Size() >= 4_KB) {
            SendBuffer();
        }

        return EScan::Feed;
    }

    EScan Exhausted() override {
        return EScan::Final;
    }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        if (!Buffer.Empty()) {
            SendBuffer();
        }

        TEvCompleteSnapshot* result;
        switch (status) {
            case EStatus::Done:
                result = new TEvCompleteSnapshot(TableId, true);
                break;
            case EStatus::Lost:
                result = new TEvCompleteSnapshot(TableId, false, "Owner entity is lost");
                break;
            case EStatus::Term:
                result = new TEvCompleteSnapshot(TableId, false, "Explicit process termination by owner");
                break;
            case EStatus::StorageError:
                result = new TEvCompleteSnapshot(TableId, false, "Some blob has been failed to load");
                break;
            case EStatus::Exception:
                result = new TEvCompleteSnapshot(TableId, false, "Unhandled exception has happened");
                break;
        }

        auto* handle = new IEventHandle(SnapshotWriter, TActorId(), result);
        TlsActivationContext->Send(handle);

        delete this;
        return nullptr;
    }

    void SendBuffer() {
        auto* handle = new IEventHandle(SnapshotWriter, TActorId(), new TEvWriteSnapshot(TableId, std::move(Buffer)));
        TlsActivationContext->Send(handle);
    }

private:
    IDriver* Driver = nullptr;
    TIntrusiveConstPtr<TScheme> Scheme;

    TActorId SnapshotWriter;
    ui32 TableId;
    THashMap<ui32, TColumn> Columns;

    TBuffer Buffer;
};

IActor* CreateSnapshotWriter(TActorId owner, const NKikimrConfig::TSystemTabletBackupConfig& config,
                             const THashMap<ui32, TScheme::TTableInfo>& tables,
                             TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation)
{
    if (config.HasFilesystem()) {
        auto path = TFsPath(config.GetFilesystem().GetPath())
            .Child(TTabletTypes::EType_Name(tabletType))
            .Child(ToString(tabletId))
            .Child(ToString(generation));
        return new TSnapshotWriter(path, owner, tables);
    } else {
        return nullptr;
    }
}

IScan* CreateSnapshotScan(TActorId snapshotWriter, ui32 tableId, const THashMap<ui32, TColumn>& columns) {
    return new TBackupSnapshotScan(snapshotWriter, tableId, columns);
}

} // NKikimr::NTabletFlatExecutor::NBackup

