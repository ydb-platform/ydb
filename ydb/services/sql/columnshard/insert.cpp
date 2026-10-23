#include "events.h"

#include <ydb/services/sql/codec/codec.h>
#include <ydb/services/sql/datashard/events.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <algorithm>
#include <cstring>

namespace NKikimr::NSql {
namespace {

struct TPrepared {
    TString Name;
    ui32 Id = 0;
    i32 KeyOrder = -1;
    bool NotNull = false;
    NScheme::TTypeInfo Type;
    ui8 Precision = 0;
    ui8 Scale = 0;
};

TString TablePath(const TString& database, const TInsertStmt& statement) {
    TString path = database;
    if (path && path.back() == '/') {
        path.pop_back();
    }
    if (statement.Schema) {
        path += "/" + statement.Schema->Name;
    }
    path += "/" + statement.Name.Name;
    return path;
}

class TColumnInsertActor : public NActors::TActorBootstrapped<TColumnInsertActor> {
public:
    TColumnInsertActor(const NActors::TActorId& replyTo, TString database, TInsertStmt statement, ui32 internalErrorRetries, bool defer)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , Defer(defer)
    {
        Y_UNUSED(internalErrorRetries);
    }

    void Bootstrap() {
        if (Defer || Statement.Source || Statement.Conflict != EConflictAction::None || !Statement.Returning.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        Path = TablePath(Database, Statement);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(Path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TColumnInsertActor::StateNavigate);
    }

private:
    void Fail(const TString& state, const TString& message) {
        if (Replied) {
            return;
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = false;
        result->SqlState = state;
        result->Message = message;
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void Unlink() {
        if (Linked) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(TabletId));
            Linked = false;
        }
    }

    const TSysTables::TTableColumnInfo* FindColumn(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& name) const {
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            if (column.Name == name) {
                return &column;
            }
        }
        return nullptr;
    }

    bool Prepare(const TSysTables::TTableColumnInfo& info, TPrepared& column) const {
        column.Name = info.Name;
        column.Id = info.Id;
        column.KeyOrder = info.KeyOrder;
        column.NotNull = info.IsNotNullColumn || info.KeyOrder >= 0;
        column.Type = info.PType;
        if (info.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
            column.Precision = static_cast<ui8>(info.PType.GetDecimalType().GetPrecision());
            column.Scale = static_cast<ui8>(info.PType.GetDecimalType().GetScale());
        }
        const TCodecValue probe = EncodeCell(column.Type.GetTypeId(), true, {}, EWireFormat::Text, column.Precision, column.Scale);
        return probe.Ok;
    }

    bool Append(arrow::ArrayBuilder& builder, const TPrepared& column, const TLiteral* literal) {
        if (!literal || literal->Kind == TLiteral::EKind::Null) {
            if (column.NotNull) {
                Fail("23502", "null value in column " + column.Name);
                return false;
            }
            return builder.AppendNull().ok();
        }
        if (literal->Kind == TLiteral::EKind::Parameter) {
            Fail("0A000", "not supported");
            return false;
        }
        const TCodecValue decoded = DecodeCell(column.Type.GetTypeId(), false, literal->Text, EWireFormat::Text, column.Precision, column.Scale);
        if (!decoded.Ok) {
            Fail(decoded.SqlState.empty() ? TString("0A000") : decoded.SqlState, decoded.Message.empty() ? TString("not supported") : decoded.Message);
            return false;
        }
        const TStringBuf bytes = decoded.Bytes;
        switch (column.Type.GetTypeId()) {
            case NScheme::NTypeIds::Int64: {
                i64 value;
                if (bytes.size() != sizeof(value)) {
                    Fail("0A000", "not supported");
                    return false;
                }
                memcpy(&value, bytes.data(), sizeof(value));
                return static_cast<arrow::Int64Builder&>(builder).Append(value).ok();
            }
            case NScheme::NTypeIds::Utf8:
                return static_cast<arrow::StringBuilder&>(builder).Append(std::string(bytes.data(), bytes.size())).ok();
            default:
                Fail("0A000", "not supported");
                return false;
        }
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            Fail("42P01", Path);
            return;
        }
        const auto& entry = request->ResultSet[0];
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindColumnTable || !entry.ColumnTableInfo) {
            Fail("42P01", Path);
            return;
        }
        const auto& sharding = entry.ColumnTableInfo->Description.GetSharding();
        if (sharding.ColumnShardsSize() != 1 || entry.ColumnTableInfo->Description.GetSchema().IndexesSize() > 0) {
            Fail("0A000", "not supported");
            return;
        }
        Table = entry.TableId;
        TabletId = sharding.GetColumnShards(0);

        TVector<const TSysTables::TTableColumnInfo*> ordered;
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            ordered.push_back(&column);
        }
        std::sort(ordered.begin(), ordered.end(), [](const auto* left, const auto* right) {
            return left->Id < right->Id;
        });
        absl::flat_hash_map<TString, const TLiteral*> given;
        if (Statement.Columns.empty()) {
            if (ordered.size() != Statement.Values.size()) {
                Fail("42601", "column count");
                return;
            }
            for (size_t i = 0; i < ordered.size(); ++i) {
                given[ordered[i]->Name] = &Statement.Values[i];
            }
        } else {
            if (Statement.Columns.size() != Statement.Values.size()) {
                Fail("42601", "column count");
                return;
            }
            for (size_t i = 0; i < Statement.Columns.size(); ++i) {
                if (given.contains(Statement.Columns[i].Name)) {
                    Fail("42701", Statement.Columns[i].Name);
                    return;
                }
                if (!FindColumn(entry, Statement.Columns[i].Name)) {
                    Fail("42703", Statement.Columns[i].Name);
                    return;
                }
                given[Statement.Columns[i].Name] = &Statement.Values[i];
            }
        }

        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        std::vector<ui32> columnIds;
        for (const auto* info : ordered) {
            TPrepared column;
            if (!Prepare(*info, column)) {
                Fail("0A000", "not supported");
                return;
            }
            const TLiteral* literal = nullptr;
            if (const auto found = given.find(column.Name); found != given.end()) {
                literal = found->second;
            }
            std::shared_ptr<arrow::ArrayBuilder> builder;
            std::shared_ptr<arrow::DataType> type;
            if (column.Type.GetTypeId() == NScheme::NTypeIds::Int64) {
                type = arrow::int64();
                builder = std::make_shared<arrow::Int64Builder>();
            } else if (column.Type.GetTypeId() == NScheme::NTypeIds::Utf8) {
                type = arrow::utf8();
                builder = std::make_shared<arrow::StringBuilder>();
            } else {
                Fail("0A000", "not supported");
                return;
            }
            if (!Append(*builder, column, literal)) {
                if (!Replied) {
                    Fail("XX000", "arrow");
                }
                return;
            }
            auto finished = builder->Finish();
            if (!finished.ok()) {
                Fail("XX000", "arrow");
                return;
            }
            fields.push_back(arrow::field(std::string(column.Name), type));
            arrays.push_back(*finished);
            columnIds.push_back(column.Id);
        }
        auto schema = arrow::schema(fields);
        auto batch = arrow::RecordBatch::Make(schema, 1, arrays);
        auto write = std::make_unique<NEvents::TDataEvents::TEvWrite>(1, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        auto& operation = write->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, Table, columnIds, 0, NKikimrDataEvents::FORMAT_ARROW);
        *operation.MutablePayloadSchema() = NArrow::SerializeSchema(*schema);
        NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite>(*write).AddDataToPayload(NArrow::SerializeBatchNoCompression(batch));
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(write.release(), TabletId, true));
        Linked = true;
        Become(&TColumnInsertActor::StateWrite);
    }

    void HandleWrite(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
            const TString message = record.IssuesSize() > 0 ? record.GetIssues(0).message() : TString("insert failed");
            Fail("XX000", message);
            return;
        }
        if (Replied) {
            return;
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "INSERT 0 1";
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        Fail("08006", "connection failure");
    }

    STATEFN(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateWrite) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NEvents::TDataEvents::TEvWriteResult, HandleWrite);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDelivery);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TInsertStmt Statement;
    const bool Defer;
    TString Path;
    TTableId Table;
    ui64 TabletId = 0;
    bool Linked = false;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateColumnInsertActor(const NActors::TActorId& replyTo, TString database, TInsertStmt statement, ui32 internalErrorRetries, bool defer) {
    return new TColumnInsertActor(replyTo, std::move(database), std::move(statement), internalErrorRetries, defer);
}

} // namespace NKikimr::NSql
