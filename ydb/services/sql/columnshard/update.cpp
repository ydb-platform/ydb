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

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <algorithm>
#include <cstring>

namespace NKikimr::NSql {
namespace {

struct TPrepared {
    TString Name;
    ui32 Id = 0;
    i32 KeyOrder = -1;
    NScheme::TTypeInfo Type;
    ui8 Precision = 0;
    ui8 Scale = 0;
};

struct TValue {
    TPrepared Column;
    TString Bytes;
};

TString TablePath(const TString& database, const TUpdateStmt& statement) {
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

class TColumnUpdateActor : public NActors::TActorBootstrapped<TColumnUpdateActor> {
public:
    TColumnUpdateActor(const NActors::TActorId& replyTo, TString database, TUpdateStmt statement, ui32 internalErrorRetries, bool defer)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , Defer(defer)
    {
        Y_UNUSED(internalErrorRetries);
    }

    void Bootstrap() {
        if (Defer || !Statement.Returning.empty() || Statement.Set.empty()) {
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
        Become(&TColumnUpdateActor::StateNavigate);
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
        column.Type = info.PType;
        if (info.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
            column.Precision = static_cast<ui8>(info.PType.GetDecimalType().GetPrecision());
            column.Scale = static_cast<ui8>(info.PType.GetDecimalType().GetScale());
        }
        return EncodeCell(column.Type.GetTypeId(), true, {}, EWireFormat::Text, column.Precision, column.Scale).Ok;
    }

    bool Decode(const TPrepared& column, const TLiteral& literal, TString& bytes) {
        if (literal.Kind == TLiteral::EKind::Null || literal.Kind == TLiteral::EKind::Parameter) {
            Fail("0A000", "not supported");
            return false;
        }
        const TCodecValue decoded = DecodeCell(column.Type.GetTypeId(), false, literal.Text, EWireFormat::Text, column.Precision, column.Scale);
        if (!decoded.Ok) {
            Fail(decoded.SqlState.empty() ? TString("0A000") : decoded.SqlState, decoded.Message.empty() ? TString("not supported") : decoded.Message);
            return false;
        }
        bytes = decoded.Bytes;
        return true;
    }

    bool Append(arrow::ArrayBuilder& builder, const TValue& value) {
        switch (value.Column.Type.GetTypeId()) {
            case NScheme::NTypeIds::Int64: {
                i64 number;
                if (value.Bytes.size() != sizeof(number)) {
                    Fail("0A000", "not supported");
                    return false;
                }
                memcpy(&number, value.Bytes.data(), sizeof(number));
                return static_cast<arrow::Int64Builder&>(builder).Append(number).ok();
            }
            case NScheme::NTypeIds::Utf8:
                return static_cast<arrow::StringBuilder&>(builder).Append(std::string(value.Bytes.data(), value.Bytes.size())).ok();
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
        const auto& description = entry.ColumnTableInfo->Description;
        if (description.GetSharding().ColumnShardsSize() != 1 || description.GetSchema().IndexesSize() > 0) {
            Fail("0A000", "not supported");
            return;
        }
        Table = entry.TableId;
        TabletId = description.GetSharding().GetColumnShards(0);

        TVector<const TSysTables::TTableColumnInfo*> keys;
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            if (column.KeyOrder >= 0) {
                keys.push_back(&column);
            }
        }
        std::sort(keys.begin(), keys.end(), [](const auto* left, const auto* right) {
            return left->KeyOrder < right->KeyOrder;
        });
        TVector<TValue> values;
        for (const auto* info : keys) {
            const TComparison* match = nullptr;
            for (const TComparison& comparison : Statement.Where) {
                if (comparison.Column.Name == info->Name) {
                    if (match || comparison.Op != ECompareOp::Eq) {
                        Fail("0A000", "not supported");
                        return;
                    }
                    match = &comparison;
                }
            }
            if (!match) {
                Fail("0A000", "not supported");
                return;
            }
            TValue value;
            if (!Prepare(*info, value.Column) || !Decode(value.Column, match->Value, value.Bytes)) {
                if (!Replied) {
                    Fail("0A000", "not supported");
                }
                return;
            }
            values.push_back(std::move(value));
        }
        for (const TComparison& comparison : Statement.Where) {
            bool key = false;
            for (const auto* info : keys) {
                key = key || info->Name == comparison.Column.Name;
            }
            if (!key) {
                Fail("0A000", "not supported");
                return;
            }
        }
        for (const TSetAssign& assign : Statement.Set) {
            if (assign.FromColumn) {
                Fail("0A000", "not supported");
                return;
            }
            const auto* info = FindColumn(entry, assign.Column.Name);
            if (!info) {
                Fail("42703", assign.Column.Name);
                return;
            }
            if (info->KeyOrder >= 0) {
                Fail("0A000", "not supported");
                return;
            }
            TValue value;
            if (!Prepare(*info, value.Column) || !Decode(value.Column, assign.Value, value.Bytes)) {
                if (!Replied) {
                    Fail("0A000", "not supported");
                }
                return;
            }
            values.push_back(std::move(value));
        }
        std::sort(values.begin(), values.end(), [](const TValue& left, const TValue& right) {
            return left.Column.Id < right.Column.Id;
        });

        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        std::vector<ui32> columnIds;
        for (const TValue& value : values) {
            std::shared_ptr<arrow::ArrayBuilder> builder;
            std::shared_ptr<arrow::DataType> type;
            if (value.Column.Type.GetTypeId() == NScheme::NTypeIds::Int64) {
                type = arrow::int64();
                builder = std::make_shared<arrow::Int64Builder>();
            } else if (value.Column.Type.GetTypeId() == NScheme::NTypeIds::Utf8) {
                type = arrow::utf8();
                builder = std::make_shared<arrow::StringBuilder>();
            } else {
                Fail("0A000", "not supported");
                return;
            }
            if (!Append(*builder, value)) {
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
            fields.push_back(arrow::field(std::string(value.Column.Name), type));
            arrays.push_back(*finished);
            columnIds.push_back(value.Column.Id);
        }
        auto schema = arrow::schema(fields);
        auto batch = arrow::RecordBatch::Make(schema, 1, arrays);
        auto write = std::make_unique<NEvents::TDataEvents::TEvWrite>(1, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        auto& operation = write->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE, Table, columnIds, 0, NKikimrDataEvents::FORMAT_ARROW);
        *operation.MutablePayloadSchema() = NArrow::SerializeSchema(*schema);
        NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite>(*write).AddDataToPayload(NArrow::SerializeBatchNoCompression(batch));
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(write.release(), TabletId, true));
        Linked = true;
        Become(&TColumnUpdateActor::StateWrite);
    }

    void HandleWrite(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
            const TString message = record.IssuesSize() > 0 ? record.GetIssues(0).message() : TString("update failed");
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
        result->CommandTag = "UPDATE 1";
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
    const TUpdateStmt Statement;
    const bool Defer;
    TString Path;
    TTableId Table;
    ui64 TabletId = 0;
    bool Linked = false;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateColumnUpdateActor(const NActors::TActorId& replyTo, TString database, TUpdateStmt statement, ui32 internalErrorRetries, bool defer) {
    return new TColumnUpdateActor(replyTo, std::move(database), std::move(statement), internalErrorRetries, defer);
}

} // namespace NKikimr::NSql
