#include <ydb/core/grpc_services/base/base.h>

#include "rpc_common/rpc_common.h"
#include "service_table.h"
#include "audit_dml_operations.h"

#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/minikql/dom/yson.h>
#include <yql/essentials/minikql/dom/json.h>
#include <yql/essentials/utils/utf8.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/string/vector.h>
#include <util/generic/size_literals.h>
#include <algorithm>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

namespace {

// TODO: no mapping for DATE, DATETIME, TZ_*, YSON, JSON, UUID, JSON_DOCUMENT, DYNUMBER
bool ConvertArrowToYdbPrimitive(const arrow::DataType& type, Ydb::Type& toType, const NScheme::TTypeInfo* tableColumnType = nullptr) {
    switch (type.id()) {
        case arrow::Type::UINT8:
            toType.set_type_id(Ydb::Type::UINT8);
            return true;
        case arrow::Type::INT8:
            toType.set_type_id(Ydb::Type::INT8);
            return true;
        case arrow::Type::UINT16:
            toType.set_type_id(Ydb::Type::UINT16);
            return true;
        case arrow::Type::INT16:
            toType.set_type_id(Ydb::Type::INT16);
            return true;
        case arrow::Type::UINT32:
            toType.set_type_id(Ydb::Type::UINT32);
            return true;
        case arrow::Type::INT32:
            toType.set_type_id(Ydb::Type::INT32);
            return true;
        case arrow::Type::UINT64:
            toType.set_type_id(Ydb::Type::UINT64);
            return true;
        case arrow::Type::INT64:
            toType.set_type_id(Ydb::Type::INT64);
            return true;
        case arrow::Type::FLOAT:
            toType.set_type_id(Ydb::Type::FLOAT);
            return true;
        case arrow::Type::DOUBLE:
            toType.set_type_id(Ydb::Type::DOUBLE);
            return true;
        case arrow::Type::STRING:
            toType.set_type_id(Ydb::Type::UTF8);
            return true;
        case arrow::Type::BINARY:
            toType.set_type_id(Ydb::Type::STRING);
            return true;
        case arrow::Type::TIMESTAMP:
            toType.set_type_id(Ydb::Type::TIMESTAMP);
            return true;
        case arrow::Type::DURATION:
            toType.set_type_id(Ydb::Type::INTERVAL);
            return true;
        case arrow::Type::FIXED_SIZE_BINARY: {
            if (!tableColumnType || dynamic_cast<const arrow::FixedSizeBinaryType&>(type).byte_width() != NScheme::FSB_SIZE) {
                break;
            }

            switch (tableColumnType->GetTypeId()) {
                case NScheme::NTypeIds::Decimal: {
                    Ydb::DecimalType* decimalType = toType.mutable_decimal_type();
                    decimalType->set_precision(tableColumnType->GetDecimalType().GetPrecision());
                    decimalType->set_scale(tableColumnType->GetDecimalType().GetScale());
                    return true;
                }

                case NScheme::NTypeIds::Uuid: {
                    toType.set_type_id(Ydb::Type::UUID);
                    return true;
                }
            }
            break;
        }
        case arrow::Type::BOOL:
        case arrow::Type::NA:
        case arrow::Type::HALF_FLOAT:
        case arrow::Type::DATE32:
        case arrow::Type::DATE64:
        case arrow::Type::TIME32:
        case arrow::Type::TIME64:
        case arrow::Type::INTERVAL_MONTHS:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::DECIMAL:
        case arrow::Type::DECIMAL256:
        case arrow::Type::DENSE_UNION:
        case arrow::Type::DICTIONARY:
        case arrow::Type::EXTENSION:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::INTERVAL_DAY_TIME:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::LIST:
        case arrow::Type::MAP:
        case arrow::Type::MAX_ID:
        case arrow::Type::SPARSE_UNION:
        case arrow::Type::STRUCT:
            break;
    }
    return false;
}

bool CheckAccess(const TString& table, const TString& token, const NSchemeCache::TSchemeCacheNavigate* resolveResult, TString& errorMessage) {
    if (token.empty())
        return true;

    NACLib::TUserToken userToken(token);
    const ui32 access = NACLib::EAccessRights::UpdateRow;
    if (!resolveResult) {
        TStringStream explanation;
        explanation << "Access denied for " << userToken.GetUserSID()
                    << " table '" << table
                    << "' has not been resolved yet";

        errorMessage = explanation.Str();
        return false;
    }
    for (const NSchemeCache::TSchemeCacheNavigate::TEntry& entry : resolveResult->ResultSet) {
        if (entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
            && entry.SecurityObject != nullptr
            && !entry.SecurityObject->CheckAccess(access, userToken))
        {
            TStringStream explanation;
            explanation << "Access denied for " << userToken.GetUserSID()
                        << " with access " << NACLib::AccessRightsToString(access)
                        << " to table '" << table << "'";

            errorMessage = explanation.Str();
            return false;
        }
    }
    return true;
}

}

using TEvBulkUpsertRequest = TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest,
    Ydb::Table::BulkUpsertResponse>;

const Ydb::Table::BulkUpsertRequest* GetProtoRequest(IRequestOpCtx* req) {
    return TEvBulkUpsertRequest::GetProtoRequest(req);
}

class TUploadRowsRPCPublic : public NTxProxy::TUploadRowsBase<NKikimrServices::TActivity::GRPC_REQ> {
    using TBase = NTxProxy::TUploadRowsBase<NKikimrServices::TActivity::GRPC_REQ>;
public:
    explicit TUploadRowsRPCPublic(IRequestOpCtx* request, bool diskQuotaExceeded, const char* name)
        : TBase(std::make_shared<TVector<std::pair<TSerializedCellVec, TString>>>(), GetDuration(GetProtoRequest(request)->operation_params().operation_timeout()), diskQuotaExceeded,
                NWilson::TSpan(TWilsonKqp::BulkUpsertActor, request->GetWilsonTraceId(), name))
        , Request(request)
        , Database(Request->GetDatabaseName().GetOrElse(""))
    {
    }

private:
    void OnBeforeStart(const TActorContext& ctx) override {
        Request->SetFinishAction([selfId = ctx.SelfID, as = ctx.ActorSystem()]() {
            as->Send(selfId, new TEvents::TEvPoison);
        });
    }

    void OnBeforePoison(const TActorContext&) override {
        // Client is gone, but we need to "reply" anyway?
        Request->ReplyWithYdbStatus(Ydb::StatusIds::CANCELLED);
    }

    bool ReportCostInfoEnabled() const {
        return GetProtoRequest(Request.get())->operation_params().report_cost_info() == Ydb::FeatureFlag::ENABLED;
    }

    void AuditContextStart() override {
        NKikimr::NGRpcService::AuditContextAppend(Request.get(), *GetProtoRequest(Request.get()));
    }

    const TString& GetDatabase() const override {
        return Database;
    }

    const TString& GetTable() const override {
        return GetProtoRequest(Request.get())->table();
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        return Request->RaiseIssue(issue);
    }

    void SendResult(const NActors::TActorContext&, const StatusIds::StatusCode& status) override {
        const Ydb::Table::BulkUpsertResult result;
        if (status == StatusIds::SUCCESS) {
            ui64 cost = std::ceil(RuCost);
            Request->SetRuHeader(cost);
            if (ReportCostInfoEnabled()) {
                Request->SetCostInfo(cost);
            }
        }
        return Request->SendResult(result, status);
    }

    bool CheckAccess(TString& errorMessage) override {
        return ::NKikimr::NGRpcService::CheckAccess(GetTable(), Request->GetSerializedToken(), GetResolveNameResult(), errorMessage);
    }

    TConclusion<TVector<std::pair<TString, Ydb::Type>>> GetRequestColumns() const override {
        const auto& type = GetProtoRequest(Request.get())->Getrows().Gettype();
        const auto& rowType = type.Getlist_type();
        const auto& rowFields = rowType.Getitem().Getstruct_type().Getmembers();

        TVector<std::pair<TString, Ydb::Type>> result;

        for (i32 pos = 0; pos < rowFields.size(); ++pos) {
            const auto& name = rowFields[pos].Getname();
            const auto& typeInProto = rowFields[pos].type().has_optional_type() ?
                        rowFields[pos].type().optional_type().item() : rowFields[pos].type();

            result.emplace_back(name, typeInProto);
        }
        return result;
    }

    bool ExtractRows(TString& errorMessage) override {
        // Parse type field
        // Check that it is a list of stuct
        // List all memebers and check their names and types
        // Save indexes of key column members and no-key members

        TVector<TCell> keyCells;
        TVector<TCell> valueCells;
        float cost = 0.0f;
        TVector<std::pair<TSerializedCellVec, TString>> rows;

        // TODO: check that value is a list of structs

        // For each row in values
        TMemoryPool valueDataPool(256);
        for (const auto& r : GetProtoRequest(Request.get())->Getrows().Getvalue().Getitems()) {
            valueDataPool.Clear();

            ui64 sz = 0;
            // Take members corresponding to key columns
            if (!FillCellsFromProto(keyCells, KeyColumnPositions, r, errorMessage, valueDataPool)) {
                return false;
            }

            // Fill rest of cells with non-key column members
            if (!FillCellsFromProto(valueCells, ValueColumnPositions, r, errorMessage, valueDataPool, IsInfinityInJsonAllowed())) {
                return false;
            }

            for (const auto& cell : keyCells) {
                sz += cell.Size();
            }

            for (const auto& cell : valueCells) {
                sz += cell.Size();
            }

            cost += TUpsertCost::OneRowCost(sz);

            // Save serialized key and value
            TSerializedCellVec serializedKey(keyCells);
            TString serializedValue = TSerializedCellVec::Serialize(valueCells);
            rows.emplace_back(std::move(serializedKey), std::move(serializedValue));
        }

        Rows = std::make_shared<TVector<std::pair<TSerializedCellVec, TString>>>(std::move(rows));
        RuCost = TUpsertCost::CostToRu(cost);
        return true;
    }

    bool ExtractBatch(TString& errorMessage) override {
        Batch = RowsToBatch(*Rows, errorMessage);
        return Batch.get();
    }

    std::shared_ptr<arrow::RecordBatch> RowsToBatch(const TVector<std::pair<TSerializedCellVec, TString>>& rows,
                                                    TString& errorMessage)
    {
        NArrow::TArrowBatchBuilder batchBuilder(arrow::Compression::UNCOMPRESSED, NotNullColumns);
        batchBuilder.Reserve(rows.size()); // TODO: ReserveData()
        const auto startStatus = batchBuilder.Start(YdbSchema);
        if (!startStatus.ok()) {
            errorMessage = "Cannot make Arrow batch from rows: " + startStatus.ToString();
            return {};
        }

        for (const auto& kv : rows) {
            const TSerializedCellVec& key = kv.first;
            const TSerializedCellVec value(kv.second);

            batchBuilder.AddRow(key.GetCells(), value.GetCells());
        }

        return batchBuilder.FlushBatch(false);
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
    const TString Database;
};

class TUploadColumnsRPCPublic : public NTxProxy::TUploadRowsBase<NKikimrServices::TActivity::GRPC_REQ> {
    using TBase = NTxProxy::TUploadRowsBase<NKikimrServices::TActivity::GRPC_REQ>;
public:
    explicit TUploadColumnsRPCPublic(IRequestOpCtx* request, bool diskQuotaExceeded)
        : TBase(std::make_shared<TVector<std::pair<TSerializedCellVec, TString>>>(), GetDuration(GetProtoRequest(request)->operation_params().operation_timeout()), diskQuotaExceeded)
        , Request(request)
        , Database(Request->GetDatabaseName().GetOrElse(""))
    {
    }

private:
    void OnBeforeStart(const TActorContext& ctx) override {
        Request->SetFinishAction([selfId = ctx.SelfID, as = ctx.ActorSystem()]() {
            as->Send(selfId, new TEvents::TEvPoison);
        });
    }

    void OnBeforePoison(const TActorContext&) override {
        // Client is gone, but we need to "reply" anyway?
        Request->ReplyWithYdbStatus(Ydb::StatusIds::CANCELLED);
    }

    bool ReportCostInfoEnabled() const {
        return GetProtoRequest(Request.get())->operation_params().report_cost_info() == Ydb::FeatureFlag::ENABLED;
    }

    EUploadSource GetSourceType() const override {
        auto* req = GetProtoRequest(Request.get());
        if (req->has_arrow_batch_settings()) {
            return EUploadSource::ArrowBatch;
        }
        if (req->has_csv_settings()) {
            return EUploadSource::CSV;
        }
        Y_ABORT_UNLESS(false, "unexpected format");
    }

    void AuditContextStart() override {
        NKikimr::NGRpcService::AuditContextAppend(Request.get(), *GetProtoRequest(Request.get()));
    }

    const TString& GetDatabase() const override {
        return Database;
    }

    const TString& GetTable() const override {
        return GetProtoRequest(Request.get())->table();
    }

    const TString& GetSourceData() const override {
        return GetProtoRequest(Request.get())->data();
    }

    const TString& GetSourceSchema() const override {
        static const TString none;
        if (GetProtoRequest(Request.get())->has_arrow_batch_settings()) {
            return GetProtoRequest(Request.get())->arrow_batch_settings().schema();
        }
        return none;
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        return Request->RaiseIssue(issue);
    }

    void SendResult(const NActors::TActorContext&, const StatusIds::StatusCode& status) override {
        const Ydb::Table::BulkUpsertResult result;
        if (status == StatusIds::SUCCESS) {
            ui64 cost = std::ceil(RuCost);
            Request->SetRuHeader(cost);
            if (ReportCostInfoEnabled()) {
                Request->SetCostInfo(cost);
            }
        }
        return Request->SendResult(result, status);
    }

    bool CheckAccess(TString& errorMessage) override {
        return ::NKikimr::NGRpcService::CheckAccess(GetTable(), Request->GetSerializedToken(), GetResolveNameResult(), errorMessage);
    }

    TConclusion<TVector<std::pair<TString, Ydb::Type>>> GetRequestColumns() const override {
        TVector<std::pair<TString, Ydb::Type>> out;
        if (GetSourceType() == EUploadSource::CSV) {
            // TODO: for CSV with header we have to extract columns from data (from first batch in file stream)
            return out;
        }

        auto schema = NArrow::DeserializeSchema(GetSourceSchema());
        if (!schema) {
            return TConclusionStatus::Fail("Wrong schema in bulk upsert data");
        }

        out.reserve(schema->num_fields());

        const NSchemeCache::TSchemeCacheNavigate* resolveResult = GetResolveNameResult();
        THashMap<TString, NScheme::TTypeInfo> tableColumnTypes;
        if (!resolveResult || resolveResult->ResultSet.size() != 1) {
            return TConclusionStatus::Fail(TStringBuilder() <<
                "Wrong table resolve result: expected exactly one entry, got " <<
                (resolveResult ? std::to_string(resolveResult->ResultSet.size()) : "none"));
        }

        const auto& entry = resolveResult->ResultSet.front();
        for (auto&& [_, colInfo] : entry.Columns) {
            tableColumnTypes[colInfo.Name] = colInfo.PType;
        }

        for (auto& field : schema->fields()) {
            auto& name = field->name();
            auto& type = field->type();

            Ydb::Type ydbType;
            const NScheme::TTypeInfo* tableColumnType = nullptr;
            auto tableTypeIt = tableColumnTypes.find(name);
            if (tableTypeIt != tableColumnTypes.end()) {
                tableColumnType = &tableTypeIt->second;
            }

            if (!ConvertArrowToYdbPrimitive(*type, ydbType, tableColumnType)) {
                return TConclusionStatus::Fail("Cannot convert arrow type to ydb one: " + type->ToString());
            }

            out.emplace_back(name, std::move(ydbType));
        }

        return out;
    }

    bool ExtractRows(TString& errorMessage) override {
        Y_ABORT_UNLESS(Batch);
        Rows = std::make_shared<TVector<std::pair<TSerializedCellVec, TString>>>(BatchToRows(Batch, errorMessage));
        return errorMessage.empty();
    }

    TVector<std::pair<TSerializedCellVec, TString>> BatchToRows(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                                TString& errorMessage) {
        Y_ABORT_UNLESS(batch);
        TVector<std::pair<TSerializedCellVec, TString>> out;
        out.reserve(batch->num_rows());

        ui32 keySize = KeyColumnPositions.size(); // YdbSchema contains keys first
        TRowWriter writer(out, keySize);
        NArrow::TArrowToYdbConverter batchConverter(YdbSchema, writer, IsInfinityInJsonAllowed());
        if (!batchConverter.Process(*batch, errorMessage)) {
            return {};
        }

        RuCost = writer.GetRuCost();
        return out;
    }

    bool ExtractBatch(TString& errorMessage) override {
        switch (GetSourceType()) {
            case EUploadSource::ProtoValues:
            {
                errorMessage = "Unexpected data format in column upsert";
                return false;
            }
            case EUploadSource::ArrowBatch:
            {
                auto schema = NArrow::DeserializeSchema(GetSourceSchema());
                if (!schema) {
                    errorMessage = "Bad schema in bulk upsert data";
                    return false;
                }

                auto& data = GetSourceData();
                Batch = NArrow::DeserializeBatch(data, schema);
                if (!Batch) {
                    errorMessage = "Cannot deserialize arrow batch with specified schema";
                    return false;
                }

                break;
            }
            case EUploadSource::CSV:
            {
                auto& data = GetSourceData();
                auto& csvSettings = GetCsvSettings();
                auto reader = NFormats::TArrowCSVScheme::Create(SrcColumns, csvSettings.header(), NotNullColumns);
                if (!reader.ok()) {
                    errorMessage = reader.status().ToString();
                    return false;
                }
                Batch = reader->ReadSingleBatch(data, csvSettings, errorMessage);
                if (!Batch) {
                    return false;
                }

                if (!Batch->num_rows()) {
                    errorMessage = "No rows in CSV";
                    return false;
                }

                break;
            }
        }

        return true;
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
    const TString Database;

    const Ydb::Formats::CsvSettings& GetCsvSettings() const {
        return GetProtoRequest(Request.get())->csv_settings();
    }
};

void DoBulkUpsertRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    bool diskQuotaExceeded = p->GetDiskQuotaExceeded();

    if (GetProtoRequest(p.get())->has_arrow_batch_settings()) {
        f.RegisterActor(new TUploadColumnsRPCPublic(p.release(), diskQuotaExceeded));
    } else if (GetProtoRequest(p.get())->has_csv_settings()) {
        f.RegisterActor(new TUploadColumnsRPCPublic(p.release(), diskQuotaExceeded));
    } else {
        f.RegisterActor(new TUploadRowsRPCPublic(p.release(), diskQuotaExceeded, "BulkRowsUpsertActor"));
    }
}

template<>
IActor* TEvBulkUpsertRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    bool diskQuotaExceeded = msg->GetDiskQuotaExceeded();

    if (GetProtoRequest(msg)->has_arrow_batch_settings()) {
        return new TUploadColumnsRPCPublic(msg, diskQuotaExceeded);
    } else if (GetProtoRequest(msg)->has_csv_settings()) {
        return new TUploadColumnsRPCPublic(msg, diskQuotaExceeded);
    } else {
        return new TUploadRowsRPCPublic(msg, diskQuotaExceeded, "BulkRowsUpsertActor");
    }
}


} // namespace NKikimr
} // namespace NGRpcService
