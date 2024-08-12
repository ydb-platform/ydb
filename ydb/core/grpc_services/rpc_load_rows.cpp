#include <ydb/core/grpc_services/base/base.h>

#include "rpc_common/rpc_common.h"
#include "service_table.h"
#include "audit_dml_operations.h"

#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/minikql/dom/yson.h>
#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/utils/utf8.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>

#include <ydb/library/binary_json/write.h>
#include <ydb/library/dynumber/dynumber.h>

#include <util/string/vector.h>
#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

namespace {

// TODO: no mapping for DATE, DATETIME, TZ_*, YSON, JSON, UUID, JSON_DOCUMENT, DYNUMBER
bool ConvertArrowToYdbPrimitive(const arrow::DataType& type, Ydb::Type& toType) {
    switch (type.id()) {
        case arrow::Type::BOOL:
            toType.set_type_id(Ydb::Type::BOOL);
            return true;
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
        case arrow::Type::DECIMAL:
            // TODO
            return false;
        case arrow::Type::NA:
        case arrow::Type::HALF_FLOAT:
        case arrow::Type::FIXED_SIZE_BINARY:
        case arrow::Type::DATE32:
        case arrow::Type::DATE64:
        case arrow::Type::TIME32:
        case arrow::Type::TIME64:
        case arrow::Type::INTERVAL_MONTHS:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::LARGE_BINARY:
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
        : TBase(GetDuration(GetProtoRequest(request)->operation_params().operation_timeout()), diskQuotaExceeded,
                NWilson::TSpan(TWilsonKqp::BulkUpsertActor, request->GetWilsonTraceId(), name))
        , Request(request)
    {}

private:
    void OnBeforeStart(const TActorContext& ctx) override {
        Request->SetFinishAction([selfId = ctx.SelfID, as = ctx.ExecutorThread.ActorSystem]() {
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

    TString GetDatabase() override {
        return Request->GetDatabaseName().GetOrElse(DatabaseFromDomain(AppData()));
    }

    const TString& GetTable() override {
        return GetProtoRequest(Request.get())->table();
    }

    const TVector<std::pair<TSerializedCellVec, TString>>& GetRows() const override {
        return AllRows;
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
        if (Request->GetSerializedToken().empty())
            return true;

        NACLib::TUserToken userToken(Request->GetSerializedToken());
        const ui32 access = NACLib::EAccessRights::UpdateRow;
        auto resolveResult = GetResolveNameResult();
        if (!resolveResult) {
            TStringStream explanation;
            explanation << "Access denied for " << userToken.GetUserSID()
                        << " table '" << GetProtoRequest(Request.get())->table()
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
                            << " to table '" << GetProtoRequest(Request.get())->table() << "'";

                errorMessage = explanation.Str();
                return false;
            }
        }
        return true;
    }

    TVector<std::pair<TString, Ydb::Type>> GetRequestColumns(TString& errorMessage) const override {
        Y_UNUSED(errorMessage);

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

        // TODO: check that value is a list of structs

        // For each row in values
        TMemoryPool valueDataPool(256);
        const auto& rows = GetProtoRequest(Request.get())->Getrows().Getvalue().Getitems();
        for (const auto& r : rows) {
            valueDataPool.Clear();

            ui64 sz = 0;
            // Take members corresponding to key columns
            if (!FillCellsFromProto(keyCells, KeyColumnPositions, r, errorMessage, valueDataPool)) {
                return false;
            }

            // Fill rest of cells with non-key column members
            if (!FillCellsFromProto(valueCells, ValueColumnPositions, r, errorMessage, valueDataPool)) {
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
            AllRows.emplace_back(std::move(serializedKey), std::move(serializedValue));
        }

        RuCost = TUpsertCost::CostToRu(cost);
        return true;
    }

    bool ExtractBatch(TString& errorMessage) override {
        Batch = RowsToBatch(AllRows, errorMessage);
        return Batch.get();
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
    TVector<std::pair<TSerializedCellVec, TString>> AllRows;
};

class TUploadColumnsRPCPublic : public NTxProxy::TUploadRowsBase<NKikimrServices::TActivity::GRPC_REQ> {
    using TBase = NTxProxy::TUploadRowsBase<NKikimrServices::TActivity::GRPC_REQ>;
public:
    explicit TUploadColumnsRPCPublic(IRequestOpCtx* request, bool diskQuotaExceeded)
        : TBase(GetDuration(GetProtoRequest(request)->operation_params().operation_timeout()), diskQuotaExceeded)
        , Request(request)
    {}

private:
    void OnBeforeStart(const TActorContext& ctx) override {
        Request->SetFinishAction([selfId = ctx.SelfID, as = ctx.ExecutorThread.ActorSystem]() {
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

    TString GetDatabase() override {
        return Request->GetDatabaseName().GetOrElse(DatabaseFromDomain(AppData()));
    }

    const TString& GetTable() override {
        return GetProtoRequest(Request.get())->table();
    }

    const TVector<std::pair<TSerializedCellVec, TString>>& GetRows() const override {
        return Rows;
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
        if (Request->GetSerializedToken().empty())
            return true;

        NACLib::TUserToken userToken(Request->GetSerializedToken());
        const ui32 access = NACLib::EAccessRights::UpdateRow;
        auto resolveResult = GetResolveNameResult();
        if (!resolveResult) {
            TStringStream explanation;
            explanation << "Access denied for " << userToken.GetUserSID()
                        << " table '" << GetProtoRequest(Request.get())->table()
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
                            << " to table '" << GetProtoRequest(Request.get())->table() << "'";

                errorMessage = explanation.Str();
                return false;
            }
        }
        return true;
    }

    TVector<std::pair<TString, Ydb::Type>> GetRequestColumns(TString& errorMessage) const override {
        if (GetSourceType() == EUploadSource::CSV) {
            // TODO: for CSV with header we have to extract columns from data (from first batch in file stream)
            return {};
        }

        auto schema = NArrow::DeserializeSchema(GetSourceSchema());
        if (!schema) {
            errorMessage = TString("Wrong schema in bulk upsert data");
            return {};
        }

        TVector<std::pair<TString, Ydb::Type>> out;
        out.reserve(schema->num_fields());

        for (auto& field : schema->fields()) {
            auto& name = field->name();
            auto& type = field->type();

            Ydb::Type ydbType;
            if (!ConvertArrowToYdbPrimitive(*type, ydbType)) {
                errorMessage = TString("Cannot convert arrow type to ydb one: " + type->ToString());
                return {};
            }
            out.emplace_back(name, std::move(ydbType));
        }

        return out;
    }

    bool ExtractRows(TString& errorMessage) override {
        Y_ABORT_UNLESS(Batch);
        Rows = BatchToRows(Batch, errorMessage);
        return errorMessage.empty();
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
                auto& cvsSettings = GetCsvSettings();
                ui32 skipRows = cvsSettings.skip_rows();
                auto& delimiter = cvsSettings.delimiter();
                auto& nullValue = cvsSettings.null_value();
                bool withHeader = cvsSettings.header();

                auto reader = NFormats::TArrowCSV::Create(SrcColumns, withHeader, NotNullColumns);
                if (!reader.ok()) {
                    errorMessage = reader.status().ToString();
                    return false;
                }
                const auto& quoting = cvsSettings.quoting();
                if (quoting.quote_char().length() > 1) {
                    errorMessage = TStringBuilder() << "Wrong quote char '" << quoting.quote_char() << "'";
                    return false;
                }
                const char qchar = quoting.quote_char().empty() ? '"' : quoting.quote_char().front();
                reader->SetQuoting(!quoting.disabled(), qchar, !quoting.double_quote_disabled());
                reader->SetSkipRows(skipRows);

                if (!delimiter.empty()) {
                    if (delimiter.size() != 1) {
                        errorMessage = TStringBuilder() << "Wrong delimiter '" << delimiter << "'";
                        return false;
                    }

                    reader->SetDelimiter(delimiter[0]);
                }

                if (!nullValue.empty()) {
                    reader->SetNullValue(nullValue);
                }

                if (data.size() > NFormats::TArrowCSV::DEFAULT_BLOCK_SIZE) {
                    ui32 blockSize = NFormats::TArrowCSV::DEFAULT_BLOCK_SIZE;
                    blockSize *= data.size() / blockSize + 1;
                    reader->SetBlockSize(blockSize);
                }

                Batch = reader->ReadSingleBatch(data, errorMessage);
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
    TVector<std::pair<TSerializedCellVec, TString>> Rows;

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
