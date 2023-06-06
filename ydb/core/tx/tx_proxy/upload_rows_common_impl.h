#pragma once

#include <ydb/core/actorlib_impl/long_timer.h>

#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/rpc_long_tx.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/io_formats/csv.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/base/path.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/grpc/draft/ydb_long_tx_v1.pb.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <util/string/join.h>
#include <util/string/vector.h>
#include <util/generic/size_literals.h>

namespace NKikimr {

using namespace NActors;

struct TUpsertCost {
    static constexpr float OneRowCost(ui64 sz) {
        constexpr ui64 unitSize = 1_KB;
        constexpr ui64 unitSizeAdjust = unitSize - 1;

        return (sz + unitSizeAdjust) / unitSize;
    }

    static constexpr float BatchCost(ui64 batchSize, ui32 rows) {
        constexpr ui64 unitSize = 1_KB;

        return Max<ui64>(rows, batchSize / unitSize);
    }

    static constexpr float CostToRu(float cost) {
        constexpr float ruPerKB = 0.5f; // 0.5 ru for 1 KB

        return cost * ruPerKB;
    }
};

namespace {

class TRowWriter : public NArrow::IRowWriter {
public:
    TRowWriter(TVector<std::pair<TSerializedCellVec, TString>>& rows, ui32 keySize)
        : Rows(rows)
        , KeySize(keySize)
        , RowCost(0)
    {}

    void AddRow(const TConstArrayRef<TCell>& cells) override {
        ui64 sz = 0;
        for (const auto& cell : cells) {
            sz += cell.Size();
        }
        RowCost += TUpsertCost::OneRowCost(sz);

        TConstArrayRef<TCell> keyCells(&cells[0], KeySize);
        TConstArrayRef<TCell> valueCells(&cells[KeySize], cells.size() - KeySize);

        TSerializedCellVec serializedKey(TSerializedCellVec::Serialize(keyCells));
        Rows.emplace_back(serializedKey, TSerializedCellVec::Serialize(valueCells));
    }

    float GetRuCost() const {
        return TUpsertCost::CostToRu(RowCost);
    }

private:
    TVector<std::pair<TSerializedCellVec, TString>>& Rows;
    ui32 KeySize;
    float RowCost;
};

}

namespace NTxProxy {

template <NKikimrServices::TActivity::EType DerivedActivityType>
class TUploadRowsBase : public TActorBootstrapped<TUploadRowsBase<DerivedActivityType>> {
    using TBase = TActorBootstrapped<TUploadRowsBase<DerivedActivityType>>;
    using TThis = typename TBase::TThis;

private:
    using TTabletId = ui64;

    static constexpr TDuration DEFAULT_TIMEOUT = TDuration::Seconds(5*60);

    TActorId SchemeCache;
    TActorId LeaderPipeCache;
    TDuration Timeout;
    TInstant StartTime;
    TActorId TimeoutTimerActorId;
    bool WaitingResolveReply;
    bool Finished;

    TAutoPtr<NSchemeCache::TSchemeCacheRequest> ResolvePartitionsResult;
    std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> ResolveNamesResult;
    TSerializedCellVec MinKey;
    TSerializedCellVec MaxKey;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<NScheme::TTypeInfo> ValueColumnTypes;
    NSchemeCache::TSchemeCacheNavigate::EKind TableKind = NSchemeCache::TSchemeCacheNavigate::KindUnknown;
    THashSet<TTabletId> ShardRepliesLeft;
    Ydb::StatusIds::StatusCode Status;
    TString ErrorMessage;
    std::shared_ptr<NYql::TIssues> Issues = std::make_shared<NYql::TIssues>();
    NLongTxService::TLongTxId LongTxId;
    NThreading::TFuture<Ydb::LongTx::WriteResponse> WriteBatchResult;

protected:
    enum class EUploadSource {
        ProtoValues = 0,
        ArrowBatch = 1,
        CSV = 2,
    };
public:
    // Positions of key and value fields in the request proto struct
    struct TFieldDescription {
        ui32 ColId;
        TString ColName;
        ui32 PositionInStruct;
        NScheme::TTypeInfo Type;
        i32 Typmod;
        bool NotNull = false;
    };
protected:
    TVector<TString> KeyColumnNames;
    TVector<TFieldDescription> KeyColumnPositions;
    TVector<TString> ValueColumnNames;
    TVector<TFieldDescription> ValueColumnPositions;

    // Additional schema info (for OLAP dst or source format)
    TVector<std::pair<TString, NScheme::TTypeInfo>> SrcColumns; // source columns in CSV could have any order
    TVector<std::pair<TString, NScheme::TTypeInfo>> YdbSchema;
    THashMap<ui32, size_t> Id2Position; // columnId -> its position in YdbSchema
    THashMap<TString, NScheme::TTypeInfo> ColumnsToConvert;
    THashMap<TString, NScheme::TTypeInfo> ColumnsToConvertInplace;

    bool WriteToTableShadow = false;
    bool AllowWriteToPrivateTable = false;
    bool DiskQuotaExceeded = false;

    std::shared_ptr<arrow::RecordBatch> Batch;
    float RuCost = 0.0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return DerivedActivityType;
    }

    explicit TUploadRowsBase(TDuration timeout = TDuration::Max(), bool diskQuotaExceeded = false)
        : TBase()
        , SchemeCache(MakeSchemeCacheID())
        , LeaderPipeCache(MakePipePeNodeCacheID(false))
        , Timeout((timeout && timeout <= DEFAULT_TIMEOUT) ? timeout : DEFAULT_TIMEOUT)
        , WaitingResolveReply(false)
        , Finished(false)
        , Status(Ydb::StatusIds::SUCCESS)
        , DiskQuotaExceeded(diskQuotaExceeded)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        StartTime = TAppData::TimeProvider->Now();
        ResolveTable(GetTable(), ctx);
    }

    void Die(const NActors::TActorContext& ctx) override {
        Y_VERIFY(Finished);
        Y_VERIFY(!WaitingResolveReply);
        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
        if (TimeoutTimerActorId) {
            ctx.Send(TimeoutTimerActorId, new TEvents::TEvPoisonPill());
        }
        TBase::Die(ctx);
    }

protected:
    TInstant Deadline() const {
        return StartTime + Timeout;
    }

    const NSchemeCache::TSchemeCacheNavigate* GetResolveNameResult() const {
        return ResolveNamesResult.get();
    }

    const TKeyDesc* GetKeyRange() const {
        Y_VERIFY(ResolvePartitionsResult->ResultSet.size() == 1);
        return ResolvePartitionsResult->ResultSet[0].KeyDescription.Get();
    }

    std::shared_ptr<arrow::RecordBatch> RowsToBatch(const TVector<std::pair<TSerializedCellVec, TString>>& rows,
                                                    TString& errorMessage)
    {
        NArrow::TArrowBatchBuilder batchBuilder;
        batchBuilder.Reserve(rows.size()); // TODO: ReserveData()
        if (!batchBuilder.Start(YdbSchema)) {
            errorMessage = "Cannot make Arrow batch from rows";
            return {};
        }

        for (const auto& kv : rows) {
            const TSerializedCellVec& key = kv.first;
            const TSerializedCellVec value(kv.second);

            batchBuilder.AddRow(key.GetCells(), value.GetCells());
        }

        return batchBuilder.FlushBatch(false);
    }

    TVector<std::pair<TSerializedCellVec, TString>> BatchToRows(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                                TString& errorMessage) {
        Y_VERIFY(batch);
        TVector<std::pair<TSerializedCellVec, TString>> out;
        out.reserve(batch->num_rows());

        ui32 keySize = KeyColumnPositions.size(); // YdbSchema contains keys first
        TRowWriter writer(out, keySize);
        NArrow::TArrowToYdbConverter batchConverter(YdbSchema, writer);
        if (!batchConverter.Process(*batch, errorMessage)) {
            return {};
        }

        RuCost = writer.GetRuCost();
        return out;
    }

private:
    virtual TString GetDatabase() = 0;
    virtual const TString& GetTable() = 0;
    virtual const TVector<std::pair<TSerializedCellVec, TString>>& GetRows() const = 0;
    virtual bool CheckAccess(TString& errorMessage) = 0;
    virtual TVector<std::pair<TString, Ydb::Type>> GetRequestColumns(TString& errorMessage) const = 0;
    virtual bool ExtractRows(TString& errorMessage) = 0;
    virtual bool ExtractBatch(TString& errorMessage) = 0;
    virtual void RaiseIssue(const NYql::TIssue& issue) = 0;
    virtual void SendResult(const NActors::TActorContext& ctx, const ::Ydb::StatusIds::StatusCode& status) = 0;

    virtual EUploadSource GetSourceType() const {
        return EUploadSource::ProtoValues;
    }

    virtual const TString& GetSourceData() const {
        static const TString none;
        return none;
    }

    virtual const TString& GetSourceSchema() const {
        static const TString none;
        return none;
    }

private:
    STFUNC(StateWaitResolveTable) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    static bool SameDstType(NScheme::TTypeInfo type1, NScheme::TTypeInfo type2, bool allowConvert) {
        bool res = (type1 == type2);
        if (!res && allowConvert) {
            res = (NArrow::GetArrowType(type1)->id() == NArrow::GetArrowType(type2)->id());
        }
        return res;
    }

    static bool SameOrConvertableDstType(NScheme::TTypeInfo type1, NScheme::TTypeInfo type2, bool allowConvert) {
        bool ok = SameDstType(type1, type2, allowConvert) || NArrow::TArrowToYdbConverter::NeedInplaceConversion(type1, type2);
        if (!ok && allowConvert) {
            ok = NArrow::TArrowToYdbConverter::NeedConversion(type1, type2);
        }
        return ok;
    }

    bool BuildSchema(const NActors::TActorContext& ctx, TString& errorMessage, bool makeYqbSchema) {
        Y_UNUSED(ctx);
        Y_VERIFY(ResolveNamesResult);

        auto& entry = ResolveNamesResult->ResultSet.front();

        for (const auto& index : entry.Indexes) {
            switch (index.GetType()) {
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
                if (AppData(ctx)->FeatureFlags.GetEnableBulkUpsertToAsyncIndexedTables()) {
                    continue;
                } else {
                    errorMessage = "Bulk upsert is not supported for tables with indexes";
                    return false;
                }
            default:
                errorMessage = "Only async-indexed tables are supported by BulkUpsert";
                return false;
            }
        }

        TVector<ui32> keyColumnIds;
        THashMap<TString, ui32> columnByName;
        THashSet<TString> keyColumnsLeft;
        THashSet<TString> notNullColumnsLeft = entry.NotNullColumns;
        SrcColumns.reserve(entry.Columns.size());

        for (const auto& [_, colInfo] : entry.Columns) {
            ui32 id = colInfo.Id;
            auto& name = colInfo.Name;
            auto& type = colInfo.PType;
            SrcColumns.emplace_back(name, type); // TODO: is it in correct order ?

            columnByName[name] = id;
            i32 keyOrder = colInfo.KeyOrder;
            if (keyOrder != -1) {
                Y_VERIFY(keyOrder >= 0);
                KeyColumnTypes.resize(Max<size_t>(KeyColumnTypes.size(), keyOrder + 1));
                KeyColumnTypes[keyOrder] = type;
                keyColumnIds.resize(Max<size_t>(keyColumnIds.size(), keyOrder + 1));
                keyColumnIds[keyOrder] = id;
                keyColumnsLeft.insert(name);
            }
        }

        KeyColumnPositions.resize(KeyColumnTypes.size());
        KeyColumnNames.resize(KeyColumnTypes.size());

        auto reqColumns = GetRequestColumns(errorMessage);
        if (!errorMessage.empty()) {
            return false;
        } else if (reqColumns.empty()) {
            for (auto& [name, typeInfo] : SrcColumns) {
                Ydb::Type ydbType;
                if (typeInfo.GetTypeId() != NScheme::NTypeIds::Pg) {
                    ydbType.set_type_id((Ydb::Type::PrimitiveTypeId)typeInfo.GetTypeId());
                } else {
                    auto* typeDesc = typeInfo.GetTypeDesc();
                    auto* pg = ydbType.mutable_pg_type();
                    pg->set_type_name(NPg::PgTypeNameFromTypeDesc(typeDesc));
                    pg->set_oid(NPg::PgTypeIdFromTypeDesc(typeDesc));
                }
                reqColumns.emplace_back(name, std::move(ydbType));
            }
        }

        for (size_t pos = 0; pos < reqColumns.size(); ++pos) {
            auto& name = reqColumns[pos].first;
            const auto* cp = columnByName.FindPtr(name);
            if (!cp) {
                errorMessage = Sprintf("Unknown column: %s", name.c_str());
                return false;
            }
            i32 typmod = -1;
            ui32 colId = *cp;
            auto& ci = *entry.Columns.FindPtr(colId);

            const auto& typeInProto = reqColumns[pos].second;

            if (typeInProto.type_id()) {
                auto typeInRequest = NScheme::TTypeInfo(typeInProto.type_id());
                bool sourceIsArrow = GetSourceType() != EUploadSource::ProtoValues;
                bool ok = SameOrConvertableDstType(typeInRequest, ci.PType, sourceIsArrow); // TODO
                if (!ok) {
                    errorMessage = Sprintf("Type mismatch for column %s: expected %s, got %s",
                                           name.c_str(), NScheme::TypeName(ci.PType).c_str(),
                                           NScheme::TypeName(typeInRequest).c_str());
                    return false;
                }
                if (NArrow::TArrowToYdbConverter::NeedInplaceConversion(typeInRequest, ci.PType)) {
                    ColumnsToConvertInplace[name] = ci.PType;
                }
            } else if (typeInProto.has_decimal_type() && ci.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
                int precision = typeInProto.decimal_type().precision();
                int scale = typeInProto.decimal_type().scale();
                if (precision != NScheme::DECIMAL_PRECISION || scale != NScheme::DECIMAL_SCALE) {
                    errorMessage = Sprintf("Unsupported Decimal(%d,%d) for column %s: expected Decimal(%d,%d)",
                                           precision, scale,
                                           name.c_str(),
                                           NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE);

                    return false;
                }
            } else if (typeInProto.has_pg_type()) {
                const auto& typeName = typeInProto.pg_type().type_name();
                auto* typeDesc = NPg::TypeDescFromPgTypeName(typeName);
                if (!typeDesc) {
                    errorMessage = Sprintf("Unknown pg type for column %s: %s",
                                           name.c_str(), typeName.c_str());
                    return false;
                }
                auto typeInRequest = NScheme::TTypeInfo(NScheme::NTypeIds::Pg, typeDesc);
                bool ok = SameDstType(typeInRequest, ci.PType, false);
                if (!ok) {
                    errorMessage = Sprintf("Type mismatch for column %s: expected %s, got %s",
                                           name.c_str(), NScheme::TypeName(ci.PType).c_str(),
                                           NScheme::TypeName(typeInRequest).c_str());
                    return false;
                }
                if (!ci.PTypeMod.empty() && NPg::TypeDescNeedsCoercion(typeDesc)) {
                    auto result = NPg::BinaryTypeModFromTextTypeMod(ci.PTypeMod, typeDesc);
                    if (result.Error) {
                        errorMessage = Sprintf("Invalid typemod for column %s: type %s, error %s",
                            name.c_str(), NScheme::TypeName(ci.PType, ci.PTypeMod).c_str(),
                            result.Error->c_str());
                        return false;
                    }
                    typmod = result.Typmod;
                }
            } else {
                errorMessage = Sprintf("Unexpected type for column %s: expected %s",
                                       name.c_str(), NScheme::TypeName(ci.PType).c_str());
                return false;
            }

            bool notNull = entry.NotNullColumns.contains(ci.Name);
            if (notNull) {
                notNullColumnsLeft.erase(ci.Name);
            }

            if (ci.KeyOrder != -1) {
                KeyColumnPositions[ci.KeyOrder] = TFieldDescription{ci.Id, ci.Name, (ui32)pos, ci.PType, typmod, notNull};
                keyColumnsLeft.erase(ci.Name);
                KeyColumnNames[ci.KeyOrder] = ci.Name;
            } else {
                ValueColumnPositions.emplace_back(TFieldDescription{ci.Id, ci.Name, (ui32)pos, ci.PType, typmod, notNull});
                ValueColumnNames.emplace_back(ci.Name);
                ValueColumnTypes.emplace_back(ci.PType);
            }
        }

        if (makeYqbSchema) {
            Id2Position.clear();
            YdbSchema.resize(KeyColumnTypes.size() + ValueColumnTypes.size());

            for (size_t i = 0; i < KeyColumnPositions.size(); ++i) {
                ui32 columnId = KeyColumnPositions[i].ColId;
                Id2Position[columnId] = i;
                YdbSchema[i] = std::make_pair(KeyColumnNames[i], KeyColumnPositions[i].Type);
            }
            for (size_t i = 0; i < ValueColumnPositions.size(); ++i) {
                ui32 columnId = ValueColumnPositions[i].ColId;
                size_t position = KeyColumnPositions.size() + i;
                Id2Position[columnId] = position;
                YdbSchema[position] = std::make_pair(ValueColumnNames[i], ValueColumnPositions[i].Type);
            }

            for (const auto& [colName, colType] : YdbSchema) {
                if (NArrow::TArrowToYdbConverter::NeedDataConversion(colType)) {
                    ColumnsToConvert[colName] = colType;
                }
            }
        }

        if (!keyColumnsLeft.empty()) {
            errorMessage = Sprintf("Missing key columns: %s", JoinSeq(", ", keyColumnsLeft).c_str());
            return false;
        }

        if (!notNullColumnsLeft.empty()) {
            errorMessage = Sprintf("Missing not null columns: %s", JoinSeq(", ", notNullColumnsLeft).c_str());
            return false;
        }

        return true;
    }

    void ResolveTable(const TString& table, const NActors::TActorContext& ctx) {
        // TODO: check all params;
        // Cerr << *Request->GetProtoRequest() << Endl;

        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = ::NKikimr::SplitPath(table);
        if (entry.Path.empty()) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Invalid table path specified", ctx);
        }
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = true;
        entry.ShowPrivatePath = AllowWriteToPrivateTable;
        request->ResultSet.emplace_back(entry);
        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request));

        TimeoutTimerActorId = CreateLongTimer(ctx, Timeout,
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup()));

        TBase::Become(&TThis::StateWaitResolveTable);
        WaitingResolveReply = true;
    }

    void HandleTimeout(const TActorContext& ctx) {
        ShardRepliesLeft.clear();
        return ReplyWithError(Ydb::StatusIds::TIMEOUT, TStringBuilder() << "Bulk upsert to table " << GetTable()
            << " longTx " << LongTxId.ToString()
            << " timed out, duration: " << (TAppData::TimeProvider->Now() - StartTime).Seconds() << " sec", ctx);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;

        Y_VERIFY(request.ResultSet.size() == 1);
        const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = request.ResultSet.front();

        switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                return ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Sprintf("Table '%s' unavaliable", GetTable().c_str()), ctx);
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("Unknown table '%s'", GetTable().c_str()), ctx);
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("Unknown database for table '%s'", GetTable().c_str()), ctx);
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
                return ReplyWithError(Ydb::StatusIds::GENERIC_ERROR, Sprintf("Unknown error on table '%s'", GetTable().c_str()), ctx);
        }

        TableKind = entry.Kind;
        bool isColumnTable = (TableKind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable);

        if (entry.TableId.IsSystemView()) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                Sprintf("Table '%s' is a system view. Bulk upsert is not supported.", GetTable().c_str()), ctx);
        }

        // TODO: fast fail for all tables?
        if (isColumnTable && DiskQuotaExceeded) {
            return ReplyWithError(Ydb::StatusIds::UNAVAILABLE,
                "Cannot perform writes: database is out of disk space", ctx);
        }

        ResolveNamesResult.reset(ev->Get()->Request.Release());

        bool makeYdbSchema = isColumnTable || (GetSourceType() != EUploadSource::ProtoValues);
        TString errorMessage;
        if (!BuildSchema(ctx, errorMessage, makeYdbSchema)) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, errorMessage, ctx);
        }

        switch (GetSourceType()) {
            case EUploadSource::ProtoValues:
            {
                if (!ExtractRows(errorMessage)) {
                    return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, errorMessage, ctx);
                }

                if (isColumnTable) {
                    // TUploadRowsRPCPublic::ExtractBatch() - converted JsonDocument, DynNumbers, ...
                    if (!ExtractBatch(errorMessage)) {
                        return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, errorMessage, ctx);
                    }
                } else {
                    FindMinMaxKeys();
                }
                break;
            }
            case EUploadSource::ArrowBatch:
            case EUploadSource::CSV:
            {
                if (isColumnTable) {
                    // TUploadColumnsRPCPublic::ExtractBatch() - NOT converted JsonDocument, DynNumbers, ...
                    if (!ExtractBatch(errorMessage)) {
                        return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, errorMessage, ctx);
                    }
                    if (!ColumnsToConvertInplace.empty()) {
                        Batch = NArrow::InplaceConvertColumns(Batch, ColumnsToConvertInplace);
                    }
                    // Explicit types conversion
                    if (!ColumnsToConvert.empty()) {
                        Batch = NArrow::ConvertColumns(Batch, ColumnsToConvert);
                        if (!Batch) {
                            errorMessage = "Cannot upsert arrow batch: one of data types has no conversion";
                            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, errorMessage, ctx);
                        }
                    }
                } else {
                    // TUploadColumnsRPCPublic::ExtractBatch() - NOT converted JsonDocument, DynNumbers, ...
                    if (!ExtractBatch(errorMessage)) {
                        return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, errorMessage, ctx);
                    }
                    // Implicit types conversion inside ExtractRows(), in TArrowToYdbConverter
                    if (!ExtractRows(errorMessage)) {
                        return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, errorMessage, ctx);
                    }
                    FindMinMaxKeys();
                }

                // (re)calculate RuCost for batch variant if it's bigger then RuCost calculated in ExtractRows()
                Y_VERIFY(Batch && Batch->num_rows() >= 0);
                ui32 numRows = Batch->num_rows();
                ui64 bytesSize = Max<ui64>(NArrow::GetBatchDataSize(Batch), GetSourceData().Size());
                float batchRuCost = TUpsertCost::CostToRu(TUpsertCost::BatchCost(bytesSize, numRows));
                if (batchRuCost > RuCost) {
                    RuCost = batchRuCost;
                }

                break;
            }
        }

        if (TableKind == NSchemeCache::TSchemeCacheNavigate::KindTable) {
            ResolveShards(ctx);
        } else if (isColumnTable) {
            // Batch is already converted
            WriteToColumnTable(ctx);
        } else {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                Sprintf("Table '%s': Bulk upsert is not supported for this table kind.", GetTable().c_str()), ctx);
        }
    }

    void WriteToColumnTable(const NActors::TActorContext& ctx) {
        TString accessCheckError;
        if (!CheckAccess(accessCheckError)) {
            return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, accessCheckError, ctx);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Bulk upsert to table " << GetTable()
                    << " starting LongTx");

        // Begin Long Tx for writing a batch into OLAP table
        TActorId longTxServiceId = NLongTxService::MakeLongTxServiceID(ctx.SelfID.NodeId());
        NKikimrLongTxService::TEvBeginTx::EMode mode = NKikimrLongTxService::TEvBeginTx::MODE_WRITE_ONLY;
        ctx.Send(longTxServiceId, new NLongTxService::TEvLongTxService::TEvBeginTx(GetDatabase(), mode));
        TBase::Become(&TThis::StateWaitBeginLongTx);
    }

    STFUNC(StateWaitBeginLongTx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NLongTxService::TEvLongTxService::TEvBeginTxResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NLongTxService::TEvLongTxService::TEvBeginTxResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();

        if (msg->Record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(msg->Record.GetIssues(), issues);
            for (const auto& issue: issues) {
                RaiseIssue(issue);
            }
            return ReplyWithResult(msg->Record.GetStatus(), ctx);
        }

        LongTxId = msg->GetLongTxId();

        LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Bulk upsert to table " << GetTable()
                    << " started LongTx " << LongTxId.ToString());

        auto outputColumns = GetOutputColumns(ctx);
        if (!outputColumns.empty()) {
            if (!Batch) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, "No batch in bulk upsert data", ctx);
            }

            auto batch = NArrow::ExtractColumns(Batch, outputColumns);
            if (!batch) {
                for (auto& columnName : outputColumns) {
                    if (Batch->schema()->GetFieldIndex(columnName) < 0) {
                        return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                            "No column '" + columnName + "' in bulk upsert data", ctx);
                    }
                }
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Cannot prepare bulk upsert data", ctx);
            }

            Y_VERIFY(batch);

#if 1 // TODO: check we call ValidateFull() once over pipeline (upsert -> long tx -> shard insert)
            auto validationInfo = batch->ValidateFull();
            if (!validationInfo.ok()) {
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Bad batch in bulk upsert data: " + validationInfo.message() + "; order:" + JoinSeq(", ", outputColumns), ctx);
            }
#endif

            Batch = batch;
        }

        WriteBatchInLongTx(ctx);
    }

    std::vector<TString> GetOutputColumns(const NActors::TActorContext& ctx) {
        Y_VERIFY(ResolveNamesResult);

        if (ResolveNamesResult->ErrorCount > 0) {
            ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Failed to get table schema", ctx);
            return {};
        }

        auto& entry = ResolveNamesResult->ResultSet[0];

        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "The specified path is not an olap table", ctx);
            return {};
        }

        if (!entry.ColumnTableInfo || !entry.ColumnTableInfo->Description.HasSchema()) {
            ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Olap table expected", ctx);
            return {};
        }

        const auto& description = entry.ColumnTableInfo->Description;
        const auto& schema = description.GetSchema();

        std::vector<TString> outColumns;
        outColumns.reserve(YdbSchema.size());

        for (size_t i = 0; i < (size_t)schema.GetColumns().size(); ++i) {
            auto columnId = schema.GetColumns(i).GetId();
            if (!Id2Position.count(columnId)) {
                continue;
            }
            size_t position = Id2Position[columnId];
            outColumns.push_back(YdbSchema[position].first);
        }

        Y_VERIFY(!outColumns.empty());
        return outColumns;
    }

    void WriteBatchInLongTx(const TActorContext& ctx) {
        Y_VERIFY(ResolveNamesResult);
        Y_VERIFY(Batch);

        TBase::Become(&TThis::StateWaitWriteBatchResult);
        ui32 batchNo = 0;
        TString dedupId = ToString(batchNo);
        NGRpcService::DoLongTxWriteSameMailbox(ctx, ctx.SelfID, LongTxId, dedupId,
            GetDatabase(), GetTable(), ResolveNamesResult, Batch, Issues);
    }

    void RollbackLongTx(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Bulk upsert to table " << GetTable()
                    << " rolling back LongTx " << LongTxId.ToString());

        TActorId longTxServiceId = NLongTxService::MakeLongTxServiceID(ctx.SelfID.NodeId());
        ctx.Send(longTxServiceId, new NLongTxService::TEvLongTxService::TEvRollbackTx(LongTxId));
    }

    STFUNC(StateWaitWriteBatchResult) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvCompleted, HandleWriteBatchResult);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleWriteBatchResult(TEvents::TEvCompleted::TPtr& ev, const TActorContext& ctx) {
        Ydb::StatusIds::StatusCode status = (Ydb::StatusIds::StatusCode)ev->Get()->Status;
        if (status != Ydb::StatusIds::SUCCESS) {
            Y_VERIFY(Issues);
            for (const auto& issue: *Issues) {
                RaiseIssue(issue);
            }
            Finished = true;
            return ReplyWithResult(status, ctx);
        }

        CommitLongTx(ctx);
    }

    void CommitLongTx(const TActorContext& ctx) {
        TActorId longTxServiceId = NLongTxService::MakeLongTxServiceID(ctx.SelfID.NodeId());
        ctx.Send(longTxServiceId, new NLongTxService::TEvLongTxService::TEvCommitTx(LongTxId));
        TBase::Become(&TThis::StateWaitCommitLongTx);
    }

    STFUNC(StateWaitCommitLongTx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NLongTxService::TEvLongTxService::TEvCommitTxResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NLongTxService::TEvLongTxService::TEvCommitTxResult::TPtr& ev, const NActors::TActorContext& ctx) {
        const auto* msg = ev->Get();

        Finished = true;

        if (msg->Record.GetStatus() == Ydb::StatusIds::SUCCESS) {
            // We are done with the transaction, forget it
            LongTxId = NLongTxService::TLongTxId();
        }

        NYql::TIssues issues;
        NYql::IssuesFromMessage(msg->Record.GetIssues(), issues);
        for (const auto& issue: issues) {
            RaiseIssue(issue);
        }
        return ReplyWithResult(msg->Record.GetStatus(), ctx);
    }

    void FindMinMaxKeys() {

        for (const auto& pair : GetRows()) {
             const auto& serializedKey = pair.first;

            if (MinKey.GetCells().empty()) {
                // Only for the first key
                MinKey = serializedKey;
                MaxKey = serializedKey;
            } else {
                // For all next keys
                if (CompareTypedCellVectors(serializedKey.GetCells().data(), MinKey.GetCells().data(),
                                            KeyColumnTypes.data(),
                                            serializedKey.GetCells().size(), MinKey.GetCells().size()) < 0)
                {
                    MinKey = serializedKey;
                } else if (CompareTypedCellVectors(serializedKey.GetCells().data(), MaxKey.GetCells().data(),
                                                   KeyColumnTypes.data(),
                                                   serializedKey.GetCells().size(), MaxKey.GetCells().size()) > 0)
                {
                    MaxKey = serializedKey;
                }
            }
        }
    }

    void ResolveShards(const NActors::TActorContext& ctx) {
        if (GetRows().empty()) {
            // We have already resolved the table and know it exists
            // No reason to resolve table range as well
            return ReplyIfDone(ctx);
        }

        Y_VERIFY(ResolveNamesResult);

        auto& entry = ResolveNamesResult->ResultSet.front();

        // We are going to set all columns
        TVector<TKeyDesc::TColumnOp> columns;
        for (const auto& ci : entry.Columns) {
            TKeyDesc::TColumnOp op = { ci.second.Id, TKeyDesc::EColumnOperation::Set, ci.second.PType, 0, 0 };
            columns.push_back(op);
        }

        TTableRange range(MinKey.GetCells(), true, MaxKey.GetCells(), true, false);
        auto keyRange = MakeHolder<TKeyDesc>(entry.TableId, range, TKeyDesc::ERowOperation::Update, KeyColumnTypes, columns);

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());

        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        ctx.Send(SchemeCache, resolveReq.Release());

        TBase::Become(&TThis::StateWaitResolveShards);
        WaitingResolveReply = true;
    }

    STFUNC(StateWaitResolveShards) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr &ev, const TActorContext &ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        TEvTxProxySchemeCache::TEvResolveKeySetResult *msg = ev->Get();
        ResolvePartitionsResult = msg->Request;

        if (ResolvePartitionsResult->ErrorCount > 0) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("Unknown table '%s'", GetTable().c_str()), ctx);
        }

        TString accessCheckError;
        if (!CheckAccess(accessCheckError)) {
            return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, accessCheckError, ctx);
        }

        auto getShardsString = [] (const TVector<TKeyDesc::TPartitionInfo>& partitions) {
            TVector<ui64> shards;
            shards.reserve(partitions.size());
            for (auto& partition : partitions) {
                shards.push_back(partition.ShardId);
            }

            return JoinVectorIntoString(shards, ", ");
        };

        LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Range shards: "
            << getShardsString(GetKeyRange()->GetPartitions()));

        MakeShardRequests(ctx);
    }

    void MakeShardRequests(const NActors::TActorContext& ctx) {
        const auto* keyRange = GetKeyRange();

        Y_VERIFY(!keyRange->GetPartitions().empty());

        // Group rows by shard id
        TVector<std::unique_ptr<TEvDataShard::TEvUploadRowsRequest>> shardRequests(keyRange->GetPartitions().size());
        for (const auto& keyValue : GetRows()) {
            // Find partition for the key
            auto it = std::lower_bound(keyRange->GetPartitions().begin(), keyRange->GetPartitions().end(), keyValue.first.GetCells(),
                [this](const auto &partition, const auto& key) {
                    const auto& range = *partition.Range;
                    const int cmp = CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                        range.IsInclusive || range.IsPoint, true, KeyColumnTypes);

                    return (cmp < 0);
                });

            size_t shardIdx = it - keyRange->GetPartitions().begin();

            TEvDataShard::TEvUploadRowsRequest* ev = shardRequests[shardIdx].get();
            if (!ev) {
                shardRequests[shardIdx].reset(new TEvDataShard::TEvUploadRowsRequest());
                ev = shardRequests[shardIdx].get();
                ev->Record.SetCancelDeadlineMs(Deadline().MilliSeconds());

                ev->Record.SetTableId(keyRange->TableId.PathId.LocalPathId);
                for (const auto& fd : KeyColumnPositions) {
                    ev->Record.MutableRowScheme()->AddKeyColumnIds(fd.ColId);
                }
                for (const auto& fd : ValueColumnPositions) {
                    ev->Record.MutableRowScheme()->AddValueColumnIds(fd.ColId);
                }
                if (WriteToTableShadow) {
                    ev->Record.SetWriteToTableShadow(true);
                }
            }

            auto* row = ev->Record.AddRows();
            row->SetKeyColumns(keyValue.first.GetBuffer());
            row->SetValueColumns(keyValue.second);
        }

        // Send requests to the shards
        for (size_t idx = 0; idx < shardRequests.size(); ++idx) {
            if (!shardRequests[idx])
                continue;

            TTabletId shardId = keyRange->GetPartitions()[idx].ShardId;

            LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Sending request to shards " << shardId);

            ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvForward(shardRequests[idx].release(), shardId, true), IEventHandle::FlagTrackDelivery);

            auto res = ShardRepliesLeft.insert(shardId);
            if (!res.second) {
                LOG_CRIT_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Upload rows: shard " << shardId << "has already been added!");
            }
        }

        TBase::Become(&TThis::StateWaitResults);

        // Sanity check: don't break when we don't have any shards for some reason
        ReplyIfDone(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        SetError(Ydb::StatusIds::INTERNAL_ERROR, "Internal error: pipe cache is not available, the cluster might not be configured properly");

        ShardRepliesLeft.clear();

        ReplyIfDone(ctx);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(GetKeyRange()->TableId, TActorId()));

        SetError(Ydb::StatusIds::UNAVAILABLE, Sprintf("Failed to connect to shard %" PRIu64, ev->Get()->TabletId));
        ShardRepliesLeft.erase(ev->Get()->TabletId);

        ReplyIfDone(ctx);
    }

    STFUNC(StateWaitResults) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvDataShard::TEvUploadRowsResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    void Handle(TEvDataShard::TEvUploadRowsResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        const auto& shardResponse = ev->Get()->Record;

        // Notify the cache that we are done with the pipe
        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(shardResponse.GetTabletID()));

        LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Upload rows: got "
                    << NKikimrTxDataShard::TError::EKind_Name((NKikimrTxDataShard::TError::EKind)shardResponse.GetStatus())
                    << " from shard " << shardResponse.GetTabletID());

        if (shardResponse.GetStatus() != NKikimrTxDataShard::TError::OK) {
            ::Ydb::StatusIds::StatusCode status = Ydb::StatusIds::GENERIC_ERROR;

            switch (shardResponse.GetStatus()) {
            case NKikimrTxDataShard::TError::WRONG_SHARD_STATE:
                ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(GetKeyRange()->TableId, TActorId()));
                status = Ydb::StatusIds::OVERLOADED;
                break;
            case NKikimrTxDataShard::TError::OUT_OF_SPACE:
                status = Ydb::StatusIds::UNAVAILABLE;
                break;
            case NKikimrTxDataShard::TError::SCHEME_ERROR:
                status = Ydb::StatusIds::SCHEME_ERROR;
                break;
            case NKikimrTxDataShard::TError::BAD_ARGUMENT:
                status = Ydb::StatusIds::BAD_REQUEST;
                break;
            case NKikimrTxDataShard::TError::EXECUTION_CANCELLED:
                status = Ydb::StatusIds::TIMEOUT;
                break;
            };

            SetError(status, shardResponse.GetErrorDescription());
        }

        ShardRepliesLeft.erase(shardResponse.GetTabletID());

        ReplyIfDone(ctx);
    }

    void SetError(::Ydb::StatusIds::StatusCode status, const TString& message) {
        if (Status != ::Ydb::StatusIds::SUCCESS) {
            return;
        }

        Status = status;
        ErrorMessage = message;
    }

    void ReplyIfDone(const NActors::TActorContext& ctx) {
        if (!ShardRepliesLeft.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Upload rows: waiting for " << ShardRepliesLeft.size() << " shards replies");
            return;
        }

        Finished = true;

        if (!ErrorMessage.empty()) {
            RaiseIssue(NYql::TIssue(ErrorMessage));
        }

        ReplyWithResult(Status, ctx);
    }

    void ReplyWithError(::Ydb::StatusIds::StatusCode status, const TString& message, const TActorContext& ctx) {
        SetError(status, message);

        Y_VERIFY_DEBUG(ShardRepliesLeft.empty());
        ReplyIfDone(ctx);
    }

    void ReplyWithResult(::Ydb::StatusIds::StatusCode status, const TActorContext& ctx) {
        SendResult(ctx, status);

        LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Bulk upsert to table " << GetTable()
                    << " completed with status " << status);

        if (LongTxId != NLongTxService::TLongTxId()) {
            // LongTxId is reset after successful commit
            // If it si still there it means we need to rollback
            Y_VERIFY_DEBUG(status != ::Ydb::StatusIds::SUCCESS);
            RollbackLongTx(ctx);
        }

        if (!WaitingResolveReply) {
            Die(ctx);
        }
    }
};

using TFieldDescription = NTxProxy::TUploadRowsBase<NKikimrServices::TActivity::GRPC_REQ>::TFieldDescription;

template <class TProto>
inline bool FillCellsFromProto(TVector<TCell>& cells, const TVector<TFieldDescription>& descr, const TProto& proto,
                            TString& err, TMemoryPool& valueDataPool)
{
    cells.clear();
    cells.reserve(descr.size());

    for (auto& fd : descr) {
        if (proto.items_size() <= (int)fd.PositionInStruct) {
            err = "Invalid request";
            return false;
        }
        cells.push_back({});
        if (!CellFromProtoVal(fd.Type, fd.Typmod, &proto.Getitems(fd.PositionInStruct), cells.back(), err, valueDataPool)) {
            return false;
        }

        if (fd.NotNull && cells.back().IsNull()) {
            err = TStringBuilder() << "Received NULL value for not null column: " << fd.ColName;
            return false;
        }
    }

    return true;
}

} // namespace NTxProxy
} // namespace NKikimr
