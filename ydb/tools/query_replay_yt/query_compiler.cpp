#include "query_replay.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <util/string/escape.h>
#include <util/string/strip.h>

#include <ydb/core/client/scheme_cache_lib/yql_db_scheme_resolver.h>

#include <memory>

using namespace NKikimrConfig;
using namespace NThreading;
using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr;
using namespace NKikimr::NKqp;


enum EReadType : ui32 {
    Lookup = 1,
    Scan = 2,
    FullScan = 3
};

TString ToString(EReadType readType) {
    switch (readType) {
        case Lookup:
            return "lookup";
        case Scan:
            return "scan";
        case FullScan:
            return "fullscan";
    }

    return "unspecified";
}

struct TTableReadAccessInfo {
    EReadType ReadType;
    i32 PushedLimit = -1;
    std::vector<std::string> ReadColumns;

    constexpr bool operator==(const TTableReadAccessInfo& other) const {
        return std::tie(ReadType, PushedLimit, ReadColumns) == std::tie(other.ReadType, other.PushedLimit, other.ReadColumns);
    }

    constexpr bool operator<(const TTableReadAccessInfo& other) const  {
        return std::tie(ReadType, PushedLimit, ReadColumns) < std::tie(other.ReadType, other.PushedLimit, other.ReadColumns);
    }
};

enum EWriteType : ui32 {
    Upsert = 1,
    Erase = 2
};

struct TTableWriteInfo {
    EWriteType WriteType;
    std::vector<std::string> WriteColumns;

    constexpr bool operator==(const TTableWriteInfo& other) const {
        return std::tie(WriteType, WriteColumns) == std::tie(other.WriteType, other.WriteColumns);
    }

    constexpr bool operator<(const TTableWriteInfo& other) const  {
        return std::tie(WriteType, WriteColumns) < std::tie(other.WriteType, other.WriteColumns);
    }
};

struct TTableStats {
    TString Name;
    std::vector<TTableReadAccessInfo> Reads;
    std::vector<TTableWriteInfo> Writes;

    constexpr bool operator==(const TTableStats& other) const {
        return std::tie(Name, Reads, Writes) == std::tie(other.Name, other.Reads, other.Writes);
    }

    constexpr bool operator<(const TTableStats& other) const {
        return std::tie(Name, Reads, Writes) < std::tie(other.Name, other.Reads, other.Writes);
    }
};

struct TMetadataInfoHolder {
    const THashMap<TString, NYql::TKikimrTableMetadataPtr> TableMetadata;
    THashMap<TString, NYql::TKikimrTableMetadataPtr> Indexes;

    explicit TMetadataInfoHolder(THashMap<TString,  NYql::TKikimrTableMetadataPtr>&& tableMetadata)
        : TableMetadata(tableMetadata)
    {
        for (auto& [name, ptr] : TableMetadata) {
            for (auto& secondary : ptr->SecondaryGlobalIndexMetadata) {
                Indexes.emplace(secondary->Name, secondary);
            }
        }
    }

    THashMap<TString, NYql::TKikimrTableMetadataPtr>::const_iterator find(const TString& key) {
        return TableMetadata.find(key);
    }

    const NYql::TKikimrTableMetadataPtr* FindPtr(const TString& key) const {
        const auto* result = TableMetadata.FindPtr(key);
        if (result != nullptr) {
            return result;
        }

        return Indexes.FindPtr(key);
    }

    THashMap<TString, NYql::TKikimrTableMetadataPtr>::const_iterator begin() const {
        return TableMetadata.begin();
    }

    THashMap<TString, NYql::TKikimrTableMetadataPtr>::const_iterator end() const {
        return TableMetadata.end();
    }
};


class TStaticTableMetadataLoader: public NYql::IKikimrGateway::IKqpTableMetadataLoader, public NYql::IDbSchemeResolver {
    TActorSystem* ActorSystem;
    std::shared_ptr<TMetadataInfoHolder> TableMetadata;

public:
    TStaticTableMetadataLoader(TActorSystem* actorSystem, std::shared_ptr<TMetadataInfoHolder>& tableMetadata)
        : ActorSystem(actorSystem)
        , TableMetadata(tableMetadata)
    {}

    NThreading::TFuture<NYql::IKikimrGateway::TTableMetadataResult> LoadTableMetadata(
        const TString& cluster, const TString& table, const NYql::IKikimrGateway::TLoadTableMetadataSettings& settings, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) override {
        Y_UNUSED(cluster);
        Y_UNUSED(settings);
        Y_UNUSED(database);
        Y_UNUSED(userToken);
        auto ptr = TableMetadata->find(table);
        Y_ENSURE(ptr != TableMetadata->end());

        NYql::IKikimrGateway::TTableMetadataResult result;
        result.SetSuccess();
        result.Metadata = ptr->second;
        return MakeFuture<NYql::IKikimrGateway::TTableMetadataResult>(result);
    }

    TTableResult MakeTable(const TTable& table, const NYql::TKikimrTableMetadataPtr meta) const {
        TTableResult reply{TTableResult::Ok};
        reply.Status = TTableResult::Ok;
        reply.Table.TableName = table.TableName;
        reply.CacheGeneration = meta->SchemaVersion;
        reply.TableId = new TTableId(meta->PathId.OwnerId(), meta->PathId.TableId(), meta->SchemaVersion);
        reply.KeyColumnCount = meta->KeyColumnNames.size();
        TVector<TString> ColumnNames;
        ColumnNames.reserve(meta->Columns.size());
        THashMap<TString, i32> KeyPosition;
        i32 idx = 0;
        for (auto& key : meta->KeyColumnNames) {
            KeyPosition[key] = idx;
            ++idx;
        }

        for (auto& [name, column] : meta->Columns) {
            ColumnNames.push_back(name);
            reply.Columns[name] = TTableResult::TColumn{column.Id, KeyPosition[name], column.TypeInfo, 0, EColumnTypeConstraint::Nullable};
        }

        return reply;
    }

    TTableResults Resolve(const TVector<TTable>& tables) const {
        TTableResults results;
        for (auto& table : tables) {
            const NYql::TKikimrTableMetadataPtr* metaIt = TableMetadata->FindPtr(table.TableName);
            if (metaIt != nullptr) {
                results.push_back(MakeTable(table, *metaIt));
                continue;
            }

            results.push_back({TTableResult::LookupError, TStringBuilder() << "Unknow table " << table.TableName});
        }

        return results;
    }

    virtual NThreading::TFuture<TTableResults> ResolveTables(const TVector<TTable>& tables) override {
        return NThreading::MakeFuture(Resolve(tables));
    }

    virtual void ResolveTables(const TVector<TTable>& tables, NActors::TActorId responseTo) override {
        auto results = Resolve(tables);
        ActorSystem->Send(responseTo, new NYql::IDbSchemeResolver::TEvents::TEvResolveTablesResult(std::move(results)));
    }

    TVector<NKikimrKqp::TKqpTableMetadataProto> GetCollectedSchemeData() override {
        return {};
    }
};

class TReplayCompileActor: public TActorBootstrapped<TReplayCompileActor> {
public:
    TReplayCompileActor(TIntrusivePtr<TModuleResolverState> moduleResolverState, const NMiniKQL::IFunctionRegistry* functionRegistry)
        : ModuleResolverState(moduleResolverState)
        , KqpSettings()
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , FunctionRegistry(functionRegistry)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateInit);
    }

    void PassAway() override {
        TActor::PassAway();
    }

private:
    STATEFN(StateInit) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TQueryReplayEvents::TEvCompileRequest, Handle);
                default:
                    Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected event in StateInit");
            }
        } catch (const yexception& e) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, e.what());
        }
    }
    STATEFN(StateCompile) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvContinueProcess, Handle);
                cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
                default:
                    Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected event in CompileState");
            }
        } catch (const yexception& e) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, e.what());
        }
    }

private:

    void StartCompilation() {
        IKqpHost::TPrepareSettings prepareSettings;
        prepareSettings.DocumentApiRestricted = false;

        switch (Query->Settings.QueryType) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
                AsyncCompileResult = KqpHost->PrepareDataQuery(Query->Text, prepareSettings);
                break;

            case NKikimrKqp::QUERY_TYPE_AST_DML:
                AsyncCompileResult = KqpHost->PrepareDataQueryAst(Query->Text, prepareSettings);
                break;

            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN:
                AsyncCompileResult = KqpHost->PrepareScanQuery(Query->Text, Query->IsSql(), prepareSettings);
                break;
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
                AsyncCompileResult = KqpHost->PrepareGenericScript(Query->Text, prepareSettings);
                break;
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY:
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
            default:
                YQL_ENSURE(false, "Unexpected query type: " << Query->Settings.QueryType);
        }
    }
    void Continue() {
        TActorSystem* actorSystem = TlsActivationContext->ExecutorThread.ActorSystem;
        TActorId selfId = SelfId();
        auto callback = [actorSystem, selfId](const TFuture<bool>& future) {
            bool finished = future.GetValue();
            auto processEv = MakeHolder<TEvKqp::TEvContinueProcess>(0, finished);
            actorSystem->Send(selfId, processEv.Release());
        };

        AsyncCompileResult->Continue().Apply(callback);
    }

    NJson::TJsonValue ExtractQueryPlan(const TString& plan) {
        static NJson::TJsonReaderConfig readConfig;
        TStringInput in(plan);
        NJson::TJsonValue reply;
        NJson::ReadJsonTree(&in, &readConfig, &reply, false);
        return reply;
    }

    std::map<std::string, TTableStats> ExtractTableStats(const NJson::TJsonValue& rhs) {
        std::map<std::string, TTableStats> result;

        if (rhs.Has("tables")) {
            for(const auto& table : rhs["tables"].GetArraySafe()) {
                TString name = table["name"].GetStringSafe();
                std::vector<TTableReadAccessInfo> reads;
                std::vector<TTableWriteInfo> writes;

                if (table.Has("reads")) {
                    for(const auto& read: table["reads"].GetArraySafe()) {
                        std::vector <std::string> columns;
                        if (read.Has("columns")) {
                            const auto tableMetadata = TableMetadata->FindPtr(name);
                            Y_ENSURE(tableMetadata);
                            const auto &keyColumnNames = tableMetadata->Get()->KeyColumnNames;
                            std::unordered_set <std::string_view> keyColumns(keyColumnNames.begin(),
                                                                             keyColumnNames.end());
                            for (const auto &column : read["columns"].GetArraySafe()) {
                                if (!keyColumns.contains(column.GetStringSafe())) {
                                    columns.push_back(column.GetStringSafe());
                                }
                            }
                            std::sort(columns.begin(), columns.end());
                        }

                        const auto &type = read["type"].GetStringSafe();
                        i32 limit = -1;
                        if (read.Has("limit") && read["limit"].IsInteger()) {
                            limit = read["limit"].GetIntegerSafe();
                        }

                        bool probablyLookupScan = false;
                        if (read.Has("lookup_by") && read.Has("scan_by")) {
                            probablyLookupScan = true;
                        }

                        if (type == "Scan") {
                            reads.push_back(TTableReadAccessInfo{EReadType::Scan, limit, columns});
                        } else if (type == "FullScan") {
                            reads.push_back(TTableReadAccessInfo{EReadType::FullScan, limit, columns});
                        } else if (type == "Lookup") {
                            if (probablyLookupScan) {
                                reads.push_back(TTableReadAccessInfo{EReadType::Scan, limit, columns});
                            } else {
                                reads.push_back(TTableReadAccessInfo{EReadType::Lookup, limit, columns});
                            }
                        } else if (type == "MultiLookup") {
                            reads.push_back(TTableReadAccessInfo{EReadType::Lookup, limit, columns});
                        }
                    }

                    std::sort(reads.begin(), reads.end());
                    auto last = std::unique(reads.begin(), reads.end());
                    reads.erase(last, reads.end());
                }

                if (table.Has("writes")) {
                    for (const auto& write : table["writes"].GetArraySafe()) {
                        std::vector<std::string> columns;
                        if (write.Has("columns")) {
                            const auto tableMetadata = TableMetadata->FindPtr(name);
                            Y_ENSURE(tableMetadata);
                            const auto& keyColumnNames = tableMetadata->Get()->KeyColumnNames;
                            std::unordered_set<std::string_view> keyColumns(keyColumnNames.begin(), keyColumnNames.end());
                            for (const auto& column : write["columns"].GetArraySafe()) {
                                if (!keyColumns.contains(column.GetStringSafe())) {
                                    columns.push_back(column.GetStringSafe());
                                }
                            }
                            std::sort(columns.begin(), columns.end());
                        }

                        const auto& type = write["type"].GetStringSafe();
                        if (type == "Upsert" || type == "MultiUpsert") {
                            writes.push_back(TTableWriteInfo{EWriteType::Upsert, columns});
                        } else if (type == "Erase" || type == "MultiErase") {
                            writes.push_back(TTableWriteInfo{EWriteType::Erase, columns});
                        }
                    }

                    std::sort(writes.begin(), writes.end());
                }

                result.emplace(name, TTableStats{name, std::move(reads), std::move(writes)});
            }
        }

        return result;
    }

    void WriteJsonData(const TString& fname, const NJson::TJsonValue& data) {
        TFileOutput out(ReplayDetails["query_id"].GetStringSafe() + fname);
        NJson::WriteJson(&out, &data, true);
    }

    void WriteQueryMismatchInfo(const NJson::TJsonValue& lhs, const NJson::TJsonValue& rhs) {
        Cerr << "Found plan mismatch, query " << ReplayDetails["query_id"].GetStringSafe() << Endl;
        WriteJsonData("-repro.txt", ReplayDetails);
        WriteJsonData("-plan-lhs.txt", lhs);
        WriteJsonData("-plan-rhs.txt", rhs);
    }

    std::pair<TQueryReplayEvents::TCheckQueryPlanStatus, TString> OnTableOperationsMismatch(const TTableStats& oldEngineStats, const TTableStats& newEngineStats) {
        Y_ENSURE(oldEngineStats.Name == newEngineStats.Name);
        if (oldEngineStats.Reads.size() > newEngineStats.Reads.size()) {
            return {TQueryReplayEvents::ExtraReadingOldEngine, TStringBuilder()
                << "Extra reading in old engine plan for table " << oldEngineStats.Name};
        }

        if (oldEngineStats.Reads.size() < newEngineStats.Reads.size()) {
            return {TQueryReplayEvents::ExtraReadingNewEngine, TStringBuilder()
                << "Extra reading in new engine plan for table " << newEngineStats.Name};
        }

        for (size_t i = 0; i < oldEngineStats.Reads.size(); ++i) {
            if (oldEngineStats.Reads[i].ReadType != newEngineStats.Reads[i].ReadType) {
                return {TQueryReplayEvents::ReadTypesMismatch, TStringBuilder() << "Read types mismatch, old engine: "
                    << ToString(oldEngineStats.Reads[i].ReadType) << ", new engine: " << ToString(newEngineStats.Reads[i].ReadType)};
            }

            if (oldEngineStats.Reads[i].PushedLimit != newEngineStats.Reads[i].PushedLimit) {
                return {TQueryReplayEvents::ReadLimitsMismatch, TStringBuilder() << "Read limits mismatch, old engine: "
                    << oldEngineStats.Reads[i].PushedLimit << ", new engine: " << newEngineStats.Reads[i].PushedLimit};
            }

            if (oldEngineStats.Reads[i].ReadColumns != newEngineStats.Reads[i].ReadColumns) {
                return {TQueryReplayEvents::ReadColumnsMismatch, TStringBuilder() << "Read columns mismatch"};
            }
        }

        if (oldEngineStats.Writes.size() > newEngineStats.Writes.size()) {
            return {TQueryReplayEvents::ExtraWriting, TStringBuilder()
                << "Extra write operation in old engine plan for table " << oldEngineStats.Name};
        }

        if (oldEngineStats.Writes.size() < newEngineStats.Writes.size()) {
            return {TQueryReplayEvents::ExtraWriting, TStringBuilder()
                << "Extra write operation in new engine plan for table " << newEngineStats.Name};
        }

        for (size_t i = 0; i < oldEngineStats.Writes.size(); ++i) {
            if (oldEngineStats.Writes[i].WriteColumns != newEngineStats.Writes[i].WriteColumns) {
                return {TQueryReplayEvents::WriteColumnsMismatch, TStringBuilder() << "Write columns mismatch"};
            }
        }

        return {TQueryReplayEvents::UncategorizedPlanMismatch, ""};
    }

    std::pair<TQueryReplayEvents::TCheckQueryPlanStatus, TString> CheckQueryPlan(const NJson::TJsonValue& newEnginePlan) {
        NJson::TJsonValue oldEnginePlan = ExtractQueryPlan(ReplayDetails["query_plan"].GetStringSafe());
        const auto oldEngineStats = ExtractTableStats(oldEnginePlan);
        const auto newEngineStats = ExtractTableStats(newEnginePlan);

        for (const auto& [table, stats] : oldEngineStats) {
            auto it = newEngineStats.find(table);
            if (it == newEngineStats.end()) {
                WriteQueryMismatchInfo(oldEnginePlan, newEnginePlan);
                return {TQueryReplayEvents::TableMissing,
                    TStringBuilder() << "Table " << table << " not found in new engine plan"};
            }

            if (stats != it->second) {
                WriteQueryMismatchInfo(oldEnginePlan, newEnginePlan);
                return OnTableOperationsMismatch(stats, it->second);
            }
        }

        return {TQueryReplayEvents::Success, ""};
    }

    void Reply(const Ydb::StatusIds::StatusCode& status, const TString& message) {
        NYql::TIssue issue(NYql::TPosition(), message);
        Reply(status, {issue});
    }

    void Reply(const Ydb::StatusIds::StatusCode& status, const TIssues& issues, const std::optional<TString>& queryPlan = std::nullopt) {
        std::unique_ptr<TQueryReplayEvents::TEvCompileResponse> ev = std::make_unique<TQueryReplayEvents::TEvCompileResponse>(true);
        Y_UNUSED(queryPlan);
        if (status != Ydb::StatusIds::SUCCESS) {
            ev->Success = false;
            ev->Status = status == Ydb::StatusIds::TIMEOUT ? TQueryReplayEvents::CompileTimeout : TQueryReplayEvents::CompileError;
            ev->Message = issues.ToString();
            Cerr << "Failed to compile query: " << ev->Message << Endl;
            WriteJsonData("-repro.txt", ReplayDetails);
        } else {
            Y_ENSURE(queryPlan);
            ev->Plan = *queryPlan;
            std::tie(ev->Status, ev->Message) = CheckQueryPlan(ExtractQueryPlan(*queryPlan));
        }

        Send(Owner, ev.release());
        PassAway();
    }

    static THashMap<TString, NYql::TKikimrTableMetadataPtr> ExtractStaticMetadata(const NJson::TJsonValue& data, EMetaSerializationType metaType) {
        THashMap<TString, NYql::TKikimrTableMetadataPtr> meta;

        if (metaType == EMetaSerializationType::EncodedProto) {
            static NJson::TJsonReaderConfig readerConfig;
            NJson::TJsonValue tablemetajson;
            TStringInput in(data.GetStringSafe());
            NJson::ReadJsonTree(&in, &readerConfig, &tablemetajson, false);
            Y_ENSURE(tablemetajson.IsArray());
            for (auto& node : tablemetajson.GetArray()) {
                NKikimrKqp::TKqpTableMetadataProto proto;

                TString decoded = Base64Decode(node.GetStringRobust());
                Y_ENSURE(proto.ParseFromString(decoded));

                NYql::TKikimrTableMetadataPtr ptr = MakeIntrusive<NYql::TKikimrTableMetadata>(&proto);
                meta.emplace(proto.GetName(), ptr);
            }
        } else {
            Y_ENSURE(data.IsArray());
            for (auto& node : data.GetArray()) {
                NKikimrKqp::TKqpTableMetadataProto proto;
                NProtobufJson::Json2Proto(node.GetStringRobust(), proto);
                NYql::TKikimrTableMetadataPtr ptr = MakeIntrusive<NYql::TKikimrTableMetadata>(&proto);
                meta.emplace(proto.GetName(), ptr);
            }
        }
        return meta;
    }

    void Handle(TQueryReplayEvents::TEvCompileRequest::TPtr& ev) {
        Owner = ev->Sender;

        ReplayDetails = std::move(ev->Get()->ReplayDetails);

        EMetaSerializationType metaType = EMetaSerializationType::EncodedProto;
        if (ReplayDetails.Has("table_meta_serialization_type")) {
            metaType = static_cast<EMetaSerializationType>(ReplayDetails["table_meta_serialization_type"].GetUIntegerSafe());
        }
        TableMetadata = std::make_shared<TMetadataInfoHolder>(std::move(ExtractStaticMetadata(ReplayDetails["table_metadata"], metaType)));
        TString queryText = UnescapeC(ReplayDetails["query_text"].GetStringSafe());

        std::map<TString, Ydb::Type> queryParameterTypes;
        if (ReplayDetails.Has("query_parameter_types")) {
            for (const auto& [paramName, paramType] : ReplayDetails["query_parameter_types"].GetMapSafe()) {
                if (!queryParameterTypes[paramName].ParseFromString(Base64Decode(paramType.GetStringSafe()))) {
                    queryParameterTypes.erase(paramName);
                }
            }
        }

        const google::protobuf::EnumDescriptor *descriptor = NKikimrKqp::EQueryType_descriptor();
        auto queryType = NKikimrKqp::QUERY_TYPE_SQL_DML;
        if (ReplayDetails.Has("query_type")) {
            auto res = descriptor->FindValueByName(ReplayDetails["query_type"].GetStringSafe());
            if (res) {
                queryType = static_cast<NKikimrKqp::EQueryType>(res->number());
            }
        }

        TKqpQuerySettings settings(queryType);
        Query = std::make_unique<NKikimr::NKqp::TKqpQueryId>(
            ReplayDetails["query_cluster"].GetStringSafe(),
            ReplayDetails["query_database"].GetStringSafe(),
            queryText,
            settings,
            !queryParameterTypes.empty()
                ? std::make_shared<std::map<TString, Ydb::Type>>(std::move(queryParameterTypes))
                : nullptr,
            GUCSettings ? *GUCSettings : TGUCSettings());

        GUCSettings->ImportFromJson(ReplayDetails);

        Config->Init(KqpSettings.DefaultSettings.GetDefaultSettings(), ReplayDetails["query_cluster"].GetStringSafe(), KqpSettings.Settings, false);
        if (!Query->Database.empty()) {
            Config->_KqpTablePathPrefix = ReplayDetails["query_database"].GetStringSafe();
        }

        ui32 syntax = (ReplayDetails["query_syntax"].GetStringSafe() == "1") ? 1 : 0;
        if (queryType == NKikimrKqp::QUERY_TYPE_SQL_SCAN) {
            syntax = 1;
        }
	    Config->_KqpYqlSyntaxVersion = syntax;
        Config->FreezeDefaults();

        std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader = make_shared<TStaticTableMetadataLoader>(TlsActivationContext->ActorSystem(), TableMetadata);

        auto c = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto counters = MakeIntrusive<TKqpRequestCounters>();
        counters->Counters = new TKqpCounters(c);
        counters->TxProxyMon = new NTxProxy::TTxProxyMon(c);

        Gateway = CreateKikimrIcGateway(Query->Cluster, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY, Query->Database, std::move(loader),
            TlsActivationContext->ExecutorThread.ActorSystem, SelfId().NodeId(), counters);
        auto federatedQuerySetup = std::make_optional<TKqpFederatedQuerySetup>({NYql::IHTTPGateway::Make(), nullptr, nullptr, nullptr, {}, {}, {}, nullptr, nullptr, {}});
        KqpHost = CreateKqpHost(Gateway, Query->Cluster, Query->Database, Config, ModuleResolverState->ModuleResolver,
            federatedQuerySetup, nullptr, GUCSettings, NKikimrConfig::TQueryServiceConfig(), Nothing(), FunctionRegistry, false);

        StartCompilation();
        Continue();

        Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
        Become(&TThis::StateCompile);
    }

    void Handle(TEvKqp::TEvContinueProcess::TPtr& ev) {
        Y_ENSURE(!ev->Get()->QueryId);

        if (!ev->Get()->Finished) {
            Continue();
            return;
        }

        auto kqpResult = AsyncCompileResult->GetResult();
        auto status = GetYdbStatus(kqpResult);

        std::optional<TString> queryPlan;

        if (status == Ydb::StatusIds::SUCCESS) {
            queryPlan = std::move(kqpResult.QueryPlan);
        }

        Reply(status, kqpResult.Issues(), queryPlan);
    }

    void HandleTimeout() {
        return Reply(Ydb::StatusIds::TIMEOUT, "Query compilation timed out.");
    }

private:
    TActorId Owner;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    std::unique_ptr<TKqpQueryId> Query;
    TGUCSettings::TPtr GUCSettings = std::make_shared<TGUCSettings>();
    TKqpSettings KqpSettings;
    TKikimrConfiguration::TPtr Config;
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<IKqpHost> KqpHost;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncCompileResult;
    const NMiniKQL::IFunctionRegistry* FunctionRegistry;
    std::shared_ptr<TMetadataInfoHolder> TableMetadata;
    TActorId MiniKQLCompileService;
    NJson::TJsonValue ReplayDetails;
};

IActor* CreateQueryCompiler(TIntrusivePtr<TModuleResolverState> moduleResolverState,
    const NMiniKQL::IFunctionRegistry* functionRegistry)
{
    return new TReplayCompileActor(moduleResolverState, functionRegistry);
}
