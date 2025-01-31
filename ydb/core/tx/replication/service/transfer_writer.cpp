#include "json_change_record.h"
#include "logging.h"
#include "transfer_writer.h"
#include "worker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/replication/service/lightweight_schema.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/filters/purecalc_filter.h>
#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/providers/common/schema/parser/yql_type_parser.h>
#include <yql/essentials/public/purecalc/common/interface.h>
#include <yql/essentials/public/purecalc/helpers/stream/stream_from_vector.h>
#include <yql/essentials/public/udf/udf_string.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/core/persqueue/purecalc/purecalc.h>


#include <library/cpp/json/json_writer.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>

using namespace NFq::NRowDispatcher;

namespace NKikimr::NReplication::NService {

namespace {

constexpr const char* RESULT_COLUMN_NAME = "__ydb_r";

using namespace NYql::NPureCalc;
using namespace NKikimr::NMiniKQL;

struct TSchemaColumn {
    TString Name;
    TString TypeYson;

    bool operator==(const TSchemaColumn& other) const = default;

    TString ToString() const;
};


struct TOutputType {
    // TODO
};

class TMessageOutputSpec : public NYql::NPureCalc::TOutputSpecBase {
public:
    explicit TMessageOutputSpec(const TVector<TSchemaColumn>& tableColumns, const NYT::TNode& schema)
        : TableColumns(tableColumns)
        , Schema(schema)
    {}

public:
    const NYT::TNode& GetSchema() const override {
        return Schema;
    }

    const TVector<TSchemaColumn> GetTableColumns() const {
        return TableColumns;
    }

private:
    const TVector<TSchemaColumn> TableColumns;
    const NYT::TNode Schema;
};

class TOutputListImpl final: public IStream<TOutputType*> {
protected:
    TWorkerHolder<IPullListWorker> WorkerHolder_;
    const TMessageOutputSpec& OutputSpec;

public:
    explicit TOutputListImpl(const TMessageOutputSpec& outputSpec, TWorkerHolder<IPullListWorker> worker)
        : WorkerHolder_(std::move(worker))
        , OutputSpec(outputSpec)
    {
    }

public:
    TOutputType* Fetch() override {
        TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

        with_lock(WorkerHolder_->GetScopedAlloc()) {
            NYql::NUdf::TUnboxedValue value;

            if (!WorkerHolder_->GetOutputIterator().Next(value)) {
                return nullptr;
            }

            auto v = value.GetElement(0);

            size_t i = 0;
            for (auto& c : OutputSpec.GetTableColumns()) {
                auto e = v.GetElement(i++);

                // TODO
                if (!e.HasValue()) {
                    Cerr << ">>>>> " << c.Name << " IS NULL" << Endl << Flush;
                } else if (c.TypeYson == "Uint32") {
                    Cerr << ">>>>> " << c.Name << " = " << e.Get<ui32>() << Endl << Flush;
                } else if (c.TypeYson == "Utf8") {
                    Cerr << ">>>>> " << c.Name << " = '" << TString(e.AsStringRef()) << "'" << Endl << Flush;
                }
            }

            return nullptr; // new TOutputType(); // TODO кто собирает мусор?
        }
    }
};

} // namespace

} // namespace NKikimr::NReplication::NService


template <>
struct NYql::NPureCalc::TOutputSpecTraits<NKikimr::NReplication::NService::TMessageOutputSpec> {
    static const constexpr bool IsPartial = false;

//    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = true;
//    static const constexpr bool SupportPushStreamMode = true;

    using TOutputItemType = NKikimr::NReplication::NService::TOutputType*;
    using TPullStreamReturnType = THolder<IStream<TOutputItemType>>;
    using TPullListReturnType = THolder<IStream<TOutputItemType>>;

//    static const constexpr TOutputItemType StreamSentinel = nullptr;

    //static TPullStreamReturnType ConvertPullStreamWorkerToOutputType(const NKikimr::NReplication::NService::TMessageOutputSpec&, TWorkerHolder<IPullStreamWorker>);
    static TPullListReturnType ConvertPullListWorkerToOutputType(
        const NKikimr::NReplication::NService::TMessageOutputSpec& outputSpec,
        TWorkerHolder<IPullListWorker> worker
    ) {
        return MakeHolder<NKikimr::NReplication::NService::TOutputListImpl>(outputSpec, std::move(worker));
    }
};


namespace NKikimr::NReplication::NService {

namespace {

NYT::TNode CreateTypeNode(const TString& fieldType) {
    return NYT::TNode::CreateList()
        .Add("DataType")
        .Add(fieldType);
}

NYT::TNode CreateOptionalTypeNode(const TString& fieldType) {
    return NYT::TNode::CreateList()
        .Add("OptionalType")
        .Add(CreateTypeNode(fieldType));
}


void AddField(NYT::TNode& node, const TString& fieldName, const TString& fieldType) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(CreateOptionalTypeNode(fieldType))
    );
}

NYT::TNode MakeOutputSchema(const TVector<TSchemaColumn>& columns) {
    auto structMembers = NYT::TNode::CreateList();

    for (const auto& column : columns) {
        AddField(structMembers, column.Name, column.TypeYson);
    }

    auto rootMembers = NYT::TNode::CreateList();
    rootMembers.Add(
        NYT::TNode::CreateList()
            .Add(RESULT_COLUMN_NAME)
            .Add(NYT::TNode::CreateList()
                .Add("StructType")
                .Add(std::move(structMembers)))
    );

    return NYT::TNode::CreateList().Add("StructType").Add(std::move(rootMembers));
}

class TProgramHolder : public NFq::IProgramHolder {
public:
    using TPtr = TIntrusivePtr<TProgramHolder>;

public:
    TProgramHolder(
        const TVector<TSchemaColumn>& tableColumns,
        const TString& sql
    )
        : TopicColumns()
        , TableColumns(tableColumns)
        , Sql(sql)
    {}

public:
    void CreateProgram(NYql::NPureCalc::IProgramFactoryPtr programFactory) override {
        // Program should be stateless because input values
        // allocated on another allocator and should be released
        Program = programFactory->MakePullListProgram(
            NYdb::NTopic::NPurecalc::TMessageInputSpec(),
            TMessageOutputSpec(TableColumns, MakeOutputSchema(TableColumns)),
            Sql,
            NYql::NPureCalc::ETranslationMode::SQL
        );

    }

    NYql::NPureCalc::TPullListProgram<NYdb::NTopic::NPurecalc::TMessageInputSpec, TMessageOutputSpec>* GetProgram() {
        return Program.Get();
    }

private:
    //const TLightweightSchema::TCPtr Columns;
    const TVector<TSchemaColumn> TopicColumns;
    const TVector<TSchemaColumn> TableColumns;
    const TString Sql;

    THolder<NYql::NPureCalc::TPullListProgram<NYdb::NTopic::NPurecalc::TMessageInputSpec, TMessageOutputSpec>> Program;
};


} // anonymous namespace


class TTransferWriter
    : public TActorBootstrapped<TTransferWriter>
    , private NSchemeCache::TSchemeCacheHelpers
{
public:
    void Bootstrap() {
        GetTableScheme();
    }

private:
    void GetTableScheme() {
        LOG_D("GetTableScheme: worker# " << Worker);
        Become(&TThis::StateGetTableScheme);

        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(TablePathId, TNavigate::OpTable));
        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
    }

    STFUNC(StateGetTableScheme) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);

            hFunc(TEvWorker::TEvHandshake, HoldHandle);
            hFunc(TEvWorker::TEvData, HoldHandle);
            sFunc(TEvents::TEvWakeup, SendS3Request);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    void LogCritAndLeave(const TString& error) {
        LOG_C(error);
        Leave(TEvWorker::TEvGone::SCHEME_ERROR, error);
    }

    void LogWarnAndRetry(const TString& error) {
        LOG_W(error);
        Retry();
    }

    template <typename CheckFunc, typename FailFunc, typename T, typename... Args>
    bool Check(CheckFunc checkFunc, FailFunc failFunc, const T& subject, Args&&... args) {
        return checkFunc("writer", subject, std::forward<Args>(args)..., std::bind(failFunc, this, std::placeholders::_1));
    }

    template <typename T>
    bool CheckNotEmpty(const TAutoPtr<T>& result) {
        return Check(&TSchemeCacheHelpers::CheckNotEmpty<T>, &TThis::LogCritAndLeave, result);
    }

    template <typename T>
    bool CheckEntriesCount(const TAutoPtr<T>& result, ui32 expected) {
        return Check(&TSchemeCacheHelpers::CheckEntriesCount<T>, &TThis::LogCritAndLeave, result, expected);
    }

    template <typename T>
    bool CheckTableId(const T& entry, const TTableId& expected) {
        return Check(&TSchemeCacheHelpers::CheckTableId<T>, &TThis::LogCritAndLeave, entry, expected);
    }

    template <typename T>
    bool CheckEntrySucceeded(const T& entry) {
        return Check(&TSchemeCacheHelpers::CheckEntrySucceeded<T>, &TThis::LogWarnAndRetry, entry);
    }

    template <typename T>
    bool CheckEntryKind(const T& entry, TNavigate::EKind expected) {
        return Check(&TSchemeCacheHelpers::CheckEntryKind<T>, &TThis::LogCritAndLeave, entry, expected);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, TablePathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        if (TableVersion && TableVersion == entry.Self->Info.GetVersion().GetGeneralVersion()) {
            Y_ABORT_UNLESS(Initialized);
            Resolving = false;
            return CompileTransferLambda();
        }

        auto schema = MakeIntrusive<TLightweightSchema>();
        if (entry.Self && entry.Self->Info.HasVersion()) {
            schema->Version = entry.Self->Info.GetVersion().GetTableSchemaVersion();
        }

        TableColumns.reserve(entry.Columns.size());
        for (const auto& [_, column] : entry.Columns) {
            TableColumns.emplace_back(column.Name, TypeName(column.PType));

            if (column.KeyOrder >= 0) {
                if (schema->KeyColumns.size() <= static_cast<ui32>(column.KeyOrder)) {
                    schema->KeyColumns.resize(column.KeyOrder + 1);
                }

                schema->KeyColumns[column.KeyOrder] = column.PType;
            } else {
                auto res = schema->ValueColumns.emplace(column.Name, TLightweightSchema::TColumn{
                    .Tag = column.Id,
                    .Type = column.PType,
                });
                Y_ABORT_UNLESS(res.second);
            }
        }

        Schema = schema;

        CompileTransferLambda();
    }

private:
    void CompileTransferLambda() {
        LOG_D("CompileTransferLambda: worker# " << Worker);

        NFq::TPurecalcCompileSettings settings = {};
        auto programHolder = MakeIntrusive<TProgramHolder>(TableColumns, GenerateSql());
        auto result = std::make_unique<NFq::TEvRowDispatcher::TEvPurecalcCompileRequest>(std::move(programHolder), settings);

        Send(CompileServiceId, result.release(), 0, ++InFlightCompilationId);
        Become(&TThis::StateCompileTransferLambda);
    }

    STFUNC(StateCompileTransferLambda) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NFq::TEvRowDispatcher::TEvPurecalcCompileResponse, Handle);

            hFunc(TEvWorker::TEvHandshake, HoldHandle);
            hFunc(TEvWorker::TEvData, HoldHandle);
            sFunc(TEvents::TEvWakeup, SendS3Request);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    TString GenerateSql() {
        TStringBuilder sb;
        sb << Config.GetTransferSpecific().GetTargets(0).GetTransformLambda();
        sb << "SELECT $__ydb_transfer_lambda(TableRow()) AS " << RESULT_COLUMN_NAME << " FROM Input;\n";
        LOG_D("SQL: " << sb);
        return sb;
    }

    void Handle(NFq::TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) {
        const auto& result = ev->Get();

        LOG_D("Handle TEvPurecalcCompileResponse"
            << ": result# " << (result ? result->Issues.ToOneLineString() : "nullptr"));

        if (ev->Cookie != InFlightCompilationId) {
            LOG_D("Outdated compiler response ignored for id " << ev->Cookie << ", current compile id " << InFlightCompilationId);
            return;
        }

        if (!result->ProgramHolder) {
            return LogCritAndLeave(TStringBuilder() << "Compilation failed: " << result->Issues.ToOneLineString());
        }

        auto r = static_cast<TProgramHolder*>(ev->Get()->ProgramHolder.Release());
        Y_ENSURE(result, "Unexpected compile response");

        ProgramHolder = TIntrusivePtr<TProgramHolder>(r);

        StartWork();
    }

private:
    void StartWork() {
        Become(&TThis::StateWork);

        if (HandshakeEv) {
            Handle(HandshakeEv);
            HandshakeEv.Reset();
        }

        if (DataEv) {
            Handle(DataEv);
            DataEv.Reset();
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            sFunc(TEvents::TEvWakeup, SendS3Request);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    void HoldHandle(TEvWorker::TEvHandshake::TPtr& ev) {
        Y_ABORT_UNLESS(!HandshakeEv);
        HandshakeEv = ev;
    }

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        Worker = ev->Sender;
        LOG_D("Handshake"
            << ": worker# " << Worker);

        Send(Worker, new TEvWorker::TEvHandshake());

        //S3Client = RegisterWithSameMailbox(NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));
    }

    void HoldHandle(TEvWorker::TEvData::TPtr& ev) {
        Y_ABORT_UNLESS(!DataEv);
        DataEv = ev;
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        LOG_D("Handle TEvData " << ev->Get()->ToString());

        if (!ev->Get()->Records) {
            Finished = true;
            //WriteIdentity();
            return;
        }

        for (auto& message : ev->Get()->Records) {
            NYdb::NTopic::NPurecalc::TMessage input(message.Data);
            input.WithOffset(message.Offset);

            auto result = ProgramHolder->GetProgram()->Apply(NYql::NPureCalc::StreamFromVector(TVector{input}));
            while (auto* m = result->Fetch()) {
                Y_UNUSED(m);

                //Cout << "url = " << message->GetUrl() << Endl;
                //Cout << "hits = " << message->GetHits() << Endl;
            }
        }

        // TODO Send to table
    }


    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[TransferWriter]"
                << TableName
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        LOG_E("Error at '" << marker << "'"
            << ", error# " << result);
        RetryOrLeave(result.GetError());

        return false;
    }

    //bool CanRetry(const Aws::S3::S3Error& error) const {
    //    return Attempt < Retries && ShouldRetry(error);
    //}

    void Retry() {
        Delay = Min(Delay * ++Attempt, MaxDelay);
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        this->Schedule(Delay + random, new TEvents::TEvWakeup());
    }

    //void RetryOrLeave(const Aws::S3::S3Error& error) {
    //    if (CanRetry(error)) {
    //        Retry();
    //    } else {
    //        Leave(TStringBuilder() << "S3 error: " << error.GetMessage().c_str());
    //    }
    //}

    template <typename... Args>
    void Leave(Args&&... args) {
        LOG_I("Leave");

        Send(Worker, new TEvWorker::TEvGone(std::forward<Args>(args)...));
        PassAway();
    }

    void SendS3Request() {
        //Y_VERIFY(RequestInFlight);
        //Send(S3Client, new TEvExternalStorage::TEvPutObjectRequest(RequestInFlight->Request, TString(RequestInFlight->Buffer)));
    }


    void PassAway() override {
        TActor::PassAway();
    }


/*
    void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("Handle " << ev->Get()->ToString());

        if (!CheckResult(result, TStringBuf("PutObject"))) {
            return;
        } else {
            RequestInFlight = nullptr;
        }

        if (!IdentityWritten) {
            IdentityWritten = true;
            Send(Worker, new TEvWorker::TEvHandshake());
        } else if (!Finished) {
            Send(Worker, new TEvWorker::TEvPoll());
        } else {
            Send(Worker, new TEvWorker::TEvGone(TEvWorker::TEvGone::DONE));
        }
    }
*/
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_S3_WRITER;
    }

    explicit TTransferWriter(
        const NKikimrReplication::TReplicationConfig& config,
        const TPathId& tablePathId,
        const TString& tableName,
        const TString& writerName,
        const TActorId& compileServiceId)
        : Config(config)
        , TablePathId(tablePathId)
        , CompileServiceId(compileServiceId)
        , TableName(tableName)
        , WriterName(writerName)
    {}

private:
    const NKikimrReplication::TReplicationConfig Config;
    const TPathId TablePathId;

    const TActorId CompileServiceId;
    size_t InFlightCompilationId = 0;

    ui64 TableVersion = 0;
    THolder<TKeyDesc> KeyDesc;
    TLightweightSchema::TCPtr Schema;
    TVector<TSchemaColumn> TableColumns;
    bool Resolving = false;
    bool Initialized = false;

    TProgramHolder::TPtr ProgramHolder;


    mutable TMaybe<TString> LogPrefix;
    const TString TableName;
    const TString WriterName;
    TActorId Worker;
    TActorId S3Client;
    //bool IdentityWritten = false;
    bool Finished = false;

    //const ui32 Retries = 3;
    ui32 Attempt = 0;

    TEvWorker::TEvHandshake::TPtr HandshakeEv;
    TEvWorker::TEvData::TPtr DataEv;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);
}; // TS3Writer

IActor* CreateTransferWriter(const NKikimrReplication::TReplicationConfig& config,
    const TPathId& tablePathId, const TString& tableName, const TString& writerName,
    const TActorId& compileServiceId) {
    return new TTransferWriter(config, tablePathId, tableName, writerName, compileServiceId);
}

}

