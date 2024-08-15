#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <ydb/library/yql/public/udf/udf_version.h>
#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/mkql/spec.h>
#include <library/cpp/yt/yson_string/string.h>
#include <library/cpp/yt/yson_string/convert.h>
#include <util/stream/file.h>
#include <yt/yt/core/ytree/serialize.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/row_dispatcher/json_filter.h>

namespace NFq {

using TCallback = TJsonFilter::TCallback;
static const char* OffsetFieldName = "_offset"; 

static void AddField(NYT::TNode& node, const TString& fieldName, const TString& fieldType) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(NYT::TNode::CreateList().Add("DataType").Add(fieldType))
    );
}

static NYT::TNode MakeInputSchema(const TVector<TString>& columns) {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, OffsetFieldName, "Uint64");
    for (const auto& col : columns) {
        AddField(structMembers, col, "String");
    }
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

static NYT::TNode MakeOutputSchema() {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, OffsetFieldName, "Uint64");
    AddField(structMembers, "data", "String");
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

class TFilterInputSpec : public NYql::NPureCalc::TInputSpecBase {
public:
    TFilterInputSpec(const NYT::TNode& schema)
        : Schemas({schema}) {
    }

    const TVector<NYT::TNode>& GetSchemas() const override {
        return Schemas;
    }

private:
    TVector<NYT::TNode> Schemas;
};

class TFilterInputConsumer : public NYql::NPureCalc::IConsumer<std::pair<ui64, TList<TString>>> {
public:
    explicit TFilterInputConsumer(
        const NFq::TFilterInputSpec& spec,
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker)
        : Worker(std::move(worker)) {
        LOG_ROW_DISPATCHER_DEBUG("TFilterInputConsumer::TFilterInputConsumer()");

        const NKikimr::NMiniKQL::TStructType* structType = Worker->GetInputType();
        const auto nMembers = structType->GetMembersCount();

        THashMap<TString, size_t> schemaPositions;
        for (ui32 i = 0; i < nMembers; ++i) { 
            const auto name = structType->GetMemberName(i);
            if (name == OffsetFieldName) {
                OffsetPosition = i;
                continue;
            }
            schemaPositions[name] = i;
        }

        const NYT::TNode& schema = spec.GetSchemas()[0];
        const auto& fields = schema[1];
        Y_ENSURE(nMembers == fields.Size());
        Y_ENSURE(fields.IsList());
        for (size_t i = 0; i < fields.Size(); ++i) {
            auto name = fields[i][0].AsString();
            if (name == OffsetFieldName) {
               continue;
            }
            FieldsPositions.push_back(schemaPositions[name]);
        }
    }

    ~TFilterInputConsumer() override {
        with_lock(Worker->GetScopedAlloc()) {
            Cache.Clear();
        }
    }

    void OnObject(std::pair<ui64, TList<TString>> value) override {
        NKikimr::NMiniKQL::TThrowingBindTerminator bind;
        
        with_lock (Worker->GetScopedAlloc()) {
            auto& holderFactory = Worker->GetGraph().GetHolderFactory();
            NYql::NUdf::TUnboxedValue* items = nullptr;

            NYql::NUdf::TUnboxedValue result = Cache.NewArray(
                holderFactory,
                static_cast<ui32>(value.second.size() + 1),
                items);
    
            items[OffsetPosition] = NYql::NUdf::TUnboxedValuePod(value.first);

            Y_ENSURE(FieldsPositions.size() == value.second.size());

            size_t i = 0;
            for (const auto& v : value.second) {
                NYql::NUdf::TStringValue str(v.size());
                std::memcpy(str.Data(), v.data(), v.size());
                items[FieldsPositions[i++]] = NYql::NUdf::TUnboxedValuePod(std::move(str));
            }
            Worker->Push(std::move(result));
        }
    }

    void OnFinish() override {
        NKikimr::NMiniKQL::TBindTerminator bind(Worker->GetGraph().GetTerminator());
        with_lock(Worker->GetScopedAlloc()) {
            Worker->OnFinish();
        }
    }

private:
    NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> Worker;
    NKikimr::NMiniKQL::TPlainContainerCache Cache;
    TString LogPrefix = "JsonFilter: ";
    size_t OffsetPosition = 0;
    TVector<size_t> FieldsPositions;
};

class TFilterOutputConsumer: public NYql::NPureCalc::IConsumer<std::pair<ui64, TString>> {
public:
    TFilterOutputConsumer(TCallback callback)
        : Callback(callback) {
    }

    void OnObject(std::pair<ui64, TString> value) override {
        Callback(value.first, value.second);
    }

    void OnFinish() override {
        Y_UNREACHABLE();
    }
private:
    TCallback Callback;
};

class TFilterOutputSpec: public NYql::NPureCalc::TOutputSpecBase {
public:
    explicit TFilterOutputSpec(const NYT::TNode& schema)
        : Schema(schema)
    {}

public:
    const NYT::TNode& GetSchema() const override {
        return Schema;
    }

private:
    NYT::TNode Schema;
};

class TFilterPushRelayImpl: public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    TFilterPushRelayImpl(const TFilterOutputSpec& /*outputSpec*/, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, TString>>> underlying)
        : Underlying(std::move(underlying))
        , Worker(worker)
       // , FieldsMapping(outputSpec.GetSchema())
    {}
public:
    void OnObject(const NYql::NUdf::TUnboxedValue* value) override {
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Y_ENSURE(value->GetListLength() == 2);
        ui64 offset = value->GetElement(0).Get<ui64>();
        const auto& cell = value->GetElement(1);
        Y_ENSURE(cell);
        TString str(cell.AsStringRef());
        Underlying->OnObject(std::make_pair(offset, str));
    }

    void OnFinish() override {
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnFinish();
    }

private:
    THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, TString>>> Underlying;
    [[maybe_unused]]    NYql::NPureCalc::IWorker* Worker;
};

} // namespace NFq

TString LogPrefix = "JsonFilter: ";

template <>
struct NYql::NPureCalc::TInputSpecTraits<NFq::TFilterInputSpec> {
    static constexpr bool IsPartial = false;
    static constexpr bool SupportPushStreamMode = true;

    using TConsumerType = THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, TList<TString>>>>;

    static TConsumerType MakeConsumer(
        const NFq::TFilterInputSpec& spec,
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker)
    {
        LOG_STREAMS_IMPL(DEBUG, YQ_ROW_DISPATCHER, "MakeConsumer()")
        return MakeHolder<NFq::TFilterInputConsumer>(spec, std::move(worker));
    }
};

template <>
struct NYql::NPureCalc::TOutputSpecTraits<NFq::TFilterOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = false;
    static const constexpr bool SupportPushStreamMode = true;

    static void SetConsumerToWorker(const NFq::TFilterOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, TString>>> consumer) {
        worker->SetConsumer(MakeHolder<NFq::TFilterPushRelayImpl>(outputSpec, worker, std::move(consumer)));
    }
};

namespace NFq {

class TJsonFilter::TImpl {
public:
    TImpl(const TVector<TString>& columns,
        const TVector<TString>& types,
        const TString& whereFilter,
        TCallback callback)
        : LogPrefix("JsonFilter: ")
        , Sql(GenerateSql(columns, types, whereFilter)) {
        auto factory = NYql::NPureCalc::MakeProgramFactory(NYql::NPureCalc::TProgramFactoryOptions());

        LOG_ROW_DISPATCHER_DEBUG("Creating program...");
        Program = factory->MakePushStreamProgram(
            TFilterInputSpec(MakeInputSchema(columns)),
            TFilterOutputSpec(MakeOutputSchema()),
            Sql,
            NYql::NPureCalc::ETranslationMode::SQL
        );
        LOG_ROW_DISPATCHER_DEBUG("Program created");
        InputConsumer = Program->Apply(MakeHolder<TFilterOutputConsumer>(callback));
        LOG_ROW_DISPATCHER_DEBUG("InputConsumer created");
    }

    void Push(ui64 offset, const TList<TString>& value) {
        LOG_ROW_DISPATCHER_TRACE("Push");
        InputConsumer->OnObject(std::make_pair(offset, value));
    }

    TString GetSql() {
        return Sql;
    }

private:
    TString GenerateSql(const TVector<TString>& columnNames, const TVector<TString>& columnTypes, const TString& whereFilter) {
        TStringStream str;
        str << "$fields = SELECT ";
        Y_ABORT_UNLESS(columnNames.size() == columnTypes.size());
        str << OffsetFieldName << ", ";
        for (size_t i = 0; i < columnNames.size(); ++i) {
            str << "CAST(" << columnNames[i] << " as " << columnTypes[i] << ") as " << columnNames[i] << ((i != columnNames.size() - 1) ? "," : "");
        }
        str << " FROM Input;\n";
        str << "$filtered = SELECT * FROM $fields " << whereFilter << ";\n";

        str << "SELECT " << OffsetFieldName <<  ", Unwrap(Json::SerializeJson(Yson::From(RemoveMembers(TableRow(), [\"" << OffsetFieldName;
        str << "\"])))) as data FROM $filtered";
        LOG_ROW_DISPATCHER_DEBUG("Generated sql: " << str.Str());
        return str.Str();
    }

private:
    THolder<NYql::NPureCalc::TPushStreamProgram<TFilterInputSpec, TFilterOutputSpec>> Program;
    THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, TList<TString>>>> InputConsumer;
    const TString LogPrefix;
    const TString Sql;
};

TJsonFilter::TJsonFilter(
    const TVector<TString>& columns,
    const TVector<TString>& types,
    const TString& whereFilter,
    TCallback callback)
    : Impl(std::make_unique<TJsonFilter::TImpl>(columns, types, whereFilter, callback)) { 
}

TJsonFilter::~TJsonFilter() {
}
    
void TJsonFilter::Push(ui64 offset, const TList<TString>& value) {
     Impl->Push(offset, value);
}

TString TJsonFilter::GetSql() {
    return Impl->GetSql();
}

std::unique_ptr<TJsonFilter> NewJsonFilter(
    const TVector<TString>& columns,
    const TVector<TString>& types,
    const TString& whereFilter,
    TCallback callback) {
    return std::unique_ptr<TJsonFilter>(new TJsonFilter(columns, types, whereFilter, callback));
}

} // namespace NFq
