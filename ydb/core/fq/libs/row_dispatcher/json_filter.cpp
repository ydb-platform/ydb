#include <ydb/library/yql/public/udf/udf_version.h>
#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/mkql/spec.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/core/fq/libs/row_dispatcher/json_filter.h>
#include <ydb/core/fq/libs/actors/logging/log.h>


namespace {

using TCallback = NFq::TJsonFilter::TCallback;
const char* OffsetFieldName = "_offset";
TString LogPrefix = "JsonFilter: ";

void AddField(NYT::TNode& node, const TString& fieldName, const TString& fieldType) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(NYT::TNode::CreateList().Add("DataType").Add(fieldType))
    );
}

NYT::TNode MakeInputSchema(const TVector<TString>& columns) {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, OffsetFieldName, "Uint64");
    for (const auto& col : columns) {
        AddField(structMembers, col, "String");
    }
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

NYT::TNode MakeOutputSchema() {
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

class TFilterInputConsumer : public NYql::NPureCalc::IConsumer<std::pair<ui64, const TVector<TVector<std::string_view>>&>> {
public:
    TFilterInputConsumer(
        const TFilterInputSpec& spec,
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker)
        : Worker(std::move(worker)) {
        const NKikimr::NMiniKQL::TStructType* structType = Worker->GetInputType();
        const auto count = structType->GetMembersCount();

        THashMap<TString, size_t> schemaPositions;
        for (ui32 i = 0; i < count; ++i) { 
            const auto name = structType->GetMemberName(i);
            if (name == OffsetFieldName) {
                OffsetPosition = i;
                continue;
            }
            schemaPositions[name] = i;
        }

        const NYT::TNode& schema = spec.GetSchemas()[0];
        const auto& fields = schema[1];
        Y_ENSURE(count == fields.Size());
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

    void OnObject(std::pair<ui64, const TVector<TVector<std::string_view>>&> values) override {
        Y_ENSURE(FieldsPositions.size() == values.second.size());

        NKikimr::NMiniKQL::TThrowingBindTerminator bind;
        with_lock (Worker->GetScopedAlloc()) {
            auto& holderFactory = Worker->GetGraph().GetHolderFactory();

            // TODO: use blocks here
            for (size_t rowId = 0; rowId < values.second.front().size(); ++rowId) {
                NYql::NUdf::TUnboxedValue* items = nullptr;

                NYql::NUdf::TUnboxedValue result = Cache.NewArray(
                    holderFactory,
                    static_cast<ui32>(values.second.size() + 1),
                    items);

                items[OffsetPosition] = NYql::NUdf::TUnboxedValuePod(values.first++);

                size_t fieldId = 0;
                for (const auto& column : values.second) {
                    items[FieldsPositions[fieldId++]] = NKikimr::NMiniKQL::MakeString(column[rowId]);
                }

                Worker->Push(std::move(result));
            }
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
    NYql::NPureCalc::IWorker* Worker;
};

}

template <>
struct NYql::NPureCalc::TInputSpecTraits<TFilterInputSpec> {
    static constexpr bool IsPartial = false;
    static constexpr bool SupportPushStreamMode = true;

    using TConsumerType = THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, const TVector<TVector<std::string_view>>&>>>;

    static TConsumerType MakeConsumer(
        const TFilterInputSpec& spec,
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker)
    {
        return MakeHolder<TFilterInputConsumer>(spec, std::move(worker));
    }
};

template <>
struct NYql::NPureCalc::TOutputSpecTraits<TFilterOutputSpec> {
    static const constexpr bool IsPartial = false;
    static const constexpr bool SupportPushStreamMode = true;

    static void SetConsumerToWorker(const TFilterOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, TString>>> consumer) {
        worker->SetConsumer(MakeHolder<TFilterPushRelayImpl>(outputSpec, worker, std::move(consumer)));
    }
};

namespace NFq {

class TJsonFilter::TImpl {
public:
    TImpl(const TVector<TString>& columns,
        const TVector<TString>& types,
        const TString& whereFilter,
        TCallback callback)
        : Sql(GenerateSql(columns, types, whereFilter)) {
        auto factory = NYql::NPureCalc::MakeProgramFactory(NYql::NPureCalc::TProgramFactoryOptions());

        LOG_ROW_DISPATCHER_DEBUG("Creating program...");
        Program = factory->MakePushStreamProgram(
            TFilterInputSpec(MakeInputSchema(columns)),
            TFilterOutputSpec(MakeOutputSchema()),
            Sql,
            NYql::NPureCalc::ETranslationMode::SQL
        );
        InputConsumer = Program->Apply(MakeHolder<TFilterOutputConsumer>(callback));
        LOG_ROW_DISPATCHER_DEBUG("Program created");
    }

    void Push(ui64 offset, const TVector<TVector<std::string_view>>& values) {
        Y_ENSURE(values, "Expected non empty schema");
        InputConsumer->OnObject(std::make_pair(offset, values));
    }

    TString GetSql() const {
        return Sql;
    }

private:
    TString GenerateSql(const TVector<TString>& columnNames, const TVector<TString>& columnTypes, const TString& whereFilter) {
        TStringStream str;
        str << "$fields = SELECT ";
        Y_ABORT_UNLESS(columnNames.size() == columnTypes.size());
        str << OffsetFieldName << ", ";
        for (size_t i = 0; i < columnNames.size(); ++i) {
            TString columnType = columnTypes[i];
            if (columnType == "Json") {
                columnType = "String";
            } else if (columnType == "Optional<Json>") {
                columnType = "Optional<String>";
            }
            str << "CAST(" << columnNames[i] << " as " << columnType << ") as " << columnNames[i] << ((i != columnNames.size() - 1) ? "," : "");
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
    THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, const TVector<TVector<std::string_view>>&>>> InputConsumer;
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

void TJsonFilter::Push(ui64 offset, const TVector<TVector<std::string_view>>& values) {
    Impl->Push(offset, values);
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
