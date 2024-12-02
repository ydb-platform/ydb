#include <ydb/library/yql/providers/common/schema/parser/yql_type_parser.h>
#include <ydb/library/yql/public/udf/udf_version.h>
#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/mkql/spec.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/row_dispatcher/json_filter.h>

namespace {

using TCallback = NFq::TJsonFilter::TCallback;
const char* OffsetFieldName = "_offset";
TString LogPrefix = "JsonFilter: ";

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
            .Add(CreateTypeNode(fieldType))
    );
}

void AddTypedField(NYT::TNode& node, const TString& fieldName, const TString& fieldTypeYson) {
    NYT::TNode parsedType;
    Y_ENSURE(NYql::NCommon::ParseYson(parsedType, fieldTypeYson, Cerr), "Invalid field type");

    // TODO: remove this when the re-parsing is removed from pq read actor
    if (parsedType == CreateTypeNode("Json")) {
        parsedType = CreateTypeNode("String");
    } else if (parsedType == CreateOptionalTypeNode("Json")) {
        parsedType = CreateOptionalTypeNode("String");
    }

    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(parsedType)
    );
}

NYT::TNode MakeInputSchema(const TVector<TString>& columns, const TVector<TString>& types) {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, OffsetFieldName, "Uint64");
    for (size_t i = 0; i < columns.size(); ++i) {
        AddTypedField(structMembers, columns[i], types[i]);
    }
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

NYT::TNode MakeOutputSchema() {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, OffsetFieldName, "Uint64");
    AddField(structMembers, "data", "String");
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

struct TInputType {
    const TVector<ui64>& Offsets;
    const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& Values;
    const ui64 RowsOffset;  // offset of first value
    const ui64 NumberRows;

    ui64 GetOffset(ui64 rowId) const {
        return Offsets[rowId + RowsOffset];
    }
};

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

class TFilterInputConsumer : public NYql::NPureCalc::IConsumer<TInputType> {
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

    void OnObject(TInputType input) override {
        Y_ENSURE(FieldsPositions.size() == input.Values.size());

        NKikimr::NMiniKQL::TThrowingBindTerminator bind;
        with_lock (Worker->GetScopedAlloc()) {
            Y_DEFER {
                // Clear cache after each object because
                // values allocated on another allocator and should be released
                Cache.Clear();
                Worker->GetGraph().Invalidate();
            };

            auto& holderFactory = Worker->GetGraph().GetHolderFactory();

            // TODO: use blocks here
            for (size_t rowId = 0; rowId < input.NumberRows; ++rowId) {
                NYql::NUdf::TUnboxedValue* items = nullptr;

                NYql::NUdf::TUnboxedValue result = Cache.NewArray(
                    holderFactory,
                    static_cast<ui32>(input.Values.size() + 1),
                    items);

                items[OffsetPosition] = NYql::NUdf::TUnboxedValuePod(input.GetOffset(rowId));

                size_t fieldId = 0;
                for (const auto column : input.Values) {
                    items[FieldsPositions[fieldId++]] = column->at(rowId);
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

    using TConsumerType = THolder<NYql::NPureCalc::IConsumer<TInputType>>;

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

class TProgramHolder : public IProgramHolder {
public:
    using TPtr = TIntrusivePtr<TProgramHolder>;

public:
    TProgramHolder(const TVector<TString>& columns, const TVector<TString>& types, const TString& sql, TCallback callback)
        : Columns(columns)
        , Types(types)
        , Sql(sql)
        , Callback(callback)
    {
        Y_ENSURE(columns.size() == types.size(), "Number of columns and types should by equal");
    }

    NYql::NPureCalc::IConsumer<TInputType>& GetConsumer() {
        Y_ENSURE(InputConsumer, "Program is not compiled");
        return *InputConsumer;
    }

public:
    void CreateProgram(NYql::NPureCalc::IProgramFactoryPtr programFactory) override {
        Program = programFactory->MakePushStreamProgram(
            TFilterInputSpec(MakeInputSchema(Columns, Types)),
            TFilterOutputSpec(MakeOutputSchema()),
            Sql,
            NYql::NPureCalc::ETranslationMode::SQL
        );
        InputConsumer = Program->Apply(MakeHolder<TFilterOutputConsumer>(Callback));
    }

private:
    const TVector<TString> Columns;
    const TVector<TString> Types;
    const TString Sql;
    const TCallback Callback;

    THolder<NYql::NPureCalc::TPushStreamProgram<TFilterInputSpec, TFilterOutputSpec>> Program;
    THolder<NYql::NPureCalc::IConsumer<TInputType>> InputConsumer;
};

class TJsonFilter::TImpl {
public:
    TImpl(const TVector<TString>& columns, const TVector<TString>& types, const TString& whereFilter, TCallback callback, const TPurecalcCompileSettings& purecalcSettings)
        : PurecalcSettings(purecalcSettings)
        , Sql(GenerateSql(whereFilter))
        , ProgramHolder(MakeIntrusive<TProgramHolder>(columns, types, Sql, callback))
    {}

    void Push(const TVector<ui64>& offsets, const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 rowsOffset, ui64 numberRows) {
        Y_ENSURE(ProgramHolder, "Program is not compiled");
        Y_ENSURE(values, "Expected non empty schema");
        ProgramHolder->GetConsumer().OnObject({.Offsets = offsets, .Values = values, .RowsOffset = rowsOffset, .NumberRows = numberRows});
    }

    TString GetSql() const {
        return Sql;
    }

    std::unique_ptr<TEvRowDispatcher::TEvPurecalcCompileRequest> GetCompileRequest() {
        Y_ENSURE(ProgramHolder, "Can not create compile request twice");
        auto result = std::make_unique<TEvRowDispatcher::TEvPurecalcCompileRequest>(std::move(ProgramHolder), PurecalcSettings);
        ProgramHolder = nullptr;
        return result;
    }

    void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr ev) {
        Y_ENSURE(!ProgramHolder, "Can not handle compile response twice");

        auto result = static_cast<TProgramHolder*>(ev->Get()->ProgramHolder.Release());
        Y_ENSURE(result, "Unexpected compile response");

        ProgramHolder = TIntrusivePtr<TProgramHolder>(result);
    }

private:
    TString GenerateSql(const TString& whereFilter) {
        TStringStream str;
        str << "PRAGMA config.flags(\"LLVM\", \"" << (PurecalcSettings.EnabledLLVM ? "ON" : "OFF") << "\");\n";
        str << "$filtered = SELECT * FROM Input " << whereFilter << ";\n";

        str << "SELECT " << OffsetFieldName <<  ", Unwrap(Json::SerializeJson(Yson::From(RemoveMembers(TableRow(), [\"" << OffsetFieldName;
        str << "\"])))) as data FROM $filtered";
        LOG_ROW_DISPATCHER_DEBUG("Generated sql: " << str.Str());
        return str.Str();
    }

private:
    const TPurecalcCompileSettings PurecalcSettings;
    const TString Sql;
    TProgramHolder::TPtr ProgramHolder;
};

TJsonFilter::TJsonFilter(const TVector<TString>& columns, const TVector<TString>& types, const TString& whereFilter, TCallback callback, const TPurecalcCompileSettings& purecalcSettings)
    : Impl(std::make_unique<TJsonFilter::TImpl>(columns, types, whereFilter, callback, purecalcSettings))
{}

TJsonFilter::~TJsonFilter() {
}

void TJsonFilter::Push(const TVector<ui64>& offsets, const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 rowsOffset, ui64 numberRows) {
    Impl->Push(offsets, values, rowsOffset, numberRows);
}

TString TJsonFilter::GetSql() {
    return Impl->GetSql();
}

std::unique_ptr<TEvRowDispatcher::TEvPurecalcCompileRequest> TJsonFilter::GetCompileRequest() {
    return Impl->GetCompileRequest();
}

void TJsonFilter::OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr ev) {
    Impl->OnCompileResponse(std::move(ev));
}

std::unique_ptr<TJsonFilter> NewJsonFilter(const TVector<TString>& columns, const TVector<TString>& types, const TString& whereFilter, TCallback callback, const TPurecalcCompileSettings& purecalcSettings) {
    return std::unique_ptr<TJsonFilter>(new TJsonFilter(columns, types, whereFilter, callback, purecalcSettings));
}

} // namespace NFq
