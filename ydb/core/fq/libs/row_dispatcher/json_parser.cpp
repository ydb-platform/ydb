#include <ydb/core/fq/libs/row_dispatcher/json_parser.h>

#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/mkql/spec.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>
#include <ydb/core/fq/libs/actors/logging/log.h>


namespace NFq {

using TCallback = TJsonParser::TCallback;
using TInputConsumerArg = std::pair<ui64, TString>;
static const char* OffsetFieldName = "_offset"; 

static void AddField(NYT::TNode& node, const TString& fieldName, const TString& fieldType) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(NYT::TNode::CreateList().Add("DataType").Add(fieldType))
    );
}

static NYT::TNode MakeInputSchema() {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, OffsetFieldName, "Uint64");
    AddField(structMembers, "data", "String");
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

static NYT::TNode MakeOutputSchema(const TVector<TString>& columns) {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, OffsetFieldName, "Uint64");
    for (const auto& col : columns) {
        AddField(structMembers, col, "String");
    }
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

class TParserInputConsumer : public NYql::NPureCalc::IConsumer<TInputConsumerArg> {
public:
    explicit TParserInputConsumer(NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker)
        : Worker(std::move(worker)) {
    }

    ~TParserInputConsumer() override {
        with_lock(Worker->GetScopedAlloc()) {
            Cache.Clear();
        }
    }

    void OnObject(std::pair<ui64, TString> value) override {
        NKikimr::NMiniKQL::TThrowingBindTerminator bind;

        with_lock (Worker->GetScopedAlloc()) {
            auto& holderFactory = Worker->GetGraph().GetHolderFactory();
            NYql::NUdf::TUnboxedValue* items = nullptr;

            NYql::NUdf::TUnboxedValue result = Cache.NewArray(
                holderFactory,
                static_cast<ui32>(2),
                items);
    
            items[0] = NYql::NUdf::TUnboxedValuePod(value.first);
            NYql::NUdf::TStringValue str(value.second.Size());
            std::memcpy(str.Data(), value.second.Data(), value.second.Size());
            items[1] = NYql::NUdf::TUnboxedValuePod(std::move(str));
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
    TString LogPrefix = "JsonParser: ";
};


class TParserInputSpec : public NYql::NPureCalc::TInputSpecBase {
public:
    TParserInputSpec() {
        Schemas = {MakeInputSchema()};
    }

    const TVector<NYT::TNode>& GetSchemas() const override {
        return Schemas;
    }

private:
    TVector<NYT::TNode> Schemas;
};


class TParserOutputConsumer: public NYql::NPureCalc::IConsumer<std::pair<ui64, TList<TString>>> {
public:
    TParserOutputConsumer(TCallback callback)
        : Callback(callback) {
    }

    void OnObject(std::pair<ui64, TList<TString>> value) override {
        Callback(value.first, std::move(value.second));
    }

    void OnFinish() override {
        Y_UNREACHABLE();
    }
private:
    TCallback Callback;
};

class TParserOutputSpec: public NYql::NPureCalc::TOutputSpecBase {
public:
    explicit TParserOutputSpec(const NYT::TNode& schema)
        : Schema(schema)
    {}

public:
    const NYT::TNode& GetSchema() const override {
        return Schema;
    }

private:
    NYT::TNode Schema;
};

struct TFieldsMapping{
    TVector<size_t> FieldsPositions;
    size_t OffsetPosition;

    TFieldsMapping(const NYT::TNode& schema, const NKikimr::NMiniKQL::TType* outputType) {
        THashMap<TString, size_t> outputPositions;
        Y_ENSURE(outputType->IsStruct());
        const auto structType = static_cast<const NKikimr::NMiniKQL::TStructType*>(outputType);
        const auto nMembers = structType->GetMembersCount();

        for (ui32 i = 1; i < nMembers; ++i) {  // 0 index - OffsetFieldName
            const auto name = structType->GetMemberName(i);
            outputPositions[name] = i;
        }

        const auto& fields = schema[1];
        Y_ENSURE(fields.IsList());
        Y_ENSURE(nMembers == fields.Size());
        for (size_t i = 0; i < fields.Size(); ++i) {
            auto name = fields[i][0].AsString();
            if (name == OffsetFieldName) {
                OffsetPosition = i;
                continue;
            }
            FieldsPositions.push_back(outputPositions[name]);
        }
    }
};

class TParserPushRelayImpl: public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    TParserPushRelayImpl(const TParserOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, TList<TString>>>> underlying)
        : Underlying(std::move(underlying))
        , Worker(worker)
        , FieldsMapping(outputSpec.GetSchema(), Worker->GetOutputType())
    { }

public:
    void OnObject(const NYql::NUdf::TUnboxedValue* value) override {
        auto unguard = Unguard(Worker->GetScopedAlloc());
        TList<TString> result;
        
        Y_ENSURE(value->GetListLength() == FieldsMapping.FieldsPositions.size() + 1);
        ui64 offset = value->GetElement(FieldsMapping.OffsetPosition).Get<ui64>();

        for (auto pos : FieldsMapping.FieldsPositions) {
            const auto& cell = value->GetElement(pos);

            NYql::NUdf::TStringRef strRef(cell.AsStringRef());
            result.emplace_back(strRef.Data(), strRef.Size());
        }
        
        Underlying->OnObject(std::make_pair(offset, std::move(result)));
    }

    void OnFinish() override {
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnFinish();
    }

private:
    THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, TList<TString>>>> Underlying;
    [[maybe_unused]] NYql::NPureCalc::IWorker* Worker;
    TFieldsMapping FieldsMapping;
};

} // namespace NFq

template <>
struct NYql::NPureCalc::TInputSpecTraits<NFq::TParserInputSpec> {
    static constexpr bool IsPartial = false;
    static constexpr bool SupportPushStreamMode = true;

    using TConsumerType = THolder<NYql::NPureCalc::IConsumer<NFq::TInputConsumerArg>>;

    static TConsumerType MakeConsumer(
        const NFq::TParserInputSpec& spec,
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker
    ) {
        Y_UNUSED(spec);
        return MakeHolder<NFq::TParserInputConsumer>(std::move(worker));
    }
};

template <>
struct NYql::NPureCalc::TOutputSpecTraits<NFq::TParserOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = false;
    static const constexpr bool SupportPushStreamMode = true;

    static void SetConsumerToWorker(const NFq::TParserOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<std::pair<ui64, TList<TString>>>> consumer) {
        worker->SetConsumer(MakeHolder<NFq::TParserPushRelayImpl>(outputSpec, worker, std::move(consumer)));
    }
};

namespace NFq {

class TJsonParser::TImpl {
public:
    TImpl(
        const TString& udfDir,
        const TVector<TString>& columns,
        TCallback callback)
        : LogPrefix("JsonParser: ")
        , Sql(GenerateSql(columns)) {
        auto options = NYql::NPureCalc::TProgramFactoryOptions();
        if (!udfDir.empty()) {
            options.SetUDFsDir(udfDir);
        }
        auto factory = NYql::NPureCalc::MakeProgramFactory(options);

        LOG_ROW_DISPATCHER_DEBUG("Creating program...");
        Program = factory->MakePushStreamProgram(
            TParserInputSpec(),
            TParserOutputSpec(MakeOutputSchema(columns)),
            Sql,
            NYql::NPureCalc::ETranslationMode::SQL
        );
        LOG_ROW_DISPATCHER_DEBUG("Program created");
        InputConsumer = Program->Apply(MakeHolder<TParserOutputConsumer>(callback));
        LOG_ROW_DISPATCHER_DEBUG("InputConsumer created");
    }

    void Push( ui64 offset, const TString& value) {
        LOG_ROW_DISPATCHER_TRACE("Push " << value);
        InputConsumer->OnObject(std::make_pair(offset, value));
    }

    TString GetSql() {
        return Sql;
    }

private:
    TString GenerateSql(const TVector<TString>& columns) {
        TStringStream str;
        str << R"($json = SELECT CAST(data AS Json) as `Json`, )" << OffsetFieldName << " FROM Input;"; 
        str << "\nSELECT " << OffsetFieldName << ", ";
        for (auto it = columns.begin(); it != columns.end(); ++it) {
            str << R"(CAST(Unwrap(JSON_VALUE(`Json`, "$.)" << *it << "\")) as String) as "
                << *it << ((it != columns.end() - 1) ? "," : "");
        }
        str << " FROM $json;";
        LOG_ROW_DISPATCHER_DEBUG("GenerateSql " << str.Str());
        return str.Str();
    }

private:
    THolder<NYql::NPureCalc::TPushStreamProgram<TParserInputSpec, TParserOutputSpec>> Program;
    THolder<NYql::NPureCalc::IConsumer<TInputConsumerArg>> InputConsumer;
    const TString LogPrefix;
    const TString Sql;
};

TJsonParser::TJsonParser(
    const TString& udfDir,
    const TVector<TString>& columns,
    TCallback callback)
    : Impl(std::make_unique<TJsonParser::TImpl>(udfDir, columns, callback)) { 
}

TJsonParser::~TJsonParser() {
}
    
void TJsonParser::Push(ui64 offset, const TString& value) {
     Impl->Push(offset, value);
}

TString TJsonParser::GetSql() {
    return Impl->GetSql();
}

std::unique_ptr<TJsonParser> NewJsonParser(
    const TString& udfDir,
    const TVector<TString>& columns,
    TCallback callback) {
    return std::unique_ptr<TJsonParser>(new TJsonParser(udfDir, columns, callback));
}

} // namespace NFq
