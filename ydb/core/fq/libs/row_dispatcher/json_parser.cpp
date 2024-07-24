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
#include <ydb/core/fq/libs/row_dispatcher/json_parser.h>


namespace NFq {


using TCallback = TJsonParser::TCallback;
using TInputConsumerArg = std::pair<ui64, TString>;

static void AddField(NYT::TNode& node, const TString& fieldName, const TString& fieldType) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(NYT::TNode::CreateList().Add("DataType").Add(fieldType))
    );
}

static NYT::TNode MakeInputSchema() {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, "offset", "Uint64");
    AddField(structMembers, "data", "String");
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

static NYT::TNode MakeOutputSchema(const TVector<TString>& columns) {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, "offset", "Uint64");
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
        NMiniKQL::TThrowingBindTerminator bind;
        LOG_ROW_DISPATCHER_DEBUG("TParserInputConsumer::OnObject: " << value.first);
        LOG_ROW_DISPATCHER_DEBUG("TParserInputConsumer::OnObject: " << value.second);

        std::cerr << "json " << value.second << std::endl;

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
            LOG_ROW_DISPATCHER_DEBUG("Push ");
            try {
                Worker->Push(std::move(result));
            } catch (...) {
                std::cerr << "catch " << std::endl;
            }
            LOG_ROW_DISPATCHER_DEBUG("Push end");
        }
    }

    void OnFinish() override {
        std::cerr << "OnFinish " << std::endl;
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


class TParserOutputConsumer: public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    TParserOutputConsumer(TCallback callback)
        : Callback(callback) {
    }

    void OnObject(const NYql::NUdf::TUnboxedValue* value) override {
        // TString result;
        // for (const auto& field : parsedRecord->AsMap()) {
        //     result += field.second.AsString();
        // }


        TList<TString> result;
        
        ui64 offset = value->GetElement(0).Get<ui64>();
        

        for (size_t i = 1; i != value->GetListLength(); ++i) {
            const auto& cell = value->GetElement(i);

            NYql::NUdf::TStringRef strRef(cell.AsStringRef());
            result.emplace_back(strRef.Data(), strRef.Size());
        }
        
        Callback(offset, std::move(result));
    }

    void OnFinish() override {
        std::cerr << "TParserOutputConsumer::OnFinish " << std::endl;


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

// struct TFieldsMapping{
//     THashMap<TString, size_t> FieldsPositions;

//     TFieldsMapping(const NYT::TNode& schema) {
//         const auto& fields = schema[1];
//         assert(fields.IsList());

//         for (size_t i = 0; i < fields.Size(); ++i) {
//             auto name = fields[i][0].AsString();
//             FieldsPositions[std::move(name)] = i;
//         }
//     }

//     size_t GetPosition(const TString& fieldName) const {
//         return FieldsPositions.at(fieldName);
//     }

//     bool Has(const TString& fieldName) const {
//         return FieldsPositions.contains(fieldName);
//     }
// };

class TParserPushRelayImpl: public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    TParserPushRelayImpl(const TParserOutputSpec& /*outputSpec*/, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*>> underlying)
        : Underlying(std::move(underlying))
        , Worker(worker)
       // , FieldsMapping(outputSpec.GetSchema())
    {}

// void FillOutputNode(const NYql::NUdf::TUnboxedValue* source, NYT::TNode& destination, const TFieldsMapping& fieldsMapping) {
//     for (const auto& field : fieldsMapping.FieldsPositions) {
//         const auto& cell = source->GetElement(field.second);
//         if (!cell) {
//             continue;
//         }
//         destination[field.first] = TString(cell.AsStringRef());
//     }
// }

public:
    void OnObject(const NYql::NUdf::TUnboxedValue* value) override {
        NYT::TNode result = NYT::TNode::CreateMap();
        //FillOutputNode(value, result, FieldsMapping);
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnObject(value);
    }

    void OnFinish() override {
        std::cerr << "TParserPushRelayImpl::OnFinish " << std::endl;

        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnFinish();
    }

private:
    THolder<NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*>> Underlying;
    [[maybe_unused]]    NYql::NPureCalc::IWorker* Worker;
  //  TFieldsMapping FieldsMapping;
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

    static void SetConsumerToWorker(const NFq::TParserOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*>> consumer) {
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
        : LogPrefix("JsonParser: ") {
        auto options = NYql::NPureCalc::TProgramFactoryOptions();
        if (!udfDir.empty()) {
            options.SetUDFsDir(udfDir);
        }
        auto factory = NYql::NPureCalc::MakeProgramFactory(options);

        try {
            LOG_ROW_DISPATCHER_DEBUG("Creating program...");
            Program = factory->MakePushStreamProgram(
                TParserInputSpec(),
                TParserOutputSpec(MakeOutputSchema(columns)),
                GenerateSql(columns),
                NYql::NPureCalc::ETranslationMode::SQL
            );
            LOG_ROW_DISPATCHER_DEBUG("Program created");
            InputConsumer = Program->Apply(MakeHolder<TParserOutputConsumer>(callback));
            LOG_ROW_DISPATCHER_DEBUG("InputConsumer created");

        } catch (NYql::NPureCalc::TCompileError& e) {
            Cerr << e.GetIssues() << Endl; // TODO
            throw;
        }
    }

    void Push( ui64 offset, const TString& value) {
        try {
            LOG_ROW_DISPATCHER_DEBUG("Push " << value);
            InputConsumer->OnObject(std::make_pair(offset, value));
        } catch (const yexception& ex) {
                LOG_ROW_DISPATCHER_DEBUG("Push error");
        }
    }

private:
    TString GenerateSql(const TVector<TString>& columns) {
        TStringStream str;
        str << R"($json = SELECT CAST(data AS Json) as `Json`, offset FROM Input;)"; 
        str << "\nSELECT offset, ";
        for (auto it = columns.begin(); it != columns.end(); ++it) {
            str << R"(CAST(Unwrap(JSON_VALUE(`Json`, "$.)" << *it << "\")) as String) as "
                << *it << ((it != columns.end() - 1) ? "," : "");
        }
        str << " FROM $json;";
        std::cerr << "GenerateSql" << str.Str() << std::endl;
        LOG_ROW_DISPATCHER_DEBUG("GenerateSql " << str.Str());
        return str.Str();
    }

private:
    THolder<NYql::NPureCalc::TPushStreamProgram<TParserInputSpec, TParserOutputSpec>> Program;
    THolder<NYql::NPureCalc::IConsumer<TInputConsumerArg>> InputConsumer;
    const TString LogPrefix;
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

std::unique_ptr<TJsonParser> NewJsonParser(
    const TString& udfDir,
    const TVector<TString>& columns,
    TCallback callback) {
    return std::unique_ptr<TJsonParser>(new TJsonParser(udfDir, columns, callback));
}

} // namespace NFq
