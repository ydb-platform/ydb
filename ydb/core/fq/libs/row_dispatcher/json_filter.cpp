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

static void AddField(NYT::TNode& node, const TString& field) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(field)
            .Add(NYT::TNode::CreateList().Add("DataType").Add("String"))
    );
}

static NYT::TNode MakeSchema(const TVector<TString>& columns) {
    auto structMembers = NYT::TNode::CreateList();
    for (const auto& col : columns) {
        AddField(structMembers, col);
    }
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

class TFilterInputConsumer : public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    explicit TFilterInputConsumer(NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker)
        : Worker(std::move(worker)) {
        LOG_ROW_DISPATCHER_DEBUG("TFilterInputConsumer::TFilterInputConsumer()");
    }

    ~TFilterInputConsumer() override {
        with_lock(Worker->GetScopedAlloc()) {
            Cache.Clear();
        }
    }

    void OnObject(const NYql::NUdf::TUnboxedValue* value) override {
        LOG_ROW_DISPATCHER_DEBUG("TFilterInputConsumer::OnObject: ");

        with_lock (Worker->GetScopedAlloc()) {
            auto& holderFactory = Worker->GetGraph().GetHolderFactory();
            NYql::NUdf::TUnboxedValue* items = nullptr;

            NYql::NUdf::TUnboxedValue result = Cache.NewArray(
                holderFactory,
                static_cast<ui32>(value->GetListLength()),
                items);
    
            for (size_t i = 0; i != value->GetListLength(); ++i) {
                const auto& cell = value->GetElement(i);
                NYql::NUdf::TStringRef strRef(cell.AsStringRef());
                NYql::NUdf::TStringValue str(strRef.Size());
                std::memcpy(str.Data(), strRef.Data(), strRef.Size());
                items[i] = NYql::NUdf::TUnboxedValuePod(std::move(str));
            }
            LOG_ROW_DISPATCHER_DEBUG("Push ");
            Worker->Push(std::move(result));
            LOG_ROW_DISPATCHER_DEBUG("Push end");
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


class TFilterOutputConsumer: public NYql::NPureCalc::IConsumer<const TString&> {
public:
    TFilterOutputConsumer(TCallback callback)
        : Callback(callback) {
    }

    void OnObject(const TString& value) override {
        Callback(value);
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

class TFilterPushRelayImpl: public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    TFilterPushRelayImpl(const TFilterOutputSpec& /*outputSpec*/, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<const TString&>> underlying)
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
        const auto& cell = value->GetElement(0);
        if (!cell) {
            // TODO
            return;
        }
        TString str(cell.AsStringRef());
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnObject(str);
    }

    void OnFinish() override {
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnFinish();
    }

private:
    THolder<NYql::NPureCalc::IConsumer<const TString&>> Underlying;
    [[maybe_unused]]    NYql::NPureCalc::IWorker* Worker;
  //  TFieldsMapping FieldsMapping;
};

} // namespace NFq

TString LogPrefix = "JsonFilter: ";

template <>
struct NYql::NPureCalc::TInputSpecTraits<NFq::TFilterInputSpec> {
    static constexpr bool IsPartial = false;
    static constexpr bool SupportPushStreamMode = true;

    using TConsumerType = THolder<NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*>>;

    static TConsumerType MakeConsumer(
        const NFq::TFilterInputSpec& spec,
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker
    ) {

        LOG_STREAMS_IMPL(DEBUG, YQ_ROW_DISPATCHER, "MakeConsumer()")
       // LOG_ROW_DISPATCHER_DEBUG("MakeConsumer()");

        Y_UNUSED(spec);
        return MakeHolder<NFq::TFilterInputConsumer>(std::move(worker));
    }
};

template <>
struct NYql::NPureCalc::TOutputSpecTraits<NFq::TFilterOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = false;
    static const constexpr bool SupportPushStreamMode = true;

    static void SetConsumerToWorker(const NFq::TFilterOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<const TString&>> consumer) {
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
        : LogPrefix("JsonFilter: ") {
        auto factory = NYql::NPureCalc::MakeProgramFactory(NYql::NPureCalc::TProgramFactoryOptions());

        try {
            LOG_ROW_DISPATCHER_DEBUG("Creating program...");
            Program = factory->MakePushStreamProgram(
                TFilterInputSpec(MakeSchema(columns)),
                TFilterOutputSpec(MakeSchema({"data"})),
                GenerateSql(columns, types, whereFilter),
                NYql::NPureCalc::ETranslationMode::SQL
            );
            LOG_ROW_DISPATCHER_DEBUG("Program created");
            InputConsumer = Program->Apply(MakeHolder<TFilterOutputConsumer>(callback));
            LOG_ROW_DISPATCHER_DEBUG("InputConsumer created");

        } catch (NYql::NPureCalc::TCompileError& e) {
            Cerr << e.GetIssues() << Endl; // TODO
            throw;
        }
    }

    void Push(const NYql::NUdf::TUnboxedValue* value) {
        try {
            LOG_ROW_DISPATCHER_DEBUG("Push ");
            LOG_ROW_DISPATCHER_DEBUG("Push " << value->GetListLength());

            InputConsumer->OnObject(value);
        } catch (const yexception& ex) {
            LOG_ROW_DISPATCHER_DEBUG("Push error"); // TODO : how to get error
        }
    }

private:
    TString GenerateSql(const TVector<TString>& columnNames, const TVector<TString>& columnTypes, const TString& whereFilter) {
        TStringStream str;
        str << "$fields = SELECT ";
        Y_ABORT_UNLESS(columnNames.size() == columnTypes.size());

        for (size_t i = 0; i < columnNames.size(); ++i) {
            str << "CAST(" << columnNames[i] << " as " << columnTypes[i] << ") as " << columnNames[i] << ((i != columnNames.size() - 1) ? "," : "");
            //str <<  columnNames[i] << ((i != columnNames.size() - 1) ? "," : "");
        }
        str << " FROM Input;\n";
        str << "$filtered = SELECT * FROM $fields " << whereFilter << ";\n";

        str << "SELECT Unwrap(Json::SerializeJson(Yson::From(TableRow()))) as data FROM $filtered";
        LOG_ROW_DISPATCHER_DEBUG("Generated sql: " << str.Str());
        return str.Str();
    }
         //   str << "CAST(" << columnNames[i] << " as " << columnTypes[i] << ") as " << columnNames[i] << ((i != columnNames.size() - 1) ? "," : "");

private:
    THolder<NYql::NPureCalc::TPushStreamProgram<TFilterInputSpec, TFilterOutputSpec>> Program;
    THolder<NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*>> InputConsumer;
    const TString LogPrefix;
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
    
void TJsonFilter::Push(const NYql::NUdf::TUnboxedValue* value) {
     Impl->Push(value);
}

std::unique_ptr<TJsonFilter> NewJsonFilter(
    const TVector<TString>& columns,
    const TVector<TString>& types,
    const TString& whereFilter,
    TCallback callback) {
    return std::unique_ptr<TJsonFilter>(new TJsonFilter(columns, types, whereFilter, callback));
}

} // namespace NFq
