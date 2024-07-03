
#pragma once

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

namespace NFq {

void AddField(NYT::TNode& node, const TString& field) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(field)
            .Add(NYT::TNode::CreateList().Add("DataType").Add("String"))
    );
}

// NYT::TNode MakeStructInputSchema() {
//     auto structMembers = NYT::TNode::CreateList();
//     AddField(structMembers, "data");
//     return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
// }

NYT::TNode MakeSchema(const TVector<TString>& columns) {
    auto structMembers = NYT::TNode::CreateList();
    for (const auto& col : columns)
    AddField(structMembers, col);
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

class TInputConsumer : public NYql::NPureCalc::IConsumer<TString> {
public:
    explicit TInputConsumer(NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker)
        : Worker(std::move(worker)) {
    }

    ~TInputConsumer() override {
        with_lock(Worker->GetScopedAlloc()) {
            Cache.Clear();
        }
    }

    void OnObject(TString value) override {
        std::cout << "TInputConsumer::OnObject " << value << std::endl;
        with_lock (Worker->GetScopedAlloc()) {
            auto& holderFactory = Worker->GetGraph().GetHolderFactory();
            NYql::NUdf::TUnboxedValue* items = nullptr;

            NYql::NUdf::TUnboxedValue result = Cache.NewArray(
                holderFactory,
                static_cast<ui32>(1),
                items);
    
            NYql::NUdf::TStringValue str(value.Size());
            std::memcpy(str.Data(), value.Data(), value.Size());
            items[0] = NYql::NUdf::TUnboxedValuePod(std::move(str));
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
};


class TInputSpec : public NYql::NPureCalc::TInputSpecBase {
public:
    TInputSpec() {
        Schemas = {MakeSchema({"data"})};
    }

    const TVector<NYT::TNode>& GetSchemas() const override {
        return Schemas;
    }

private:
    TVector<NYT::TNode> Schemas;
};




class TOutputConsumer: public NYql::NPureCalc::IConsumer<NYT::TNode*> {
public:

    TOutputConsumer(std::function<void(const TVector<TString>&)> callback)
        : Callback(callback)
    {}

    void OnObject(NYT::TNode* parsedRecord) override {
        std::cerr << "  OnObjectOnObjectOnObjectOnObject" <<  std::endl;
        TVector<TString> result;
        for (const auto& field : parsedRecord->AsMap()) {
            std::cerr << "  out: " << field.first << ": " << field.second.AsString() << std::endl;
        }
        Callback(result);
    }

    void OnFinish() override {
        Cout << "On Finish" << Endl;
    }
private:
    std::function<void(const TVector<TString>&)> Callback;
};


class TOutputSpec: public NYql::NPureCalc::TOutputSpecBase {
public:
    explicit TOutputSpec(const NYT::TNode& schema)
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
    THashMap<TString, size_t> FieldsPositions;

    TFieldsMapping(const NYT::TNode& schema) {
        const auto& fields = schema[1];
        assert(fields.IsList());

        for (size_t i = 0; i < fields.Size(); ++i) {
            auto name = fields[i][0].AsString();
            FieldsPositions[std::move(name)] = i;
        }
    }

    size_t GetPosition(const TString& fieldName) const {
        return FieldsPositions.at(fieldName);
    }

    bool Has(const TString& fieldName) const {
        return FieldsPositions.contains(fieldName);
    }
};



class TPushRelayImpl: public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    TPushRelayImpl(const TOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<NYT::TNode*>> underlying)
        : Underlying(std::move(underlying))
        , Worker(worker)
        , FieldsMapping(outputSpec.GetSchema())
    {}

void FillOutputNode(const NYql::NUdf::TUnboxedValue* source, NYT::TNode& destination, const TFieldsMapping& fieldsMapping) {
    for (const auto& field : fieldsMapping.FieldsPositions) {
        const auto& cell = source->GetElement(field.second);
        if (!cell) {
            continue;
        }
        destination[field.first] = TString(cell.AsStringRef());
    }
}

public:
    void OnObject(const NYql::NUdf::TUnboxedValue* value) override {
        NYT::TNode result = NYT::TNode::CreateMap();
        FillOutputNode(value, result, FieldsMapping);
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnObject(&result);
    }

    void OnFinish() override {
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnFinish();
    }

private:
    THolder<NYql::NPureCalc::IConsumer<NYT::TNode*>> Underlying;
    [[maybe_unused]]    NYql::NPureCalc::IWorker* Worker;
    TFieldsMapping FieldsMapping;
};

} // namespace NFq

template <>
struct NYql::NPureCalc::TInputSpecTraits<NFq::TInputSpec> {
    static constexpr bool IsPartial = false;
    static constexpr bool SupportPushStreamMode = true;

    using TConsumerType = THolder<NYql::NPureCalc::IConsumer<TString>>;

    static TConsumerType MakeConsumer(
        const NFq::TInputSpec& spec,
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker
    ) {
        Y_UNUSED(spec);
        return MakeHolder<NFq::TInputConsumer>(std::move(worker));
    }
};


template <>
struct NYql::NPureCalc::TOutputSpecTraits<NFq::TOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = false;
    static const constexpr bool SupportPushStreamMode = true;

    static void SetConsumerToWorker(const NFq::TOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<NYT::TNode*>> consumer) {
        worker->SetConsumer(MakeHolder<NFq::TPushRelayImpl>(outputSpec, worker, std::move(consumer)));
    }
};

namespace NFq {

class TJsonParser {    
    
    const TString LogPrefix;

public:
    TJsonParser(
        const TVector<TString>& columns,
        std::function<void(const TVector<TString>&)> callback)
        : LogPrefix("JsonParser: ") {
        auto factory = NYql::NPureCalc::MakeProgramFactory(NYql::NPureCalc::TProgramFactoryOptions());

        try {
            LOG_ROW_DISPATCHER_DEBUG("Creating program...");
            Program = factory->MakePushStreamProgram(
                TInputSpec(),
                TOutputSpec(MakeSchema(columns)),
                GenerateSql(columns),
                NYql::NPureCalc::ETranslationMode::SQL
            );
            LOG_ROW_DISPATCHER_DEBUG("Program created");
            InputConsumer = Program->Apply(MakeHolder<TOutputConsumer>(callback));
            LOG_ROW_DISPATCHER_DEBUG("InputConsumer created");

        } catch (NYql::NPureCalc::TCompileError& e) {
            Cerr << e.GetIssues() << Endl;
            throw;
        }
    }
    
    void Push(const TString& value) {
        LOG_ROW_DISPATCHER_DEBUG("Push " << value);
        InputConsumer->OnObject(value);
    }

private:
    TString GenerateSql(const TVector<TString>& columns) {
        TStringStream str;
        str << R"($json = SELECT CAST(data AS Json) as `Json` FROM Input;)"; 
        str << "\nSELECT ";
        for (auto it = columns.begin(); it != columns.end(); ++it) {
            str << R"(CAST(Unwrap(JSON_VALUE(`Json`, "$.)" << *it << "\")) as String) as "
                << *it << ((it != columns.end() - 1) ? "," : "");
        }
        str << " FROM $json;";
        LOG_ROW_DISPATCHER_DEBUG("GenerateSql " << str.Str());
        return str.Str();
    }

private:
    THolder<NYql::NPureCalc::TPushStreamProgram<TInputSpec, TOutputSpec>> Program;
    THolder<NYql::NPureCalc::IConsumer<TString>> InputConsumer;
};

} // namespace NFq
