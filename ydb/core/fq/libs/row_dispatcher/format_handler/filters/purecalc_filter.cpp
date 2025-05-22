#include "purecalc_filter.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/providers/common/schema/parser/yql_type_parser.h>
#include <yql/essentials/public/purecalc/common/interface.h>

namespace NFq::NRowDispatcher {

namespace {

constexpr const char* OFFSET_FIELD_NAME = "_offset";

NYT::TNode CreateTypeNode(const TString& fieldType) {
    return NYT::TNode::CreateList()
        .Add("DataType")
        .Add(fieldType);
}

void AddField(NYT::TNode& node, const TString& fieldName, const TString& fieldType) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(CreateTypeNode(fieldType))
    );
}

void AddColumn(NYT::TNode& node, const TSchemaColumn& column) {
    TString parseTypeError;
    TStringOutput errorStream(parseTypeError);
    NYT::TNode parsedType;
    if (!NYql::NCommon::ParseYson(parsedType, column.TypeYson, errorStream)) {
        throw yexception() << "Failed to parse column '" << column.Name << "' type yson " << column.TypeYson << ", error: " << parseTypeError;
    }

    node.Add(
        NYT::TNode::CreateList()
            .Add(column.Name)
            .Add(parsedType)
    );
}

NYT::TNode MakeInputSchema(const TVector<TSchemaColumn>& columns) {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, OFFSET_FIELD_NAME, "Uint64");
    for (const auto& column : columns) {
        AddColumn(structMembers, column);
    }
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

NYT::TNode MakeOutputSchema() {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, OFFSET_FIELD_NAME, "Uint64");
    return NYT::TNode::CreateList().Add("StructType").Add(std::move(structMembers));
}

struct TInputType {
    const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& Values;
    const ui64 NumberRows;
};

class TFilterInputSpec : public NYql::NPureCalc::TInputSpecBase {
public:
    explicit TFilterInputSpec(const NYT::TNode& schema)
        : Schemas({schema})
    {}

public:
    const TVector<NYT::TNode>& GetSchemas() const override {
        return Schemas;
    }

private:
    const TVector<NYT::TNode> Schemas;
};

class TFilterInputConsumer : public NYql::NPureCalc::IConsumer<TInputType> {
public:
    TFilterInputConsumer(const TFilterInputSpec& spec, NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker)
        : Worker(std::move(worker))
    {
        const NKikimr::NMiniKQL::TStructType* structType = Worker->GetInputType();
        const ui64 count = structType->GetMembersCount();
 
        THashMap<TString, ui64> schemaPositions;
        for (ui64 i = 0; i < count; ++i) { 
            const auto name = structType->GetMemberName(i);
            if (name == OFFSET_FIELD_NAME) {
                OffsetPosition = i;
            } else {
                schemaPositions[name] = i;
            }
        }

        const auto& fields = spec.GetSchemas()[0][1];
        Y_ENSURE(fields.IsList(), "Unexpected input spec type");
        Y_ENSURE(count == fields.Size(), "Unexpected purecalc schema size");

        FieldsPositions.reserve(count);
        for (const auto& field : fields.AsList()) {
            const auto& name = field[0].AsString();
            if (name != OFFSET_FIELD_NAME) {
                FieldsPositions.emplace_back(schemaPositions[name]);
            }
        }
    }

    ~TFilterInputConsumer() override {
        with_lock(Worker->GetScopedAlloc()) {
            Cache.Clear();
        }
    }

public:
    void OnObject(TInputType input) override {
        Y_ENSURE(FieldsPositions.size() == input.Values.size(), "Unexpected input scheme size");

        NKikimr::NMiniKQL::TThrowingBindTerminator bind;
        with_lock (Worker->GetScopedAlloc()) {
            Y_DEFER {
                // Clear cache after each object because
                // values allocated on another allocator and should be released
                Cache.Clear();
                Worker->Invalidate();
            };

            auto& holderFactory = Worker->GetGraph().GetHolderFactory();

            for (ui64 rowId = 0; rowId < input.NumberRows; ++rowId) {
                NYql::NUdf::TUnboxedValue* items = nullptr;
                NYql::NUdf::TUnboxedValue result = Cache.NewArray(holderFactory, static_cast<ui32>(input.Values.size() + 1), items);

                items[OffsetPosition] = NYql::NUdf::TUnboxedValuePod(rowId);

                for (ui64 fieldId = 0; const auto column : input.Values) {
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
    const NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> Worker;
    NKikimr::NMiniKQL::TPlainContainerCache Cache;

    ui64 OffsetPosition = 0;
    TVector<ui64> FieldsPositions;
};

class TFilterOutputSpec : public NYql::NPureCalc::TOutputSpecBase {
public:
    explicit TFilterOutputSpec(const NYT::TNode& schema)
        : Schema(schema)
    {}

public:
    const NYT::TNode& GetSchema() const override {
        return Schema;
    }

private:
    const NYT::TNode Schema;
};

class TFilterOutputConsumer : public NYql::NPureCalc::IConsumer<ui64> {
public:
    explicit TFilterOutputConsumer(IPurecalcFilterConsumer::TPtr consumer)
        : Consumer(std::move(consumer))
    {}

public:
    void OnObject(ui64 value) override {
        Consumer->OnFilteredData(value);
    }

    void OnFinish() override {
    }

private:
    const IPurecalcFilterConsumer::TPtr Consumer;
};

class TFilterPushRelayImpl : public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    TFilterPushRelayImpl(const TFilterOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<ui64>> underlying)
        : Underlying(std::move(underlying))
        , Worker(worker)
    {
        Y_UNUSED(outputSpec);
    }

public:
    void OnObject(const NYql::NUdf::TUnboxedValue* value) override {
        Y_ENSURE(value->GetListLength() == 1, "Unexpected output schema size");

        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnObject(value->GetElement(0).Get<ui64>());
    }

    void OnFinish() override {
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnFinish();
    }

private:
    const THolder<NYql::NPureCalc::IConsumer<ui64>> Underlying;
    NYql::NPureCalc::IWorker* Worker;
};

}  // anonymous namespace

}  // namespace NFq::NRowDispatcher

template <>
struct NYql::NPureCalc::TInputSpecTraits<NFq::NRowDispatcher::TFilterInputSpec> {
    static constexpr bool IsPartial = false;
    static constexpr bool SupportPushStreamMode = true;

    using TConsumerType = THolder<NYql::NPureCalc::IConsumer<NFq::NRowDispatcher::TInputType>>;

    static TConsumerType MakeConsumer(const NFq::NRowDispatcher::TFilterInputSpec& spec, NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker) {
        return MakeHolder<NFq::NRowDispatcher::TFilterInputConsumer>(spec, std::move(worker));
    }
};

template <>
struct NYql::NPureCalc::TOutputSpecTraits<NFq::NRowDispatcher::TFilterOutputSpec> {
    static const constexpr bool IsPartial = false;
    static const constexpr bool SupportPushStreamMode = true;

    static void SetConsumerToWorker(const NFq::NRowDispatcher::TFilterOutputSpec& outputSpec, NYql::NPureCalc::IPushStreamWorker* worker, THolder<NYql::NPureCalc::IConsumer<ui64>> consumer) {
        worker->SetConsumer(MakeHolder<NFq::NRowDispatcher::TFilterPushRelayImpl>(outputSpec, worker, std::move(consumer)));
    }
};

namespace NFq::NRowDispatcher {

namespace {

class TProgramHolder : public IProgramHolder {
public:
    using TPtr = TIntrusivePtr<TProgramHolder>;

public:
    TProgramHolder(IPurecalcFilterConsumer::TPtr consumer, const TString& sql)
        : Consumer(consumer)
        , Sql(sql)
    {}

    NYql::NPureCalc::IConsumer<TInputType>& GetConsumer() {
        Y_ENSURE(InputConsumer, "Program is not compiled");
        return *InputConsumer;
    }

public:
    void CreateProgram(NYql::NPureCalc::IProgramFactoryPtr programFactory) override {
        // Program should be stateless because input values
        // allocated on another allocator and should be released
        Program = programFactory->MakePushStreamProgram(
            TFilterInputSpec(MakeInputSchema(Consumer->GetColumns())),
            TFilterOutputSpec(MakeOutputSchema()),
            Sql,
            NYql::NPureCalc::ETranslationMode::SQL
        );
        InputConsumer = Program->Apply(MakeHolder<TFilterOutputConsumer>(Consumer));
    }

private:
    const IPurecalcFilterConsumer::TPtr Consumer;
    const TString Sql;

    THolder<NYql::NPureCalc::TPushStreamProgram<TFilterInputSpec, TFilterOutputSpec>> Program;
    THolder<NYql::NPureCalc::IConsumer<TInputType>> InputConsumer;
};

class TPurecalcFilter : public IPurecalcFilter {
public:
    TPurecalcFilter(IPurecalcFilterConsumer::TPtr consumer)
        : Consumer(consumer)
        , PurecalcSettings(consumer->GetPurecalcSettings())
        , LogPrefix("TPurecalcFilter: ")
        , ProgramHolder(MakeIntrusive<TProgramHolder>(consumer, GenerateSql()))
    {}

public:
    void FilterData(const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 numberRows) override {
        LOG_ROW_DISPATCHER_TRACE("Do filtering for " << numberRows << " rows");
        ProgramHolder->GetConsumer().OnObject({.Values = values, .NumberRows = numberRows});
    }

    std::unique_ptr<TEvRowDispatcher::TEvPurecalcCompileRequest> GetCompileRequest() override {
        Y_ENSURE(ProgramHolder, "Can not create compile request twice");
        auto result = std::make_unique<TEvRowDispatcher::TEvPurecalcCompileRequest>(std::move(ProgramHolder), PurecalcSettings);
        ProgramHolder = nullptr;
        return result;
    }

    void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr ev) override {
        Y_ENSURE(!ProgramHolder, "Can not handle compile response twice");

        auto result = static_cast<TProgramHolder*>(ev->Get()->ProgramHolder.Release());
        Y_ENSURE(result, "Unexpected compile response");

        ProgramHolder = TIntrusivePtr<TProgramHolder>(result);
    }

private:
    TString GenerateSql() {
        TStringStream str;
        str << "PRAGMA config.flags(\"LLVM\", \"" << (PurecalcSettings.EnabledLLVM ? "ON" : "OFF") << "\");\n";
        str << "SELECT " << OFFSET_FIELD_NAME << " FROM Input " << Consumer->GetWhereFilter() << ";\n";

        LOG_ROW_DISPATCHER_DEBUG("Generated sql:\n" << str.Str());
        return str.Str();
    }

private:
    const IPurecalcFilterConsumer::TPtr Consumer;
    const TPurecalcCompileSettings PurecalcSettings;
    const TString LogPrefix;

    TProgramHolder::TPtr ProgramHolder;
};

}  // anonymous namespace

TValueStatus<IPurecalcFilter::TPtr> CreatePurecalcFilter(IPurecalcFilterConsumer::TPtr consumer) {
    try {
        return IPurecalcFilter::TPtr(MakeIntrusive<TPurecalcFilter>(consumer));
    } catch (...) {
        return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to create purecalc filter with predicate '" << consumer->GetWhereFilter() << "', got unexpected exception: " << CurrentExceptionMessage());
    }
}

}  // namespace NFq::NRowDispatcher
