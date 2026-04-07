#include "purecalc_filter.h"

#include <fmt/format.h>

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/providers/common/schema/parser/yql_type_parser.h>
#include <yql/essentials/public/purecalc/common/interface.h>

namespace NFq::NRowDispatcher {

namespace {

constexpr std::string_view FILTER_FIELD_NAME = "_filter";
constexpr std::string_view OFFSET_FIELD_NAME = "_offset";
constexpr std::string_view WATERMARK_FIELD_NAME = "_watermark";

NYT::TNode CreateNamedNode(std::string_view name, NYT::TNode&& node) {
    return NYT::TNode::CreateList().Add(NYT::TNode(name)).Add(std::move(node));
}

NYT::TNode CreateTypeNode(NYT::TNode&& typeNode) {
    return CreateNamedNode("DataType", std::move(typeNode));
}

NYT::TNode CreateOptionalTypeNode(NYT::TNode&& typeNode) {
    return CreateNamedNode("OptionalType", std::move(typeNode));
}

NYT::TNode CreateStructTypeNode(NYT::TNode&& membersNode) {
    return CreateNamedNode("StructType", std::move(membersNode));
}

NYT::TNode CreateFieldNode(std::string_view fieldName, NYT::TNode&& typeNode) {
    return CreateNamedNode(fieldName, std::move(typeNode));
}

NYT::TNode CreateColumnNode(const TSchemaColumn& column) {
    TString parseTypeError;
    TStringOutput errorStream(parseTypeError);
    NYT::TNode parsedType;
    if (!NYql::NCommon::ParseYson(parsedType, column.TypeYson, errorStream)) {
        throw yexception() << "Failed to parse column '" << column.Name << "' type yson " << column.TypeYson << ", error: " << parseTypeError;
    }

    return CreateNamedNode(column.Name, std::move(parsedType));
}

NYT::TNode MakeInputSchema(IProcessedDataConsumer::TPtr consumer) {
    auto membersNode = NYT::TNode::CreateList()
        .Add(CreateFieldNode(OFFSET_FIELD_NAME, CreateTypeNode("Uint64")));
    for (const auto& column : consumer->GetColumns()) {
        membersNode.Add(CreateColumnNode(column));
    }
    return CreateStructTypeNode(std::move(membersNode));
}

NYT::TNode MakeOutputSchema(IProcessedDataConsumer::TPtr consumer) {
    auto membersNode = NYT::TNode::CreateList()
        .Add(CreateFieldNode(FILTER_FIELD_NAME, CreateTypeNode("Bool")))
        .Add(CreateFieldNode(OFFSET_FIELD_NAME, CreateTypeNode("Uint64")));
    if (consumer->GetWatermarkExpr()) {
        membersNode.Add(CreateFieldNode(WATERMARK_FIELD_NAME, CreateOptionalTypeNode(CreateTypeNode("Timestamp"))));
    }
    return CreateStructTypeNode(std::move(membersNode));
}

struct TInputType {
    const TVector<std::span<NYql::NUdf::TUnboxedValue>>& Values;
    ui64 NumberRows;
};

class TInputSpec : public NYql::NPureCalc::TInputSpecBase {
public:
    explicit TInputSpec(const NYT::TNode& schema)
        : Schemas({schema})
    {}

public:
    const TVector<NYT::TNode>& GetSchemas() const override {
        return Schemas;
    }

private:
    const TVector<NYT::TNode> Schemas;
};

class TInputConsumer : public NYql::NPureCalc::IConsumer<TInputType> {
public:
    TInputConsumer(const TInputSpec& spec, NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker)
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

    ~TInputConsumer() override {
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

                for (ui64 fieldId = 0; const auto& column : input.Values) {
                    Y_DEBUG_ABORT_UNLESS(column.size() > rowId);
                    items[FieldsPositions[fieldId++]] = column[rowId];
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

class TOutputSpec : public NYql::NPureCalc::TOutputSpecBase {
public:
    explicit TOutputSpec(const NYT::TNode& schema)
        : Schema(schema)
    {}

public:
    const NYT::TNode& GetSchema() const override {
        return Schema;
    }

private:
    const NYT::TNode Schema;
};

class TOutputConsumer : public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    explicit TOutputConsumer(IProcessedDataConsumer::TPtr consumer)
        : Consumer_(std::move(consumer))
    {}

public:
    void OnObject(const NYql::NUdf::TUnboxedValue* value) override {
        Consumer_->OnData(value);
    }

    void OnFinish() override {
    }

private:
    IProcessedDataConsumer::TPtr Consumer_;
};

class TPushRelayImpl : public NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*> {
public:
    TPushRelayImpl(
        const TOutputSpec& outputSpec,
        NYql::NPureCalc::IPushStreamWorker* worker,
        THolder<NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*>> underlying
    )
        : Underlying(std::move(underlying))
        , Worker(worker)
    {
        Y_UNUSED(outputSpec);
    }

public:
    void OnObject(const NYql::NUdf::TUnboxedValue* value) override {
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnObject(value);
    }

    void OnFinish() override {
        auto unguard = Unguard(Worker->GetScopedAlloc());
        Underlying->OnFinish();
    }

private:
    THolder<NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*>> Underlying;
    NYql::NPureCalc::IWorker* Worker;
};

}  // anonymous namespace

}  // namespace NFq::NRowDispatcher

template <>
struct NYql::NPureCalc::TInputSpecTraits<NFq::NRowDispatcher::TInputSpec> {
    [[maybe_unused]] static constexpr bool IsPartial = false;
    [[maybe_unused]] static constexpr bool SupportPushStreamMode = true;

    using TConsumerType = THolder<NYql::NPureCalc::IConsumer<NFq::NRowDispatcher::TInputType>>;

    static TConsumerType MakeConsumer(const NFq::NRowDispatcher::TInputSpec& spec, NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPushStreamWorker> worker) {
        return MakeHolder<NFq::NRowDispatcher::TInputConsumer>(spec, std::move(worker));
    }
};

template <>
struct NYql::NPureCalc::TOutputSpecTraits<NFq::NRowDispatcher::TOutputSpec> {
    [[maybe_unused]] static const constexpr bool IsPartial = false;
    [[maybe_unused]] static const constexpr bool SupportPushStreamMode = true;

    static void SetConsumerToWorker(
        const NFq::NRowDispatcher::TOutputSpec& outputSpec,
        NYql::NPureCalc::IPushStreamWorker* worker,
        THolder<NYql::NPureCalc::IConsumer<const NYql::NUdf::TUnboxedValue*>> consumer
    ) {
        worker->SetConsumer(MakeHolder<NFq::NRowDispatcher::TPushRelayImpl>(outputSpec, worker, std::move(consumer)));
    }
};

namespace NFq::NRowDispatcher {

namespace {

class TProgramHolder final : public IProgramHolder {
public:
    using TPtr = TIntrusivePtr<TProgramHolder>;

public:
    TProgramHolder(
        IProcessedDataConsumer::TPtr consumer,
        NYT::TNode inputSchema,
        NYT::TNode outputSchema,
        TString query
    )
        : Consumer_(std::move(consumer))
        , InputSchema_(std::move(inputSchema))
        , OutputSchema_(std::move(outputSchema))
        , Query_(std::move(query))
    {}

    NYql::NPureCalc::IConsumer<TInputType>& GetConsumer() {
        Y_ENSURE(InputConsumer_, "Program is not compiled");
        return *InputConsumer_;
    }

public:
    void CreateProgram(NYql::NPureCalc::IProgramFactoryPtr programFactory) override {
        // Program should be stateless because input values
        // allocated on another allocator and should be released
        Program_ = programFactory->MakePushStreamProgram(
            TInputSpec(InputSchema_),
            TOutputSpec(OutputSchema_),
            Query_,
            NYql::NPureCalc::ETranslationMode::SQL
        );
        InputConsumer_ = Program_->Apply(MakeHolder<TOutputConsumer>(Consumer_));
    }

    TStringBuf GetQuery() const override {
        return Query_;
    }

private:
    IProcessedDataConsumer::TPtr Consumer_;
    NYT::TNode InputSchema_;
    NYT::TNode OutputSchema_;
    TString Query_;

    THolder<NYql::NPureCalc::TPushStreamProgram<TInputSpec, TOutputSpec>> Program_;
    THolder<NYql::NPureCalc::IConsumer<TInputType>> InputConsumer_;
};

class TProgramCompileHandler final : public IProgramCompileHandler, public TNonCopyable {
public:
    TProgramCompileHandler(
        IProcessedDataConsumer::TPtr consumer,
        IProgramHolder::TPtr programHolder,
        ui64 cookie,
        NActors::TActorId compileServiceId,
        NActors::TActorId owner,
        NMonitoring::TDynamicCounterPtr counters
    )
        : IProgramCompileHandler(std::move(consumer), std::move(programHolder), cookie)
        , CompileServiceId_(compileServiceId)
        , Owner_(owner)
        , InFlightCompileRequests_(counters->GetCounter("InFlightCompileRequests", false))
        , CompileErrors_(counters->GetCounter("CompileErrors", true))
    {
        InFlightCompileRequests_->Inc();
    }

    ~TProgramCompileHandler() {
        InFlightCompileRequests_->Dec();
    }

    void Compile() override {
        LOG_ROW_DISPATCHER_TRACE("Send compile request with id " << Cookie_);

        auto compileRequest = std::make_unique<TEvRowDispatcher::TEvPurecalcCompileRequest>(std::exchange(ProgramHolder_, nullptr), Consumer_->GetPurecalcSettings());
        NActors::TActivationContext::ActorSystem()->Send(
            new NActors::IEventHandle(
                CompileServiceId_,
                Owner_,
                compileRequest.release(),
                0,
                Cookie_
            )
        );
    }

    void AbortCompilation() override {
        LOG_ROW_DISPATCHER_TRACE("Send abort compile request with id " << Cookie_);
        NActors::TActivationContext::ActorSystem()->Send(
            new NActors::IEventHandle(
                CompileServiceId_,
                Owner_,
                new TEvRowDispatcher::TEvPurecalcCompileAbort(),
                0,
                Cookie_
            )
        );
    }

    void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) override {
        ProgramHolder_ = ev->Get()->ProgramHolder.Release();
        LOG_ROW_DISPATCHER_TRACE("Program compilation finished");
    }

    void OnCompileError(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) override {
        auto status = TStatus::Fail(ev->Get()->Status, std::move(ev->Get()->Issues));
        LOG_ROW_DISPATCHER_ERROR("Program compilation error: " << status.GetErrorMessage());
        CompileErrors_->Inc();
        Consumer_->OnError(status.AddParentIssue("Failed to compile client program"));
    }

private:
    // NOLINTNEXTLINE(readability-identifier-naming)
    static constexpr std::string_view LogPrefix = "TProgramCompileHandler: ";
    NActors::TActorId CompileServiceId_;
    NActors::TActorId Owner_;

    NMonitoring::TDynamicCounters::TCounterPtr InFlightCompileRequests_;
    NMonitoring::TDynamicCounters::TCounterPtr CompileErrors_;
};

class TProgramRunHandler final : public IProgramRunHandler, public TNonCopyable {
public:
    TProgramRunHandler(
        IProcessedDataConsumer::TPtr consumer,
        IProgramHolder::TPtr programHolder,
        NMonitoring::TDynamicCounterPtr counters
    )
        : IProgramRunHandler(std::move(consumer), std::move(programHolder))
        , ActiveFilters_(counters->GetCounter("ActiveFilters", false))
    {
        ActiveFilters_->Inc();
    }

    ~TProgramRunHandler() {
        ActiveFilters_->Dec();
    }

    void ProcessData(const TVector<std::span<NYql::NUdf::TUnboxedValue>>& values, ui64 numberRows) const override {
        LOG_ROW_DISPATCHER_TRACE("ProcessData for " << numberRows << " rows");

        if (!ProgramHolder_) {
            LOG_ROW_DISPATCHER_TRACE("Add " << numberRows << " rows to client " << Consumer_->GetClientId() << " without processing");
            for (ui64 rowId = 0; rowId < numberRows; ++rowId) {
                NYql::NUdf::TUnboxedValue value = NYql::NUdf::TUnboxedValuePod{rowId};
                Consumer_->OnData(&value);
            }
            return;
        }

        auto* programHolder = dynamic_cast<TProgramHolder*>(ProgramHolder_.Get());
        Y_ENSURE(programHolder, "Expected TProgramHolder");
        programHolder->GetConsumer().OnObject({.Values = values, .NumberRows = numberRows});
    }

private:
    // NOLINTNEXTLINE(readability-identifier-naming)
    static constexpr std::string_view LogPrefix = "TProgramRunHandler: ";

    NMonitoring::TDynamicCounters::TCounterPtr ActiveFilters_;
};

[[nodiscard]] TString GenerateSql(IProcessedDataConsumer::TPtr consumer) {
    static constexpr std::string_view LogPrefix = "GenerateSql: ";

    const auto& settings = consumer-> GetPurecalcSettings();
    auto filterExpr = TStringBuf(consumer->GetFilterExpr());
    const auto& watermarkExpr = consumer->GetWatermarkExpr();

    if (!filterExpr && !watermarkExpr) {
        LOG_ROW_DISPATCHER_TRACE("No sql was generated");
        return {};
    }

    constexpr auto wherePrefix = "WHERE "sv;
    if (filterExpr.starts_with(wherePrefix)) { // workaround for YQ-4827
        filterExpr.remove_prefix(wherePrefix.size());
    }

    using namespace fmt::literals;
    auto result = fmt::format(
        R"sql(
            PRAGMA config.flags("LLVM", "{enabled_llvm}");
            SELECT COALESCE({filter_expr}, FALSE) AS {filter_field_name}, {offset_field_name}{watermark_expr} FROM Input;
        )sql",
        "enabled_llvm"_a = settings.EnabledLLVM ? "ON" : "OFF",
        "filter_expr"_a = filterExpr ? filterExpr : "TRUE",
        "filter_field_name"_a = FILTER_FIELD_NAME,
        "offset_field_name"_a = OFFSET_FIELD_NAME,
        "watermark_expr"_a = watermarkExpr ? static_cast<TString>(TStringBuilder() << ", (" << watermarkExpr << ") AS " << WATERMARK_FIELD_NAME) : ""
    );

    LOG_ROW_DISPATCHER_DEBUG("Generated sql:\n" << result);
    return result;
}

}  // anonymous namespace

IProgramHolder::TPtr CreateProgramHolder(IProcessedDataConsumer::TPtr consumer) {
    auto query = GenerateSql(consumer);

    if (!query) {
        return {};
    }

    return MakeIntrusive<TProgramHolder>(
        consumer,
        MakeInputSchema(consumer),
        MakeOutputSchema(consumer),
        std::move(query)
    );
}

IProgramCompileHandler::TPtr CreateProgramCompileHandler(
    IProcessedDataConsumer::TPtr consumer,
    IProgramHolder::TPtr programHolder,
    ui64 cookie,
    NActors::TActorId compileServiceId,
    NActors::TActorId owner,
    NMonitoring::TDynamicCounterPtr counters
) {
    return MakeIntrusive<TProgramCompileHandler>(std::move(consumer), std::move(programHolder), cookie, compileServiceId, owner, std::move(counters));
}

IProgramRunHandler::TPtr CreateProgramRunHandler(
    IProcessedDataConsumer::TPtr consumer,
    IProgramHolder::TPtr programHolder,
    NMonitoring::TDynamicCounterPtr counters
) {
    return MakeIntrusive<TProgramRunHandler>(std::move(consumer), std::move(programHolder), std::move(counters));
}

}  // namespace NFq::NRowDispatcher
