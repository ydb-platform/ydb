#include "ut_common.h"

#include <library/cpp/testing/common/env.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/backtrace.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/library/yql/dq/common/rope_over_buffer.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/public/purecalc/common/interface.h>

#include <util/system/datetime.h>

namespace NFq::NRowDispatcher::NTests {

namespace {

const NKikimr::NMiniKQL::IFunctionRegistry::TPtr FunctionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(&PrintBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, {});

class TPurecalcCompileServiceMock : public NActors::TActor<TPurecalcCompileServiceMock> {
    using TBase = NActors::TActor<TPurecalcCompileServiceMock>;

public:
    TPurecalcCompileServiceMock(NActors::TActorId owner, const TDuration& responseDelay = {})
        : TBase(&TPurecalcCompileServiceMock::StateFunc)
        , Owner(owner)
        , ProgramFactory(NYql::NPureCalc::MakeProgramFactory())
        , ResponseDelay(responseDelay)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRowDispatcher::TEvPurecalcCompileRequest, Handle)
        IgnoreFunc(TEvRowDispatcher::TEvPurecalcCompileAbort)
    )

    void Handle(TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr& ev) {
        IProgramHolder::TPtr programHolder = std::move(ev->Get()->ProgramHolder);

        try {
            programHolder->CreateProgram(ProgramFactory);
        } catch (const NYql::NPureCalc::TCompileError& e) {
            UNIT_FAIL("Failed to compile purecalc filter: sql: " << e.GetYql() << ", error: " << e.GetIssues());
        }

        auto event = new TEvRowDispatcher::TEvPurecalcCompileResponse(std::move(programHolder));
        if (!ResponseDelay) {
            Send(ev->Sender, event, 0, ev->Cookie);
        } else {
            NActors::TActivationContext::Schedule(ResponseDelay, new NActors::IEventHandle(ev->Sender, ev->Sender, event, 0, ev->Cookie));
        }
        Send(Owner, new NActors::TEvents::TEvPing());
    }

private:
    const NActors::TActorId Owner;
    const NYql::NPureCalc::IProgramFactoryPtr ProgramFactory;
    const TDuration ResponseDelay;
};

//// TBaseFixture::ICell

class TOptionalCell : public TBaseFixture::ICell {
public:
    TOptionalCell(ICell::TPtr value)
        : Value(value)
    {}

public:
    TString GetType() const override {
        return TStringBuilder() << "[OptionalType; " << Value->GetType() << "]";
    }

    void Validate(const NYql::NUdf::TUnboxedValue& parsedValue) const override {
        if (!parsedValue) {
            UNIT_FAIL("Unexpected NULL value for optional cell");
            return;
        }
        Value->Validate(parsedValue.GetOptionalValue());
    }

private:
    const ICell::TPtr Value;
};

class TStringSell : public TBaseFixture::ICell {
public:
    TStringSell(const TString& value)
        : Value(value)
    {}

public:
    TString GetType() const override {
        return "[DataType; String]";
    }

    void Validate(const NYql::NUdf::TUnboxedValue& parsedValue) const override {
        UNIT_ASSERT_VALUES_EQUAL(Value, TString(parsedValue.AsStringRef()));
    }

private:
    const TString Value;
};

class TUint64Sell : public TBaseFixture::ICell {
public:
    TUint64Sell(ui64 value)
        : Value(value)
    {}

public:
    TString GetType() const override {
        return "[DataType; Uint64]";
    }

    void Validate(const NYql::NUdf::TUnboxedValue& parsedValue) const override {
        UNIT_ASSERT_VALUES_EQUAL(Value, parsedValue.Get<ui64>());
    }

private:
    const ui64 Value;
};

}  // anonymous namespace

//// TBaseFixture::TRow

TBaseFixture::TRow& TBaseFixture::TRow::AddCell(ICell::TPtr cell, bool optional) {
    if (optional) {
        cell = MakeIntrusive<TOptionalCell>(cell);
    }

    Cells.emplace_back(cell);
    return *this;
}

TBaseFixture::TRow& TBaseFixture::TRow::AddString(const TString& value, bool optional) {
    return AddCell(MakeIntrusive<TStringSell>(value), optional);
}

TBaseFixture::TRow& TBaseFixture::TRow::AddUint64(ui64 value, bool optional) {
    return AddCell(MakeIntrusive<TUint64Sell>(value), optional);
}

//// TBaseFixture::TBatch

TBaseFixture::TBatch::TBatch(std::initializer_list<TRow> rows)
    : Rows(rows)
{}

TBaseFixture::TBatch& TBaseFixture::TBatch::AddRow(TRow row) {
    Rows.emplace_back(row);
    return *this;
}

//// TBaseFixture

TFormatHandlerTestRuntime::TFormatHandlerTestRuntime()
    : TTestActorRuntimeBase(/* nodeCount */ 1, /* useRealThreads */ true)
{}

void TFormatHandlerTestRuntime::InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) {
    // These tests need AppData for actor helpers, but no tablet services or factories.
    node->AppData0 = std::make_shared<NKikimr::TAppData>(
        0, 0, 0, 0, TMap<TString, ui32>{}, nullptr, FunctionRegistry.Get(), nullptr, nullptr);
    node->LogSettings->Append(
        NKikimrServices::EServiceKikimr_MIN,
        NKikimrServices::EServiceKikimr_MAX,
        NKikimrServices::EServiceKikimr_Name);

    const THashMap<TString, NActors::NLog::EPriority> priorities = {
        {"TRACE", NActors::NLog::PRI_TRACE},
        {"DEBUG", NActors::NLog::PRI_DEBUG},
        {"INFO", NActors::NLog::PRI_INFO},
        {"NOTICE", NActors::NLog::PRI_NOTICE},
        {"WARN", NActors::NLog::PRI_WARN},
        {"ERROR", NActors::NLog::PRI_ERROR},
        {"CRIT", NActors::NLog::PRI_CRIT},
        {"ALERT", NActors::NLog::PRI_ALERT},
        {"EMERG", NActors::NLog::PRI_EMERG},
    };
    for (int service = NKikimrServices::EServiceKikimr_MIN; service <= NKikimrServices::EServiceKikimr_MAX; ++service) {
        if (!NKikimrServices::EServiceKikimr_IsValid(service)) {
            continue;
        }
        TString level = GetTestParam(TStringBuilder() << "TEST_LOG_" << NKikimrServices::EServiceKikimr_Name(static_cast<NKikimrServices::EServiceKikimr>(service)));
        if (level.empty()) {
            level = GetTestParam("TEST_LOG");
        }
        auto priority = service == NKikimrServices::FQ_ROW_DISPATCHER ? NActors::NLog::PRI_TRACE : NActors::NLog::PRI_WARN;
        if (!level.empty()) {
            level.to_upper();
            const auto it = priorities.find(level);
            UNIT_ASSERT_C(it != priorities.end(), "Unknown log level: " << level);
            priority = it->second;
        }
        TString explanation;
        UNIT_ASSERT_C(!node->LogSettings->SetLevel(priority, service, explanation), explanation);
    }
    TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
}

TBaseFixture::TBaseFixture()
    : TTypeParser(__LOCATION__, ::NFq::NRowDispatcher::NTests::FunctionRegistry.Get(), {})
    , MemoryInfo("TBaseFixture alloc")
    , HolderFactory(std::make_unique<NKikimr::NMiniKQL::THolderFactory>(Alloc.Ref(), MemoryInfo))
{
    Alloc.Ref().UseRefLocking = true;
}

void TBaseFixture::SetUp(NUnitTest::TTestContext&) {
    NKikimr::EnableYDBBacktraceFormat();
    Runtime.SetLogBackend(NActors::CreateStderrBackend());
    Runtime.SetDispatchTimeout(WAIT_TIMEOUT);
    Runtime.Initialize();

    Mailbox = std::make_unique<NActors::TMailbox>();
    ExecutorThread = std::make_unique<NActors::TExecutorThread>(0, Runtime.GetActorSystem(0), nullptr, "test thread");
    ActorCtx = std::make_unique<NActors::TActorContext>(*Mailbox, *ExecutorThread, GetCycleCountFast(), NActors::TActorId());
    PrevActorCtx = NActors::TlsActivationContext;
    NActors::TlsActivationContext = ActorCtx.get();
}

void TBaseFixture::TearDown(NUnitTest::TTestContext&) {
    with_lock(Alloc) {
        ProgramBuilder.reset();
        TypeEnv.reset();
        HolderFactory.reset();
    }

    NActors::TlsActivationContext = PrevActorCtx;
    PrevActorCtx = nullptr;
}

void TBaseFixture::CheckMessageBatch(TRope serializedBatch, const TBatch& expectedBatch) const {
    const auto& expectedRows = expectedBatch.Rows;
    UNIT_ASSERT_C(!expectedRows.empty(), "Expected batch should not be empty");

    // Parse row type (take first row for infer)
    const auto& expectedFirstRow = expectedRows.front().Cells;
    TVector<NKikimr::NMiniKQL::TType* const> columnTypes;
    columnTypes.reserve(expectedFirstRow.size());
    for (const auto& cell : expectedFirstRow) {
        columnTypes.emplace_back(CheckSuccess(ParseTypeYson(cell->GetType())));
    }

    with_lock(Alloc) {
        // Parse messages
        const auto rowType = ProgramBuilder->NewMultiType(columnTypes);
        const auto dataUnpacker = std::make_unique<NKikimr::NMiniKQL::TValuePackerTransport<true>>(rowType, NKikimr::NMiniKQL::EValuePackerVersion::V0, NYql::DefaultDatumTestValidationMode);

        NKikimr::NMiniKQL::TUnboxedValueBatch parsedData(rowType);
        dataUnpacker->UnpackBatch(NYql::MakeChunkedBuffer(std::move(serializedBatch)), *HolderFactory, parsedData);

        // Validate data
        UNIT_ASSERT_VALUES_EQUAL(parsedData.RowCount(), expectedRows.size());
        UNIT_ASSERT_VALUES_EQUAL(parsedData.Width(), expectedFirstRow.size());

        ui64 rowIndex = 0;
        parsedData.ForEachRowWide([&](NYql::NUdf::TUnboxedValue* values, ui32 width){
            UNIT_ASSERT_GE(expectedRows.size(), rowIndex + 1);
            const auto& expectedRow = expectedRows[rowIndex++].Cells;

            UNIT_ASSERT_VALUES_EQUAL(width, expectedRow.size());
            for (ui32 i = 0; i < width; ++i) {
                expectedRow[i]->Validate(values[i]);
            }
        });
    }
}

TMessageStreamRecord TBaseFixture::GetRecord(ui64 offset, const TString& data) {
    TMessageStreamRecord record;
    record.Data = data;
    record.Id.Offset = offset;
    return record;
}

//// Functions

void WaitFor(TDuration timeout, const TString& description, std::function<bool()> predicate) {
    const auto start = TInstant::Now();
    auto delay = TDuration::MilliSeconds(1);
    while (TInstant::Now() - start <= timeout) {
        if (predicate()) {
            return;
        }
        Sleep(delay);
        delay = Min(delay * 2, TDuration::Seconds(1));
    }
    UNIT_FAIL("Timeout waiting for " << description);
}

NActors::IActor* CreatePurecalcCompileServiceMock(NActors::TActorId owner, const TDuration& responseDelay) {
    return new TPurecalcCompileServiceMock(owner, responseDelay);
}

void CheckSuccess(const TStatus& status) {
    UNIT_ASSERT_C(status.IsSuccess(), "Status is not success, " << status.GetErrorMessage());
}

void CheckError(const TStatus& status, TStatusCode expectedStatusCode, const TString& expectedMessage) {
    UNIT_ASSERT_C(status.GetStatus() == expectedStatusCode, "Expected error status " << NYql::NDqProto::StatusIds_StatusCode_Name(expectedStatusCode) << ", but got: " << NYql::NDqProto::StatusIds_StatusCode_Name(status.GetStatus()) << " ("<< status.GetErrorMessage() << ")");
    UNIT_ASSERT_STRING_CONTAINS_C(status.GetErrorMessage(), expectedMessage, "Unexpected error message, Status: " << NYql::NDqProto::StatusIds_StatusCode_Name(status.GetStatus()));
}

}  // namespace NFq::NRowDispatcher::NTests
