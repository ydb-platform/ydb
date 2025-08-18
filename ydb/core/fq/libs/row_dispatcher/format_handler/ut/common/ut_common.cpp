#include "ut_common.h"

#include <ydb/core/base/backtrace.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/core/testlib/basics/appdata.h>

#include <ydb/library/yql/dq/common/rope_over_buffer.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/public/purecalc/common/interface.h>

namespace NFq::NRowDispatcher::NTests {

namespace {

class TPurecalcCompileServiceMock : public NActors::TActor<TPurecalcCompileServiceMock> {
    using TBase = NActors::TActor<TPurecalcCompileServiceMock>;

public:
    TPurecalcCompileServiceMock(NActors::TActorId owner)
        : TBase(&TPurecalcCompileServiceMock::StateFunc)
        , Owner(owner)
        , ProgramFactory(NYql::NPureCalc::MakeProgramFactory())
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRowDispatcher::TEvPurecalcCompileRequest, Handle);
    )

    void Handle(TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr& ev) {
        IProgramHolder::TPtr programHolder = std::move(ev->Get()->ProgramHolder);

        try {
            programHolder->CreateProgram(ProgramFactory);
        } catch (const NYql::NPureCalc::TCompileError& e) {
            UNIT_FAIL("Failed to compile purecalc filter: sql: " << e.GetYql() << ", error: " << e.GetIssues());
        }

        Send(ev->Sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(std::move(programHolder)), 0, ev->Cookie);
        Send(Owner, new NActors::TEvents::TEvPing());
    }

private:
    const NActors::TActorId Owner;
    const NYql::NPureCalc::IProgramFactoryPtr ProgramFactory;
};

void SegmentationFaultHandler(int) {
    Cerr << "segmentation fault call stack:" << Endl;
    FormatBackTrace(&Cerr);
    abort();
}

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

TBaseFixture::TBaseFixture()
    : TTypeParser(__LOCATION__, {})
    , MemoryInfo("TBaseFixture alloc")
    , HolderFactory(std::make_unique<NKikimr::NMiniKQL::THolderFactory>(Alloc.Ref(), MemoryInfo))
    , Runtime(1, true)
{
    NKikimr::EnableYDBBacktraceFormat();
    signal(SIGSEGV, &SegmentationFaultHandler);

    Alloc.Ref().UseRefLocking = true;
}

void TBaseFixture::SetUp(NUnitTest::TTestContext&) {
    // Init runtime
    TAutoPtr<NKikimr::TAppPrepare> app = new NKikimr::TAppPrepare();
    Runtime.SetLogBackend(NActors::CreateStderrBackend());
    Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NActors::NLog::PRI_TRACE);
    Runtime.SetDispatchTimeout(WAIT_TIMEOUT);
    Runtime.Initialize(app->Unwrap());

    // Init tls context
    auto* actorSystem = Runtime.GetActorSystem(0);
    Mailbox = std::make_unique<NActors::TMailbox>();
    ExecutorThread = std::make_unique<NActors::TExecutorThread>(0, actorSystem, nullptr, "test thread");
    ActorCtx = std::make_unique<NActors::TActorContext>(*Mailbox, *ExecutorThread, GetCycleCountFast(), NActors::TActorId());
    PrevActorCtx = NActors::TlsActivationContext;
    NActors::TlsActivationContext = ActorCtx.get();
}

void TBaseFixture::TearDown(NUnitTest::TTestContext&) {
    with_lock(Alloc) {
        ProgramBuilder.reset();
        TypeEnv.reset();
        FunctionRegistry.Reset();
        HolderFactory.reset();
    }

    // Release tls context
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
        const auto dataUnpacker = std::make_unique<NKikimr::NMiniKQL::TValuePackerTransport<true>>(rowType, NKikimr::NMiniKQL::EValuePackerVersion::V0);

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

NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage TBaseFixture::GetMessage(ui64 offset, const TString& data) {
    NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation info(offset, "", 0, TInstant::Zero(), TInstant::Zero(), nullptr, nullptr, 0, "");
    return NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage(data, nullptr, info, nullptr);
}

//// Functions

NActors::IActor* CreatePurecalcCompileServiceMock(NActors::TActorId owner) {
    return new TPurecalcCompileServiceMock(owner);
}

void CheckSuccess(const TStatus& status) {
    UNIT_ASSERT_C(status.IsSuccess(), "Status is not success, " << status.GetErrorMessage());
}

void CheckError(const TStatus& status, TStatusCode expectedStatusCode, const TString& expectedMessage) {
    UNIT_ASSERT_C(status.GetStatus() == expectedStatusCode, "Expected error status " << NYql::NDqProto::StatusIds_StatusCode_Name(expectedStatusCode) << ", but got: " << status.GetErrorMessage());
    UNIT_ASSERT_STRING_CONTAINS_C(status.GetErrorMessage(), expectedMessage, "Unexpected error message, Status: " << NYql::NDqProto::StatusIds_StatusCode_Name(status.GetStatus()));
}

}  // namespace NFq::NRowDispatcher::NTests
