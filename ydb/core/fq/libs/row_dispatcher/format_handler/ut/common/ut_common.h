#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/fq/libs/row_dispatcher/format_handler/parsers/parser_base.h>

#include <ydb/core/testlib/actors/test_runtime.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NFq::NRowDispatcher::NTests {

static constexpr TDuration WAIT_TIMEOUT = TDuration::Seconds(20);

class TBaseFixture : public NUnitTest::TBaseFixture, public TTypeParser {
public:
    // Helper classes for checking serialized rows in multi type format
    class ICell : public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<ICell>;

    public:
        virtual TString GetType() const = 0;
        virtual void Validate(const NYql::NUdf::TUnboxedValue& parsedValue) const = 0;
    };

    class TRow {
    public:
        TRow() = default;

        TRow& AddCell(ICell::TPtr cell, bool optional);

        TRow& AddString(const TString& value, bool optional = false);
        TRow& AddUint64(ui64 value, bool optional = false);

    public:
        TVector<ICell::TPtr> Cells;
    };

    class TBatch {
    public:
        TBatch() = default;
        TBatch(std::initializer_list<TRow> rows);

        TBatch& AddRow(TRow row);

    public:
        TVector<TRow> Rows;
    };

public:
    TBaseFixture();

public:
    virtual void SetUp(NUnitTest::TTestContext& ctx) override;
    virtual void TearDown(NUnitTest::TTestContext& ctx) override;

public:
    void CheckMessageBatch(TRope serializedBatch, const TBatch& expectedBatch) const;

    static NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage GetMessage(ui64 offset, const TString& data);

public:
    NKikimr::NMiniKQL::TMemoryUsageInfo MemoryInfo;
    std::unique_ptr<NKikimr::NMiniKQL::THolderFactory> HolderFactory;
    NActors::TTestActorRuntime Runtime;

private:
    // Like NKikimr::TActorSystemStub but with Runtime as actor system in tls context
    // it enables logging in unit test thread
    // and using NActors::TActivationContext::ActorSystem() method
    std::unique_ptr<NActors::TMailbox> Mailbox;
    std::unique_ptr<NActors::TExecutorThread> ExecutorThread;
    std::unique_ptr<NActors::TActorContext> ActorCtx;
    NActors::TActivationContext* PrevActorCtx;
};

NActors::IActor* CreatePurecalcCompileServiceMock(NActors::TActorId owner);

void CheckSuccess(const TStatus& status);
void CheckError(const TStatus& status, TStatusCode expectedStatusCode, const TString& expectedMessage);

template <typename TValue>
TValue CheckSuccess(TValueStatus<TValue> valueStatus) {
    UNIT_ASSERT_C(valueStatus.IsSuccess(), "Value status is not success, " << valueStatus.GetErrorMessage());
    return valueStatus.DetachResult();
}

}  // namespace NFq::NRowDispatcher::NTests
