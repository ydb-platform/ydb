#pragma once

#include "pqtablet_mock.h"

#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/writer/writer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <optional>

namespace NKikimr::NPersQueueTests {

struct TCreatePartitionWriterCacheActorParams {
    ui32 Partition = 0;
    std::optional<ui32> Generation;
    TString SourceId = "source_id";
    bool WithDeduplication = true;
    TString Database = "database";
};

struct TSendTxWriteRequestParams {
    TString SessionId;
    TString TxId;
    ui64 Cookie;
};

struct TEnsurePartitionWriterExistParams {
    TString SessionId;
    TString TxId;
};

struct TWaitForPartitionWriterOpsParams {
    TMaybe<size_t> CreateCount;
    TMaybe<size_t> DeleteCount;
};

class TPartitionWriterCacheActorFixture : public NUnitTest::TBaseFixture {
protected:
    using EErrorCode = NPQ::TEvPartitionWriter::TEvWriteResponse::EErrorCode;
    using TTxId = std::pair<TString, TString>;

    void SetUp(NUnitTest::TTestContext&) override;
    void TearDown(NUnitTest::TTestContext&) override;

    void SetupContext();
    void SetupKqpMock();
    void SetupPQTabletMock(ui64 tabletId);
    void SetupEventObserver();

    void CleanupContext();

    TActorId CreatePartitionWriterCacheActor(const TCreatePartitionWriterCacheActorParams& params = {});
    void SendTxWriteRequest(const TActorId& recipient, const TSendTxWriteRequestParams& params);

    void EnsurePartitionWriterExist(const TEnsurePartitionWriterExistParams& params);
    void EnsurePartitionWriterNotExist(const TEnsurePartitionWriterExistParams& params);
    void EnsureWriteSessionClosed(EErrorCode errorCode);

    void WaitForPartitionWriterOps(const TWaitForPartitionWriterOpsParams& params);

    void AdvanceCurrentTime(TDuration d);

    static TTxId MakeTxId(const TString& sessionId, const TString& txId);

    TMaybe<NPQ::TTestContext> Ctx;
    TMaybe<NPQ::TFinalizer> Finalizer;

    const ui64 PQTabletId = 12345;
    TPQTabletMock* PQTablet = nullptr;

    THashMap<ui64, TTxId> CookieToTxId;
    THashMap<TTxId, TActorId> TxIdToPartitionWriter;
    THashMap<TActorId, TTxId> PartitionWriterToTxId;

    size_t CreatePartitionWriterCount = 0;
    size_t DeletePartitionWriterCount = 0;

    ui64 NextWriteId = 1;
};

}
