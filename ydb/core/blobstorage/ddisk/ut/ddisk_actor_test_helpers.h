#pragma once

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDDisk::NTesting {

// Older lifecycle tests wait only for client replies or Gone, leaving their
// strict PDisk edge unarmed. Consume the new shutdown request without replying.
// Tests of release contents install their own observer instead.
template<typename TRuntime>
void IgnoreShutdownChunkForget(TRuntime& runtime) {
    auto previous = std::move(runtime.FilterFunction);
    runtime.FilterFunction = [previous = std::move(previous)](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == NPDisk::TEvChunkForget::EventType) {
            return false;
        }
        return !previous || previous(nodeId, ev);
    };
}

// The actor suites have separate contexts; their send/wait helpers are found by ADL.
template<typename TTestContext>
ui64 GetRegistrationToken(TTestContext& ctx, const TActorId& serviceId, const TQueryCredentials& creds) {
    SendToDDisk(ctx, serviceId, new TEvGetPersistentBufferRegistrationToken(creds));
    auto result = WaitFromDDisk<TEvGetPersistentBufferRegistrationTokenResult>(ctx);
    UNIT_ASSERT_C(result->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
        result->Get()->Record.GetErrorReason());
    UNIT_ASSERT(result->Get()->Record.GetToken() != 0);
    return result->Get()->Record.GetToken();
}

} // namespace NKikimr::NDDisk::NTesting
