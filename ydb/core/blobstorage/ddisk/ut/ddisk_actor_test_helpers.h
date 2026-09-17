#pragma once

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDDisk::NTesting {

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
