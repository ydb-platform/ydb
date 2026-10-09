#pragma once

#include "ut_helpers.h"

namespace NKikimr::NIamDelegation::NTests {

namespace NProto = NKikimrIamDelegation;

inline NProto::TDatabaseIdentity DatabaseIdentity(
    const TString& incarnation = "database-incarnation-1",
    const TString& path = "/Root/database",
    const TString& databaseId = "database-id-1")
{
    NProto::TDatabaseIdentity identity;
    identity.SetIncarnation(incarnation);
    identity.SetPath(path);
    identity.SetDatabaseId(databaseId);
    return identity;
}

inline NProto::TSecretIdentity SecretIdentity(ui64 localId = 42,
    const TString& incarnation = "database-incarnation-1")
{
    NProto::TSecretIdentity identity;
    identity.SetDatabaseIncarnation(incarnation);
    identity.SetPathOwnerId(100);
    identity.SetPathLocalId(localId);
    return identity;
}

inline NProto::TIamBinding Binding(const TString& referrerId = "ydb.delegation.first") {
    NProto::TIamBinding binding;
    binding.SetServiceAccountId("service-account-1");
    binding.SetCloudId("cloud-1");
    binding.SetResourceType("resource-manager.cloud");
    binding.SetReferrerId(referrerId);
    binding.SetReferrerType("ydb.secret");
    binding.SetServiceId("ydb");
    binding.SetMicroserviceId("data-plane");
    binding.SetReferencePolicy(NProto::WITHOUT_REFERENCES);
    return binding;
}

inline NProto::TRequest RegisterRequest(const NProto::TDatabaseIdentity& identity) {
    NProto::TRequest request;
    request.MutableRegisterDatabase()->MutableIdentity()->CopyFrom(identity);
    return request;
}

inline NProto::TRequest StageCreateRequest(const TString& operationId = "create-1",
    const NProto::TIamBinding& binding = Binding(),
    const TString& secretPath = "/Root/database/secret",
    const TString& incarnation = "database-incarnation-1",
    ui64 createTxId = 10)
{
    NProto::TRequest request;
    auto* stage = request.MutableStage();
    stage->SetMode(NProto::CREATE);
    stage->SetOperationId(operationId);
    stage->SetDatabaseIncarnation(incarnation);
    stage->SetSecretPath(secretPath);
    stage->SetCreateTxId(createTxId);
    stage->MutableBinding()->CopyFrom(binding);
    return request;
}

inline NProto::TRequest StageAlterRequest(const NProto::TSecretRecord& secret,
    const TString& operationId, const NProto::TIamBinding& binding)
{
    NProto::TRequest request;
    auto* stage = request.MutableStage();
    stage->SetMode(NProto::ALTER);
    stage->SetOperationId(operationId);
    stage->SetDatabaseIncarnation(secret.GetIdentity().GetDatabaseIncarnation());
    stage->SetSecretPath(secret.GetSecretPath());
    stage->SetCreateTxId(secret.GetCreateTxId());
    stage->MutableSecret()->CopyFrom(secret.GetIdentity());
    stage->SetExpectedSecretRevision(secret.GetRevision());
    stage->MutableBinding()->CopyFrom(binding);
    return request;
}

inline NProto::TRequest BindRequest(const NProto::TDelegation& delegation,
    const NProto::TSecretIdentity& secret = SecretIdentity())
{
    NProto::TRequest request;
    auto* bind = request.MutableBindSecret();
    bind->SetOperationId(delegation.GetOperationId());
    bind->SetExpectedRevision(delegation.GetRevision());
    bind->MutableSecret()->CopyFrom(secret);
    return request;
}

inline NProto::TRequest StartRequest(const NProto::TDelegation& delegation) {
    NProto::TRequest request;
    auto* start = request.MutableStartSetup();
    start->SetOperationId(delegation.GetOperationId());
    start->SetExpectedRevision(delegation.GetRevision());
    return request;
}

inline NProto::TRequest SetupResultRequest(const NProto::TDelegation& delegation,
    NProto::ESetupState outcome, const TString& operationId = "iam-setup-1")
{
    NProto::TRequest request;
    auto* result = request.MutableSetSetupResult();
    result->SetOperationId(delegation.GetOperationId());
    result->SetExpectedRevision(delegation.GetRevision());
    result->SetOutcome(outcome);
    result->SetSetupOperationId(operationId);
    return request;
}

inline NProto::TRequest PromoteRequest(const NProto::TDelegation& delegation, ui64 secretRevision) {
    NProto::TRequest request;
    auto* promote = request.MutablePromote();
    promote->SetOperationId(delegation.GetOperationId());
    promote->SetExpectedRevision(delegation.GetRevision());
    promote->SetExpectedSecretRevision(secretRevision);
    return request;
}

inline NProto::TRequest DropRequest(const NProto::TSecretRecord& secret) {
    NProto::TRequest request;
    auto* drop = request.MutableDropSecret();
    drop->MutableSecret()->CopyFrom(secret.GetIdentity());
    drop->SetExpectedRevision(secret.GetRevision());
    return request;
}

inline NProto::TRequest DropRequest(const NProto::TDelegation& delegation) {
    NProto::TRequest request;
    auto* drop = request.MutableDropSecret();
    drop->SetOperationId(delegation.GetOperationId());
    drop->SetExpectedRevision(delegation.GetRevision());
    return request;
}

inline NProto::TDelegation GetDelegation(TTestContext& ctx, const TString& operationId) {
    NProto::TRequest request;
    request.MutableGetDelegation()->SetOperationId(operationId);
    return ctx.Call(std::move(request)).GetDelegation();
}

inline NProto::TSecretRecord GetSecret(TTestContext& ctx, const NProto::TSecretIdentity& identity = SecretIdentity()) {
    NProto::TRequest request;
    request.MutableGetSecret()->MutableSecret()->CopyFrom(identity);
    return ctx.Call(std::move(request)).GetSecret();
}

inline NProto::TRequest ListRequest(const TString& incarnation = "database-incarnation-1", ui32 pageSize = 100) {
    NProto::TRequest request;
    auto* list = request.MutableListInventory();
    list->SetDatabaseIncarnation(incarnation);
    list->SetPageSize(pageSize);
    return request;
}

inline NProto::TRequest ClaimRequest(const TString& workerId = "worker-1", ui32 limit = 100) {
    NProto::TRequest request;
    auto* claim = request.MutableClaimRevocations();
    claim->SetWorkerId(workerId);
    claim->SetLimit(limit);
    claim->SetLeaseUs(TDuration::Seconds(10).MicroSeconds());
    return request;
}

inline NProto::TRequest FinishRequest(const NProto::TDelegation& delegation,
    NProto::ERevokeOutcome outcome = NProto::REVOKE_SUCCEEDED,
    TDuration retryAfter = TDuration::Zero())
{
    NProto::TRequest request;
    auto* finish = request.MutableFinishRevocation();
    finish->SetOperationId(delegation.GetOperationId());
    finish->MutableClaim()->CopyFrom(delegation.GetClaim());
    finish->SetOutcome(outcome);
    finish->SetRevokeOperationId("iam-revoke-1");
    finish->SetRetryAfterUs(retryAfter.MicroSeconds());
    return request;
}

inline NProto::TResponse CompleteSetup(TTestContext& ctx, const NProto::TDelegation& delegation) {
    const auto started = ctx.Call(StartRequest(delegation));
    return ctx.Call(SetupResultRequest(started.GetDelegation(), NProto::SETUP_SUCCEEDED));
}

inline NProto::TResponse CreateActive(TTestContext& ctx) {
    ctx.Call(RegisterRequest(DatabaseIdentity()));
    const auto staged = ctx.Call(StageCreateRequest());
    const auto bound = ctx.Call(BindRequest(staged.GetDelegation()));
    const auto setup = CompleteSetup(ctx, bound.GetDelegation());
    return ctx.Call(PromoteRequest(setup.GetDelegation(), GetSecret(ctx).GetRevision()));
}

inline void AssertBinding(const NProto::TDelegation& delegation, const NProto::TIamBinding& expected) {
    UNIT_ASSERT_VALUES_EQUAL(delegation.GetBinding().SerializeAsString(), expected.SerializeAsString());
}

} // namespace NKikimr::NIamDelegation::NTests
