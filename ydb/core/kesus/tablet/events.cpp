#include "events.h"

namespace NKikimr::NKesus::TEvKesus {

TEvSetConfig::TEvSetConfig(ui64 txId, const Ydb::Coordination::Config& config, ui64 version) {
    Record.SetTxId(txId);
    Record.MutableConfig()->CopyFrom(config);
    Record.SetVersion(version);
}

TEvSetConfigResult::TEvSetConfigResult(ui64 txId, ui64 tabletId) {
    Record.SetTxId(txId);
    Record.SetTabletId(tabletId);
    Record.MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
}

TEvDescribeSemaphore::TEvDescribeSemaphore(const TString& kesusPath, const TString& name) {
    Record.SetKesusPath(kesusPath);
    Record.SetName(name);
}

TEvCreateSemaphore::TEvCreateSemaphore(const TString& kesusPath, const TString& name, ui64 limit, const TString& data) {
    Record.SetKesusPath(kesusPath);
    Record.SetName(name);
    Record.SetLimit(limit);
    Record.SetData(data);
}

TEvUpdateSemaphore::TEvUpdateSemaphore(const TString& kesusPath, const TString& name, const TString& data) {
    Record.SetKesusPath(kesusPath);
    Record.SetName(name);
    Record.SetData(data);
}

TEvDeleteSemaphore::TEvDeleteSemaphore(const TString& kesusPath, const TString& name, bool force) {
    Record.SetKesusPath(kesusPath);
    Record.SetName(name);
    Record.SetForce(force);
}

TEvRegisterProxy::TEvRegisterProxy(const TString& kesusPath, ui64 generation) {
    Record.SetKesusPath(kesusPath);
    Record.SetProxyGeneration(generation);
}

TEvUnregisterProxy::TEvUnregisterProxy(const TString& kesusPath, ui64 generation) {
    Record.SetKesusPath(kesusPath);
    Record.SetProxyGeneration(generation);
}

TEvAttachSession::TEvAttachSession(
        const TString& kesusPath,
        ui64 generation,
        ui64 sessionId,
        ui64 timeoutMillis,
        const TString& description,
        ui64 seqNo,
        const TString& protectionKey)
{
    Record.SetKesusPath(kesusPath);
    Record.SetProxyGeneration(generation);
    Record.SetSessionId(sessionId);
    Record.SetTimeoutMillis(timeoutMillis);
    Record.SetDescription(description);
    Record.SetSeqNo(seqNo);
    Record.SetProtectionKey(protectionKey);
}

TEvDetachSession::TEvDetachSession(const TString& kesusPath, ui64 generation, ui64 sessionId) {
    Record.SetKesusPath(kesusPath);
    Record.SetProxyGeneration(generation);
    Record.SetSessionId(sessionId);
}

TEvDestroySession::TEvDestroySession(const TString& kesusPath, ui64 generation, ui64 sessionId) {
    Record.SetKesusPath(kesusPath);
    Record.SetProxyGeneration(generation);
    Record.SetSessionId(sessionId);
}

TEvAcquireSemaphore::TEvAcquireSemaphore(
        const TString& kesusPath, ui64 generation, ui64 sessionId, const TString& name, ui64 count,
        ui64 timeoutMillis, const TString& data, bool ephemeral)
{
    Record.SetKesusPath(kesusPath);
    Record.SetProxyGeneration(generation);
    Record.SetSessionId(sessionId);
    Record.SetName(name);
    Record.SetCount(count);
    Record.SetTimeoutMillis(timeoutMillis);
    Record.SetData(data);
    Record.SetEphemeral(ephemeral);
}

TEvReleaseSemaphore::TEvReleaseSemaphore(const TString& kesusPath, ui64 generation, ui64 sessionId, const TString& name) {
    Record.SetKesusPath(kesusPath);
    Record.SetProxyGeneration(generation);
    Record.SetSessionId(sessionId);
    Record.SetName(name);
}

TEvSessionStolen::TEvSessionStolen(ui64 generation, ui64 sessionId) {
    Record.SetProxyGeneration(generation);
    Record.SetSessionId(sessionId);
}

TEvDescribeSemaphoreChanged::TEvDescribeSemaphoreChanged(ui64 generation, bool dataChanged, bool ownersChanged) {
    Record.SetProxyGeneration(generation);
    Record.SetDataChanged(dataChanged);
    Record.SetOwnersChanged(ownersChanged);
}

} // namespace NKikimr::NKesus::TEvKesus
