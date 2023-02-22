#include "private_events.h"

#include <util/string/join.h>

namespace NKikimr::NReplication::NController {

TEvPrivate::TEvDiscoveryTargetsResult::TEvDiscoveryTargetsResult(ui64 rid, TVector<TAddEntry>&& toAdd, TVector<ui64>&& toDel)
    : ReplicationId(rid)
    , ToAdd(std::move(toAdd))
    , ToDelete(std::move(toDel))
{
}

TEvPrivate::TEvDiscoveryTargetsResult::TEvDiscoveryTargetsResult(ui64 rid, TVector<TFailedEntry>&& failed)
    : ReplicationId(rid)
    , Failed(std::move(failed))
{
}

TString TEvPrivate::TEvDiscoveryTargetsResult::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " ReplicationId: " << ReplicationId
        << " ToAdd [" << JoinSeq(",", ToAdd) << "]"
        << " ToDelete [" << JoinSeq(",", ToDelete) << "]"
        << " Failed [" << JoinSeq(",", Failed) << "]"
    << " }";
}

bool TEvPrivate::TEvDiscoveryTargetsResult::IsSuccess() const {
    return Failed.empty();
}

TEvPrivate::TEvAssignStreamName::TEvAssignStreamName(ui64 rid, ui64 tid)
    : ReplicationId(rid)
    , TargetId(tid)
{
}

TString TEvPrivate::TEvAssignStreamName::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " ReplicationId: " << ReplicationId
        << " TargetId: " << TargetId
    << " }";
}

TEvPrivate::TEvCreateStreamResult::TEvCreateStreamResult(ui64 rid, ui64 tid, NYdb::TStatus&& status)
    : ReplicationId(rid)
    , TargetId(tid)
    , Status(std::move(status))
{
}

TString TEvPrivate::TEvCreateStreamResult::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " ReplicationId: " << ReplicationId
        << " TargetId: " << TargetId
        << " Status: " << Status.GetStatus()
        << " Issues: " << Status.GetIssues().ToOneLineString()
    << " }";
}

bool TEvPrivate::TEvCreateStreamResult::IsSuccess() const {
    return Status.IsSuccess();
}

TEvPrivate::TEvCreateDstResult::TEvCreateDstResult(ui64 rid, ui64 tid, const TPathId& dstPathId)
    : ReplicationId(rid)
    , TargetId(tid)
    , DstPathId(dstPathId)
    , Status(NKikimrScheme::StatusSuccess)
{
}

TEvPrivate::TEvCreateDstResult::TEvCreateDstResult(ui64 rid, ui64 tid, NKikimrScheme::EStatus status, const TString& error)
    : ReplicationId(rid)
    , TargetId(tid)
    , Status(status)
    , Error(error)
{
}

TString TEvPrivate::TEvCreateDstResult::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " ReplicationId: " << ReplicationId
        << " TargetId: " << TargetId
        << " DstPathId: " << DstPathId
        << " Status: " << NKikimrScheme::EStatus_Name(Status)
        << " Error: " << Error
    << " }";
}

bool TEvPrivate::TEvCreateDstResult::IsSuccess() const {
    return Status == NKikimrScheme::StatusSuccess;
}

TEvPrivate::TEvDropStreamResult::TEvDropStreamResult(ui64 rid, ui64 tid, NYdb::TStatus&& status)
    : ReplicationId(rid)
    , TargetId(tid)
    , Status(std::move(status))
{
}

TString TEvPrivate::TEvDropStreamResult::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " ReplicationId: " << ReplicationId
        << " TargetId: " << TargetId
        << " Status: " << Status.GetStatus()
        << " Issues: " << Status.GetIssues().ToOneLineString()
    << " }";
}

bool TEvPrivate::TEvDropStreamResult::IsSuccess() const {
    return Status.IsSuccess();
}

TEvPrivate::TEvDropDstResult::TEvDropDstResult(ui64 rid, ui64 tid, NKikimrScheme::EStatus status, const TString& error)
    : ReplicationId(rid)
    , TargetId(tid)
    , Status(status)
    , Error(error)
{
}

TString TEvPrivate::TEvDropDstResult::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " ReplicationId: " << ReplicationId
        << " TargetId: " << TargetId
        << " Status: " << NKikimrScheme::EStatus_Name(Status)
        << " Error: " << Error
    << " }";
}

bool TEvPrivate::TEvDropDstResult::IsSuccess() const {
    return Status == NKikimrScheme::StatusSuccess;
}

TEvPrivate::TEvDropReplication::TEvDropReplication(ui64 rid)
    : ReplicationId(rid)
{
}

TString TEvPrivate::TEvDropReplication::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " ReplicationId: " << ReplicationId
    << " }";
}

TEvPrivate::TEvResolveTenantResult::TEvResolveTenantResult(ui64 rid, const TString& tenant)
    : ReplicationId(rid)
    , Tenant(tenant)
    , Success(true)
{
}

TEvPrivate::TEvResolveTenantResult::TEvResolveTenantResult(ui64 rid, bool success)
    : ReplicationId(rid)
    , Success(success)
{
}

TString TEvPrivate::TEvResolveTenantResult::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " ReplicationId: " << ReplicationId
        << " Tenant: " << Tenant
        << " Sucess: " << Success
    << " }";
}

bool TEvPrivate::TEvResolveTenantResult::IsSuccess() const {
    return Success;
}

TEvPrivate::TEvUpdateTenantNodes::TEvUpdateTenantNodes(const TString& tenant)
    : Tenant(tenant)
{
}

TString TEvPrivate::TEvUpdateTenantNodes::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Tenant: " << Tenant
    << " }";
}

}

Y_DECLARE_OUT_SPEC(, NKikimr::NReplication::NController::TEvPrivate::TEvDiscoveryTargetsResult::TAddEntry, stream, value) {
    stream << value.first.Name << " (" << value.first.Type << ")";
}

Y_DECLARE_OUT_SPEC(, NKikimr::NReplication::NController::TEvPrivate::TEvDiscoveryTargetsResult::TFailedEntry, stream, value) {
    stream << value.first << ": " << value.second.GetStatus() << " (";
    value.second.GetIssues().PrintTo(stream, true);
    stream << ")";
}
