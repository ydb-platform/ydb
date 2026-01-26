#include "error.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>

#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

void FormatResultCodeRaw(IOutputStream& out, ui32 code)
{
    out << static_cast<ESeverityCode>(SUCCEEDED(code) ? 0 : 1) << " | ";

    ui32 facility = FACILITY_FROM_CODE(code);
    if (facility < FACILITY_MAX) {
        out << static_cast<EFacilityCode>(facility);
    } else {
        out << "FACILITY_UNKNOWN";
    }

    out << " | " << STATUS_FROM_CODE(code);
}

void FormatResultCodePretty(IOutputStream& out, ui32 code)
{
    // TODO
    try {
        out << static_cast<EWellKnownResultCodes>(code);
    } catch (...) {
        FormatResultCodeRaw(out, code);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

EErrorKind GetErrorKind(const NProto::TError& e)
{
    const ui32 code = e.GetCode();

    if (SUCCEEDED(code)) {
        return EErrorKind::Success;
    }

    switch (code) {
        case E_REJECTED:
        case E_TIMEOUT:
        case E_BS_OUT_OF_SPACE:
        case E_FS_OUT_OF_SPACE:
        case E_BS_THROTTLED:
        case E_FS_THROTTLED:
        case E_RDMA_UNAVAILABLE:
            return EErrorKind::ErrorRetriable;

        case E_BS_INVALID_SESSION:
        case E_FS_INVALID_SESSION:
            return EErrorKind::ErrorSession;
        case E_ABORTED:
            return EErrorKind::ErrorAborted;
    }

    if (FACILITY_FROM_CODE(code) == FACILITY_GRPC) {
        if (code == E_GRPC_UNIMPLEMENTED) {
            return EErrorKind::ErrorFatal;
        }
        return EErrorKind::ErrorRetriable;
    }

    // FACILITY_SYSTEM error codes are obtained from "errno". The list of all
    // possible errors: contrib/libs/linux-headers/asm-generic/errno-base.h
    if (FACILITY_FROM_CODE(code) == FACILITY_SYSTEM) {
        switch (STATUS_FROM_CODE(code)) {
            case EIO:
            case ENODATA:
                return EErrorKind::ErrorFatal;
            default:
                // system/network errors should be retriable
                return EErrorKind::ErrorRetriable;
        }
    }

    if (FACILITY_FROM_CODE(code) == FACILITY_KIKIMR) {
        switch (STATUS_FROM_CODE(code)) {
            case 1:  // NKikimrProto::ERROR
            case 3:  // NKikimrProto::TIMEOUT
            case 4:  // NKikimrProto::RACE
            case 6:  // NKikimrProto::BLOCKED
            case 7:  // NKikimrProto::NOTREADY
            case 12: // NKikimrProto::DEADLINE
            case 20: // NKikimrProto::NOT_YET
                return EErrorKind::ErrorRetriable;
        }
    }

    if (FACILITY_FROM_CODE(code) == FACILITY_SCHEMESHARD) {
        switch (STATUS_FROM_CODE(code)) {
            case 13: // NKikimrScheme::StatusNotAvailable
            case 8:  // NKikimrScheme::StatusMultipleModifications
                return EErrorKind::ErrorRetriable;
        }
    }

    if (FACILITY_FROM_CODE(code) == FACILITY_TXPROXY) {
        switch (STATUS_FROM_CODE(code)) {
            case 16: // NKikimr::NTxProxy::TResultStatus::ProxyNotReady
            case 20: // NKikimr::NTxProxy::TResultStatus::ProxyShardNotAvailable
            case 21: // NKikimr::NTxProxy::TResultStatus::ProxyShardTryLater
            case 22: // NKikimr::NTxProxy::TResultStatus::ProxyShardOverloaded
            case 51: // NKikimr::NTxProxy::TResultStatus::ExecTimeout:
            case 55: // NKikimr::NTxProxy::TResultStatus::ExecResultUnavailable:
                return EErrorKind::ErrorRetriable;
        }
    }

    // any other errors should not be retried automatically
    return EErrorKind::ErrorFatal;
}

EDiagnosticsErrorKind GetDiagnosticsErrorKind(const NProto::TError& e)
{
    const ui32 code = e.GetCode();

    if (SUCCEEDED(code)) {
        return EDiagnosticsErrorKind::Success;
    }

    // TODO: do not retrieve E_THROTTLED from error message
    // on client side: NBS-568
    if (code == E_REJECTED && e.GetMessage() == "Throttled") {
        return EDiagnosticsErrorKind::ErrorThrottling;
    }

    // NBS-4447
    if (code == E_REJECTED &&
        e.GetMessage().StartsWith("Checkpoint reject request."))
    {
        return EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint;
    }

    if (HasProtoFlag(e.GetFlags(), NProto::EF_SILENT)
        || code == E_IO_SILENT) // TODO: NBS-3124#622886b937bf95501db66aad
    {
        return EDiagnosticsErrorKind::ErrorSilent;
    }

    switch (code) {
        case E_BS_THROTTLED:
        case E_FS_THROTTLED:
            return EDiagnosticsErrorKind::ErrorThrottling;

        case E_FS_INVALID_SESSION:
        case E_BS_INVALID_SESSION:
            return EDiagnosticsErrorKind::ErrorSession;
        case E_ABORTED:
            return EDiagnosticsErrorKind::ErrorAborted;
    }

    if (GetErrorKind(e) == EErrorKind::ErrorRetriable) {
        return EDiagnosticsErrorKind::ErrorRetriable;
    }

    if (FACILITY_FROM_CODE(code) == FACILITY_FILESTORE) {
        return EDiagnosticsErrorKind::ErrorSilent;
    }

    // any other errors should not be retried automatically
    return EDiagnosticsErrorKind::ErrorFatal;
}

bool IsConnectionError(const NProto::TError& e)
{
    return e.GetCode() == E_GRPC_UNAVAILABLE;
}

NJson::TJsonValue FormatErrorJson(const NProto::TError& e)
{
    NJson::TJsonValue result;
    if (e.GetCode()) {
        result["Code"] = e.GetCode();
        TStringStream stream;
        FormatResultCodePretty(stream, e.GetCode());
        result["CodeString"] = stream.Str();
    }

    if (e.GetMessage()){
        TStringStream stream;
        ::google::protobuf::io::PrintJSONString(stream, e.GetMessage());
        result["Message"] = stream.Str();
    }

    if (e.GetFlags()){
        result["Flags"] = e.GetFlags();
    }

    return result;
}

TString FormatError(const NProto::TError& e)
{
    TStringStream out;
    FormatResultCodePretty(out, e.GetCode());

    if (const auto& s = e.GetMessage()) {
        out << " " << s;
    }
    auto flags = e.GetFlags();
    ui32 flag = 1;
    while (flags) {
        if (flags & 1) {
            switch (static_cast<NProto::EErrorFlag>(flag)) {
                case NProto::EF_SILENT:
                    out << " f@silent";
                    break;
                case NProto::EF_HW_PROBLEMS_DETECTED:
                    out << " f@hw_problems";
                    break;
                case NProto::EF_INSTANT_RETRIABLE:
                    out << " f@instant_retry";
                    break;
                case NProto::EF_NONE:
                case NProto::EErrorFlag_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NProto::EErrorFlag_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_DEBUG_ABORT_UNLESS(false);
                    break;
            }
        }

        flags >>= 1;
        ++flag;
    }
    return out.Str();
}

TString FormatResultCode(ui32 code)
{
    TStringStream out;
    FormatResultCodePretty(out, code);

    return out.Str();
}

NProto::TError MakeError(ui32 code, TString message, ui32 flags)
{
    NProto::TError error;
    error.SetCode(code);
    error.SetFlags(flags);

    if (message) {
        error.SetMessage(std::move(message));
    }

    return error;
}

NProto::TError MakeTabletIsDeadError(ui32 code, const TSourceLocation& location)
{
    TStringStream out;
    out << "Tablet is dead: " << location.File << ":" << location.Line;
    return MakeError(code, out.Str());
}

}   // namespace NYdb::NBS

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYdb::NBS::TServiceError>(
    IOutputStream& out,
    const NYdb::NBS::TServiceError& e)
{
    NYdb::NBS::FormatResultCodePretty(out, e.GetCode());

    if (const auto& s = e.GetMessage()) {
        out << " " << s;
    }
}
