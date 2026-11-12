#include "session_id.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/uri/uri.h>

#include <util/generic/guid.h>
#include <util/string/cast.h>

#include <exception>

namespace NKikimr::NKqp {

namespace {

bool IsHexDigit(char value) {
    return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f') || (value >= 'A' && value <= 'F');
}

bool HasValidEscapes(TStringBuf value) {
    for (size_t i = 0; i < value.size(); ++i) {
        const auto ch = static_cast<unsigned char>(value[i]);
        if (ch <= ' ' || ch >= 127) {
            return false;
        }
        if (ch == '%') {
            if (i + 2 >= value.size() || !IsHexDigit(value[i + 1]) || !IsHexDigit(value[i + 2])) {
                return false;
            }
            i += 2;
        }
    }
    return true;
}

} // namespace

std::optional<ui32> ValidateSessionId(TStringBuf sessionId) {
    if (!HasValidEscapes(sessionId)) {
        return std::nullopt;
    }

    NUri::TUri uri;
    if (uri.Parse(sessionId, NUri::TFeature::FeaturesBare | NUri::TFeature::FeatureCheckHost) != NUri::TState::ParsedOK
        || uri.GetField(NUri::TField::FieldScheme) != "ydb"
        || uri.GetField(NUri::TField::FieldHost) != "session"
        || (uri.GetFieldMask() & (NUri::TField::FlagAuth | NUri::TField::FlagPort | NUri::TField::FlagFrag)))
    {
        return std::nullopt;
    }

    const auto path = uri.GetField(NUri::TField::FieldPath);
    ui32 kind = 0;
    if (path.empty() || path.front() != '/' || !TryFromString(path.SubStr(1), kind)
        || kind != static_cast<ui32>(Ydb::TOperationId::SESSION_YQL))
    {
        return std::nullopt;
    }

    const TCgiParameters parameters(uri.GetField(NUri::TField::FieldQuery));
    ui32 nodeId = 0;
    if (parameters.size() != 2 || parameters.NumOfValues("node_id") != 1 || parameters.NumOfValues("id") != 1
        || !TryFromString(parameters.Get("node_id"), nodeId) || nodeId == 0 || nodeId > NActors::TActorId::MaxNodeId)
    {
        return std::nullopt;
    }

    try {
        TGUID guid;
        if (!GetGuid(Base64StrictDecode(parameters.Get("id")), guid)) {
            return std::nullopt;
        }
    } catch (const std::exception&) {
        return std::nullopt;
    }

    return nodeId;
}

} // namespace NKikimr::NKqp
