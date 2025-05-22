#pragma once

#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/certificate_check/cert_auth_utils.h>

namespace NKikimr {

struct TTicketParserSettings {
    NKikimrProto::TAuthConfig AuthConfig;
    TCertificateAuthValues CertificateAuthValues;
};

} // NKikimr
