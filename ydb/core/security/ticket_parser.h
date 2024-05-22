#pragma once
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/base/ticket_parser.h>

namespace NKikimr {
    IActor* CreateTicketParser(const NKikimrProto::TAuthConfig& authConfig, const NKikimrConfig::TClientCertificateAuthorization& clientCertificateAuth = {});
}
