#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/base/ticket_parser.h>

namespace NKikimr {
    IActor* CreateTicketParser(const NKikimrProto::TAuthConfig& authConfig);
}

