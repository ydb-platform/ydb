#pragma once
#include <ydb/core/base/ticket_parser.h>
#include "ticket_parser_settings.h"

namespace NKikimr {
    IActor* CreateTicketParser(const TTicketParserSettings& settings);
}
