#include "utils.h"

namespace NTvmAuth::NUtils {
    TStringBuf RemoveTicketSignature(TStringBuf ticketBody) {
        if (ticketBody.size() < 2 ||
            ticketBody[0] != '3' ||
            ticketBody[1] != ':') {
            return ticketBody;
        }

        size_t pos = ticketBody.rfind(':');
        if (pos == TStringBuf::npos) { // impossible
            return ticketBody;
        }

        return ticketBody.substr(0, pos + 1);
    }
}
