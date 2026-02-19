#include "limits.h"
#include <util/string/ascii.h>

namespace NKikimr::NSqsTopic::V1 {

    std::expected<void, std::string> ValidateQueueName(const TStringBuf name) {
        if (name.empty()) {
            return std::unexpected("empty");
        }
        for (char c : name) {
            if (!IsAsciiAlnum(c) && c != '-' && c != '_' && c != '.') {
                return std::unexpected("invalid character");
            }
        }
        return {};
    }

} // namespace NKikimr::NSqsTopic::V1
