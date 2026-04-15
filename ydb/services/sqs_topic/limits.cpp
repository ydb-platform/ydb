#include "limits.h"
#include <util/string/ascii.h>

namespace NKikimr::NSqsTopic::V1 {

    std::expected<void, std::string> ValidateQueueName(const TStringBuf name, bool allowAccessExistingTopic) {
        if (name.empty()) {
            return std::unexpected("empty");
        }
        const TStringBuf validCharacters = "-_."sv;
        const TStringBuf extraCharacters = allowAccessExistingTopic ? "/"sv : ""sv; // allow to use '/' to access existing topic
        for (char c : name) {
            if (!(IsAsciiAlnum(c) || validCharacters.contains(c) || extraCharacters.contains(c))) {
                return std::unexpected("invalid character");
            }
        }
        return {};
    }

} // namespace NKikimr::NSqsTopic::V1
