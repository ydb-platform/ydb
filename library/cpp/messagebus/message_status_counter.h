#pragma once

#include "message_status.h"

#include <library/cpp/messagebus/monitoring/mon_proto.pb.h>

#include <util/generic/string.h>

#include <array>

namespace NBus {
    namespace NPrivate {
        struct TMessageStatusCounter {
            static TMessageStatusRecord::EMessageStatus MessageStatusToProtobuf(EMessageStatus status) {
                return (TMessageStatusRecord::EMessageStatus)status;
            }

            std::array<unsigned, MESSAGE_STATUS_COUNT> Counts;

            unsigned& operator[](EMessageStatus index) {
                return Counts[index];
            }
            const unsigned& operator[](EMessageStatus index) const {
                return Counts[index];
            }

            TMessageStatusCounter();

            TMessageStatusCounter& operator+=(const TMessageStatusCounter&);

            TString PrintToString() const;
            void FillErrorsProtobuf(TConnectionStatusMonRecord*) const;
        };

    }
}
