#pragma once

#include "position_info.h"

#include <util/generic/string.h>

#include <stdexcept>
#include <string>

namespace NYsonPull {
    namespace NException {
        class TBadStream: public std::exception {
            TString Message_;
            TPositionInfo Position_;
            mutable TString FormattedMessage_;

        public:
            TBadStream(
                TString message,
                const TPositionInfo& position)
                : Message_(std::move(message))
                , Position_(position)
            {
            }

            const TPositionInfo& Position() const {
                return Position_;
            }

            const char* what() const noexcept override;
        };

        class TBadInput: public TBadStream {
        public:
            using TBadStream::TBadStream;
        };

        class TBadOutput: public TBadStream {
        public:
            using TBadStream::TBadStream;
        };

        class TSystemError: public std::exception {
            int SavedErrno_;

        public:
            TSystemError();
            TSystemError(int saved_errno)
                : SavedErrno_{saved_errno} {
            }

            int saved_errno() const noexcept {
                return SavedErrno_;
            }

            const char* what() const noexcept override;
        };
    }
}
