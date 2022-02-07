#pragma once

#include <util/generic/string.h>

namespace NActors {

    class TDisconnectReason {
        TString Text;

    private:
        explicit TDisconnectReason(TString text)
            : Text(std::move(text))
        {}

    public:
        TDisconnectReason() = default;
        TDisconnectReason(const TDisconnectReason&) = default;
        TDisconnectReason(TDisconnectReason&&) = default;

        static TDisconnectReason FromErrno(int err);

        static TDisconnectReason EndOfStream()            { return TDisconnectReason("EndOfStream"); }
        static TDisconnectReason CloseOnIdle()            { return TDisconnectReason("CloseOnIdle"); }
        static TDisconnectReason LostConnection()         { return TDisconnectReason("LostConnection"); }
        static TDisconnectReason DeadPeer()               { return TDisconnectReason("DeadPeer"); }
        static TDisconnectReason NewSession()             { return TDisconnectReason("NewSession"); }
        static TDisconnectReason HandshakeFailTransient() { return TDisconnectReason("HandshakeFailTransient"); }
        static TDisconnectReason HandshakeFailPermanent() { return TDisconnectReason("HandshakeFailPermanent"); }
        static TDisconnectReason UserRequest()            { return TDisconnectReason("UserRequest"); }
        static TDisconnectReason Debug()                  { return TDisconnectReason("Debug"); }
        static TDisconnectReason ChecksumError()          { return TDisconnectReason("ChecksumError"); }
        static TDisconnectReason FormatError()            { return TDisconnectReason("FormatError"); }
        static TDisconnectReason EventTooLarge()          { return TDisconnectReason("EventTooLarge"); }
        static TDisconnectReason QueueOverload()          { return TDisconnectReason("QueueOverload"); }

        TString ToString() const {
            return Text;
        }

        static TVector<const char*> Reasons;
    };

} // NActors
