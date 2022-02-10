#include "acceptor_status.h"

#include "key_value_printer.h"

#include <util/stream/format.h>
#include <util/stream/output.h>

using namespace NBus;
using namespace NBus::NPrivate;

TAcceptorStatus::TAcceptorStatus()
    : Summary(false)
    , AcceptorId(0)
    , Fd(INVALID_SOCKET)
{
    ResetIncremental();
}

void TAcceptorStatus::ResetIncremental() {
    AcceptSuccessCount = 0;
    AcceptErrorCount = 0;
    LastAcceptErrorErrno = 0;
    LastAcceptErrorInstant = TInstant();
    LastAcceptSuccessInstant = TInstant();
}

TAcceptorStatus& TAcceptorStatus::operator+=(const TAcceptorStatus& that) {
    Y_ASSERT(Summary);
    Y_ASSERT(AcceptorId == 0);

    AcceptSuccessCount += that.AcceptSuccessCount;
    LastAcceptSuccessInstant = Max(LastAcceptSuccessInstant, that.LastAcceptSuccessInstant);

    AcceptErrorCount += that.AcceptErrorCount;
    if (that.LastAcceptErrorInstant > LastAcceptErrorInstant) {
        LastAcceptErrorInstant = that.LastAcceptErrorInstant;
        LastAcceptErrorErrno = that.LastAcceptErrorErrno;
    }

    return *this;
}

TString TAcceptorStatus::PrintToString() const {
    TStringStream ss;

    if (!Summary) {
        ss << "acceptor (" << AcceptorId << "), fd=" << Fd << ", addr=" << ListenAddr << Endl;
    }

    TKeyValuePrinter p;

    p.AddRow("accept error count", LeftPad(AcceptErrorCount, 4));

    if (AcceptErrorCount > 0) {
        p.AddRow("last accept error",
                 TString() + LastSystemErrorText(LastAcceptErrorErrno) + " at " + LastAcceptErrorInstant.ToString());
    }

    p.AddRow("accept success count", LeftPad(AcceptSuccessCount, 4));
    if (AcceptSuccessCount > 0) {
        p.AddRow("last accept success",
                 TString() + "at " + LastAcceptSuccessInstant.ToString());
    }

    ss << p.PrintToString();

    return ss.Str();
}
