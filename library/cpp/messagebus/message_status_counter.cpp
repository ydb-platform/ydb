#include "message_status_counter.h"

#include "key_value_printer.h"
#include "text_utils.h"

#include <library/cpp/messagebus/monitoring/mon_proto.pb.h>

#include <util/stream/str.h>

using namespace NBus;
using namespace NBus::NPrivate;

TMessageStatusCounter::TMessageStatusCounter() {
    Zero(Counts);
}

TMessageStatusCounter& TMessageStatusCounter::operator+=(const TMessageStatusCounter& that) {
    for (size_t i = 0; i < MESSAGE_STATUS_COUNT; ++i) {
        Counts[i] += that.Counts[i];
    }
    return *this;
}

TString TMessageStatusCounter::PrintToString() const {
    TStringStream ss;
    TKeyValuePrinter p;
    bool hasNonZeros = false;
    bool hasZeros = false;
    for (size_t i = 0; i < MESSAGE_STATUS_COUNT; ++i) {
        if (i == MESSAGE_OK) {
            Y_ABORT_UNLESS(Counts[i] == 0);
            continue;
        }
        if (Counts[i] != 0) {
            p.AddRow(EMessageStatus(i), Counts[i]);
            const char* description = MessageStatusDescription(EMessageStatus(i));
            // TODO: add third column
            Y_UNUSED(description);

            hasNonZeros = true;
        } else {
            hasZeros = true;
        }
    }
    if (!hasNonZeros) {
        ss << "message status counts are zeros\n";
    } else {
        if (hasZeros) {
            ss << "message status counts are zeros, except:\n";
        } else {
            ss << "message status counts:\n";
        }
        ss << IndentText(p.PrintToString());
    }
    return ss.Str();
}

void TMessageStatusCounter::FillErrorsProtobuf(TConnectionStatusMonRecord* status) const {
    status->clear_errorcountbystatus();
    for (size_t i = 0; i < MESSAGE_STATUS_COUNT; ++i) {
        if (i == MESSAGE_OK) {
            Y_ABORT_UNLESS(Counts[i] == 0);
            continue;
        }
        if (Counts[i] != 0) {
            TMessageStatusRecord* description = status->add_errorcountbystatus();
            description->SetStatus(TMessageStatusCounter::MessageStatusToProtobuf((EMessageStatus)i));
            description->SetCount(Counts[i]);
        }
    }
}
