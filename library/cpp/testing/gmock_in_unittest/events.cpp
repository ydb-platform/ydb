#include "events.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

void TGMockTestEventListener::OnTestPartResult(const testing::TestPartResult& result) {
    if (result.failed()) {
        const TString message = result.message();
        const TString summary = result.summary();
        TStringBuilder msg;
        if (result.file_name())
            msg << result.file_name() << TStringBuf(":");
        if (result.line_number() != -1)
            msg << result.line_number() << TStringBuf(":");
        if (summary) {
            if (msg) {
                msg << TStringBuf("\n");
            }
            msg << summary;
        }
        if (message && summary != message) {
            if (msg) {
                msg << TStringBuf("\n");
            }
            msg << message;
        }
        NUnitTest::NPrivate::RaiseError(result.summary(), msg, result.fatally_failed());
    }
}
