#include <ydb/udfs/wasm/log_parsing/protoseq.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

using namespace NLogParsing;

namespace {

TString Frame(TStringBuf payload, TStringBuf syncWord) {
    const ui32 size = payload.size();
    TString frame;
    for (size_t i = 0; i < sizeof(size); ++i) {
        frame.push_back(static_cast<char>(size >> (i * 8)));
    }
    frame.append(payload.data(), payload.size());
    frame.append(syncWord.data(), syncWord.size());
    return frame;
}

} // namespace

Y_UNIT_TEST_SUITE(TProtoseqSplitterTest) {

Y_UNIT_TEST(ValidFrames) {
    const TString data = Frame("one", "ZZ") + Frame("two", "ZZ");
    TVector<TStringBuf> frames;

    UNIT_ASSERT(TProtoseqSplitter("ZZ").Split(data, &frames));
    UNIT_ASSERT_VALUES_EQUAL(frames.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(frames[0], "one");
    UNIT_ASSERT_VALUES_EQUAL(frames[1], "two");
}

Y_UNIT_TEST(CorruptMiddleFrameFailsWholeChunk) {
    TString corrupt = Frame("two", "ZZ");
    corrupt[0] = 100; // Declared length exceeds the remaining bytes.
    const TString data = Frame("one", "ZZ") + corrupt + Frame("three", "ZZ");
    TVector<TStringBuf> frames;

    UNIT_ASSERT(!TProtoseqSplitter("ZZ").Split(data, &frames));
}

Y_UNIT_TEST(LeadingJunkFailsWholeChunk) {
    const TString data = "junkZZ" + Frame("hello", "ZZ") + Frame("world", "ZZ");
    TVector<TStringBuf> frames;

    UNIT_ASSERT(!TProtoseqSplitter("ZZ").Split(data, &frames));
}

} // Y_UNIT_TEST_SUITE
