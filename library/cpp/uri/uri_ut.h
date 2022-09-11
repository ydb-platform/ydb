#pragma once

#include "uri.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NUri {
    struct TTest {
        TStringBuf Val;
        TParseFlags Flags;
        TState::EParsed State;
        TStringBuf Scheme;
        TStringBuf User;
        TStringBuf Pass;
        TStringBuf Host;
        ui16 Port;
        TStringBuf Path;
        TStringBuf Query;
        TStringBuf Frag;
        TStringBuf HashBang;
    };

}

#define URL_MSG(url1, url2, cmp) \
    (TString("[") + url1.PrintS() + ("] " cmp " [") + url2.PrintS() + "]")
#define URL_EQ(url1, url2) \
    UNIT_ASSERT_EQUAL_C(url, url2, URL_MSG(url1, url2, "!="))
#define URL_NEQ(url1, url2) \
    UNIT_ASSERT_UNEQUAL_C(url, url2, URL_MSG(url1, url2, "=="))

#define CMP_FLD(url, test, fld) \
    UNIT_ASSERT_VALUES_EQUAL(url.GetField(TField::Field##fld), test.fld)

#define CMP_URL(url, test)                                  \
    do {                                                    \
        CMP_FLD(url, test, Scheme);                         \
        CMP_FLD(url, test, User);                           \
        CMP_FLD(url, test, Pass);                           \
        CMP_FLD(url, test, Host);                           \
        UNIT_ASSERT_VALUES_EQUAL(url.GetPort(), test.Port); \
        CMP_FLD(url, test, Path);                           \
        CMP_FLD(url, test, Query);                          \
        CMP_FLD(url, test, Frag);                           \
        CMP_FLD(url, test, HashBang);                       \
    } while (false)

#define URL_TEST_ENC(url, test, enc)                                                                                              \
    do {                                                                                                                          \
        TState::EParsed st = url.ParseUri(test.Val, test.Flags, 0, enc);                                                          \
        UNIT_ASSERT_VALUES_EQUAL(st, test.State);                                                                                 \
        CMP_URL(url, test);                                                                                                       \
        if (TState::ParsedOK != st)                                                                                               \
            break;                                                                                                                \
        TUri _url;                                                                                                                \
        TString urlstr, urlstr2;                                                                                                  \
        urlstr = url.PrintS();                                                                                                    \
        TState::EParsed st2 = _url.ParseUri(urlstr,                                                                               \
                                            (test.Flags & ~TFeature::FeatureNoRelPath) | TFeature::FeatureAllowRootless, 0, enc); \
        if (TState::ParsedEmpty != st2)                                                                                           \
            UNIT_ASSERT_VALUES_EQUAL(st2, test.State);                                                                            \
        urlstr2 = _url.PrintS();                                                                                                  \
        UNIT_ASSERT_VALUES_EQUAL(urlstr, urlstr2);                                                                                \
        CMP_URL(_url, test);                                                                                                      \
        UNIT_ASSERT_VALUES_EQUAL(url.GetUrlFieldMask(), _url.GetUrlFieldMask());                                                  \
        URL_EQ(url, _url);                                                                                                        \
        const TStringBuf hostascii = url.GetField(TField::FieldHostAscii);                                                        \
        if (hostascii.Empty())                                                                                                    \
            break;                                                                                                                \
        urlstr = url.PrintS(TField::FlagHostAscii);                                                                               \
        st2 = _url.ParseUri(urlstr,                                                                                               \
                            (test.Flags & ~TFeature::FeatureNoRelPath) | TFeature::FeatureAllowRootless, 0, enc);                 \
        UNIT_ASSERT_VALUES_EQUAL(st2, test.State);                                                                                \
        urlstr2 = _url.PrintS();                                                                                                  \
        UNIT_ASSERT_VALUES_EQUAL(urlstr, urlstr2);                                                                                \
        TTest test2 = test;                                                                                                       \
        test2.Host = hostascii;                                                                                                   \
        CMP_URL(_url, test2);                                                                                                     \
        UNIT_ASSERT_VALUES_EQUAL(url.GetUrlFieldMask(), _url.GetUrlFieldMask());                                                  \
    } while (false)

#define URL_TEST(url, test) \
    URL_TEST_ENC(url, test, CODES_UTF8)
