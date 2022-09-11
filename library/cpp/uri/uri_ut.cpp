#include "uri_ut.h"
#include "other.h"
#include "qargs.h"
#include <library/cpp/html/entity/htmlentity.h>

#include <util/system/maxlen.h>

namespace NUri {
    Y_UNIT_TEST_SUITE(URLTest) {
        static const char* urls[] = {
            "http://a/b/c/d;p?q#r",
            "g", "http://a/b/c/g",
            "./g", "http://a/b/c/g",
            "g/", "http://a/b/c/g/",
            "/g", "http://a/g",
            "//g", "http://g/",
            "?y", "http://a/b/c/d;p?y",
            "g?y", "http://a/b/c/g?y",
            "#s", "http://a/b/c/d;p?q#s",
            "g#s", "http://a/b/c/g#s",
            "g?y#s", "http://a/b/c/g?y#s",
            ";x", "http://a/b/c/;x",
            "g;x", "http://a/b/c/g;x",
            "g;x?y#s", "http://a/b/c/g;x?y#s",
            ".", "http://a/b/c/",
            "./", "http://a/b/c/",
            "./.", "http://a/b/c/",
            "././", "http://a/b/c/",
            "././.", "http://a/b/c/",
            "..", "http://a/b/",
            "../", "http://a/b/",
            "../.", "http://a/b/",
            "../g", "http://a/b/g",
            "../..", "http://a/",
            "../../", "http://a/",
            "../../.", "http://a/",
            "../../g", "http://a/g",
            "../../../g", "http://a/g",
            "../../../../g", "http://a/g",
            "/./g", "http://a/g",
            "g.", "http://a/b/c/g.",
            ".g", "http://a/b/c/.g",
            "g..", "http://a/b/c/g..",
            "..g", "http://a/b/c/..g",
            "./../g", "http://a/b/g",
            "./g/.", "http://a/b/c/g/",
            "g/./h", "http://a/b/c/g/h",
            "g/../h", "http://a/b/c/h",
            "g;x=1/./y", "http://a/b/c/g;x=1/y",
            "g;x=1/../y", "http://a/b/c/y",
            "g?y/./x", "http://a/b/c/g?y/./x",
            "g?y/../x", "http://a/b/c/g?y/../x",
            "g#s/./x", "http://a/b/c/g#s/./x",
            "g#s/../x", "http://a/b/c/g#s/../x",
            "?", "http://a/b/c/d;p?",
            "/?", "http://a/?",
            "x?", "http://a/b/c/x?",
            "x%20y", "http://a/b/c/x%20y",
            "%20y", "http://a/b/c/%20y",
            // "%2zy",          "http://a/b/c/%2zy",
            nullptr};

        Y_UNIT_TEST(test_httpURL) {
            TUri rel, base, abs;
            TState::EParsed er = base.Parse(urls[0]);
            UNIT_ASSERT_VALUES_EQUAL(er, TState::ParsedOK);
            UNIT_ASSERT(base.IsValidAbs());
            UNIT_ASSERT_VALUES_EQUAL(base.PrintS(), urls[0]);

            TString errbuf;
            TStringOutput out(errbuf);
            const long mflag = TFeature::FeaturesAll;
            for (int i = 1; urls[i]; i += 2) {
                er = rel.Parse(urls[i]);
                UNIT_ASSERT_VALUES_EQUAL_C(er, TState::ParsedOK, urls[i]);
                rel.Merge(base);
                UNIT_ASSERT_VALUES_EQUAL_C(rel.PrintS(), urls[i + 1], urls[i]);

                // try the same thing differently
                er = rel.Parse(urls[i], mflag, urls[0]);
                UNIT_ASSERT_VALUES_EQUAL_C(er, TState::ParsedOK, urls[i]);
                UNIT_ASSERT_VALUES_EQUAL_C(rel.PrintS(), urls[i + 1], urls[i]);

                // lastly...
                er = abs.Parse(urls[i + 1], mflag);
                UNIT_ASSERT_VALUES_EQUAL(er, TState::ParsedOK);
                errbuf.clear();
                out << '[' << rel.PrintS()
                    << "] != [" << abs.PrintS() << ']';
                UNIT_ASSERT_EQUAL_C(rel, abs, errbuf);
            }
        }

        Y_UNIT_TEST(test_Schemes) {
            TUri url;
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("www.ya.ru/index.html"), TState::ParsedOK);
            UNIT_ASSERT_EQUAL(url.GetScheme(), TScheme::SchemeEmpty);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("http://www.ya.ru"), TState::ParsedOK);
            UNIT_ASSERT_EQUAL(url.GetScheme(), TScheme::SchemeHTTP);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("https://www.ya.ru"), TState::ParsedBadScheme);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("https://www.ya.ru", TFeature::FeaturesDefault | TFeature::FeatureSchemeKnown), TState::ParsedOK);
            UNIT_ASSERT_EQUAL(url.GetScheme(), TScheme::SchemeHTTPS);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("httpwhatever://www.ya.ru"), TState::ParsedBadScheme);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("httpwhatever://www.ya.ru", TFeature::FeaturesDefault | TFeature::FeatureSchemeFlexible), TState::ParsedOK);
            UNIT_ASSERT_EQUAL(url.GetScheme(), TScheme::SchemeUnknown);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("httpswhatever://www.ya.ru"), TState::ParsedBadScheme);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("httpswhatever://www.ya.ru", TFeature::FeaturesDefault | TFeature::FeatureSchemeFlexible), TState::ParsedOK);
            UNIT_ASSERT_EQUAL(url.GetScheme(), TScheme::SchemeUnknown);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("ftp://www.ya.ru"), TState::ParsedBadScheme);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("ftp://www.ya.ru", TFeature::FeaturesDefault | TFeature::FeatureSchemeFlexible), TState::ParsedOK);
            UNIT_ASSERT_EQUAL(url.GetScheme(), TScheme::SchemeFTP);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("httpsssss://www.ya.ru"), TState::ParsedBadScheme);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("httpsssss://www.ya.ru", TFeature::FeaturesDefault | TFeature::FeatureSchemeFlexible), TState::ParsedOK);
            UNIT_ASSERT_EQUAL(url.GetScheme(), TScheme::SchemeUnknown);
        }

        struct Link4Norm {
            const char* const base;
            const char* const link;
            const char* const result;
            TUri::TLinkType ltype;
        };

        static const Link4Norm link4Norm[] = {
            {"http://www.alltest.ru/all.php?a=aberporth", "http://www.alltest.ru/all.php?a=domestic jobs", "", TUri::LinkIsBad},
            {"http://www.alltest.ru/all.php?a=aberporth", "http://www.alltest.ru/all.php?a=domestic%20jobs", "http://www.alltest.ru/all.php?a=domestic%20jobs", TUri::LinkIsLocal},
            {"http://president.rf/%D0%BD%D0%BE%D0%B2%D0%BE%D1%81%D1%82%D0%B8", "http://president.rf/%D0%BD%D0%BE%D0%B2%D0%BE%D1%81%D1%82%D0%B8/1024", "http://president.rf/%D0%BD%D0%BE%D0%B2%D0%BE%D1%81%D1%82%D0%B8/1024", TUri::LinkIsLocal},
            {nullptr, nullptr, nullptr, TUri::LinkIsBad},
        };

        Y_UNIT_TEST(test_httpURLNormalize) {
            TUri normalizedLink;

            for (int i = 0; link4Norm[i].link; i++) {
                TUri base;
                TState::EParsed er = base.Parse(link4Norm[i].base);
                UNIT_ASSERT_VALUES_EQUAL_C(er, TState::ParsedOK, link4Norm[i].base);
                TUri::TLinkType ltype = normalizedLink.Normalize(base, link4Norm[i].link);
                UNIT_ASSERT_VALUES_EQUAL_C(ltype, link4Norm[i].ltype, link4Norm[i].link);
                TString s = TUri::LinkIsBad == ltype ? "" : normalizedLink.PrintS();
                UNIT_ASSERT_VALUES_EQUAL_C(s, link4Norm[i].result, link4Norm[i].link);
            }
        }

        static const char* urlsWithMultipleSlash[] = {
            "http://a/http://b", "http://a/http://b",
            "http://a/https://b", "http://a/https://b",
            "http://a/b://c", "http://a/b:/c",
            "http://a/b//c", "http://a/b/c",
            nullptr, nullptr};

        Y_UNIT_TEST(test_httpURLPathOperation) {
            char copyUrl[URL_MAXLEN];
            for (int i = 0; urlsWithMultipleSlash[i]; i += 2) {
                const TStringBuf url(urlsWithMultipleSlash[i]);
                const TStringBuf normurl(urlsWithMultipleSlash[i + 1]);
                memcpy(copyUrl, url.data(), url.length());
                char* p = copyUrl;
                char* e = copyUrl + url.length();
                TUri::PathOperation(p, e, 1);
                UNIT_ASSERT_VALUES_EQUAL(TStringBuf(p, e), normurl);
                TUri uri;
                UNIT_ASSERT_VALUES_EQUAL(TState::ParsedOK, uri.Parse(url));
                UNIT_ASSERT_VALUES_EQUAL_C(uri.PrintS(), normurl, url);
            }
        }

        static const char* hostsForCheckHost[] = {
            "simplehost.ru",
            "third_level.host.ru",
            "_ok.somewhere.ru",
            "a.b",
            "second_level.ru",
            "_bad.ru",
            "_",
            "yandex.ru:443",
            nullptr};

        static TState::EParsed answersForCheckHost[] = {
            TState::ParsedOK,
            TState::ParsedOK,
            TState::ParsedOK,
            TState::ParsedOK,
            TState::ParsedBadHost,
            TState::ParsedBadHost,
            TState::ParsedBadHost,
            TState::ParsedBadHost,
        };

        Y_UNIT_TEST(test_httpURLCheckHost) {
            for (size_t index = 0; hostsForCheckHost[index]; ++index) {
                TState::EParsed state = TUri::CheckHost(hostsForCheckHost[index]);
                UNIT_ASSERT_VALUES_EQUAL(state, answersForCheckHost[index]);
            }
        }

        Y_UNIT_TEST(test_httpURLSet) {
            // set port
            {
                TUri parsedUrl;
                parsedUrl.Parse("http://www.host.com/script.cgi?param1=value1&param2=value2");
                parsedUrl.FldMemSet(TField::FieldPort, "8080");
                UNIT_ASSERT_VALUES_EQUAL(parsedUrl.GetPort(), 8080);
                UNIT_ASSERT_VALUES_EQUAL(parsedUrl.PrintS(), "http://www.host.com:8080/script.cgi?param1=value1&param2=value2");
            }

            // clear port
            {
                TUri parsedUrl;
                parsedUrl.Parse("http://www.host.com:8080/script.cgi?param1=value1&param2=value2");
                parsedUrl.FldMemSet(TField::FieldPort, nullptr);
                UNIT_ASSERT_VALUES_EQUAL(parsedUrl.GetPort(), 80);
                UNIT_ASSERT_VALUES_EQUAL(parsedUrl.PrintS(), "http://www.host.com/script.cgi?param1=value1&param2=value2");
            }

            // change scheme with default port
            {
                TUri parsedUrl;
                parsedUrl.Parse("http://www.host.com/script.cgi?param1=value1&param2=value2");
                parsedUrl.FldMemSet(TField::FieldScheme, "https");
                UNIT_ASSERT_VALUES_EQUAL(parsedUrl.GetPort(), 443);
                UNIT_ASSERT_VALUES_EQUAL(parsedUrl.PrintS(), "https://www.host.com/script.cgi?param1=value1&param2=value2");
            }

            // change scheme with non-default port
            {
                TUri parsedUrl;
                parsedUrl.Parse("http://www.host.com:8080/script.cgi?param1=value1&param2=value2");
                parsedUrl.FldMemSet(TField::FieldScheme, "https");
                UNIT_ASSERT_VALUES_EQUAL(parsedUrl.GetPort(), 8080);
                UNIT_ASSERT_VALUES_EQUAL(parsedUrl.PrintS(), "https://www.host.com:8080/script.cgi?param1=value1&param2=value2");
            }
        }

        Y_UNIT_TEST(test_httpURLAuth) {
            {
                TUri parsedUrl;
                TState::EParsed st = parsedUrl.Parse("http://@www.host.com/path", TFeature::FeaturesRobot);
                UNIT_ASSERT_VALUES_EQUAL(st, TState::ParsedBadAuth);
            }

            {
                TUri parsedUrl;
                TState::EParsed st = parsedUrl.Parse("http://loginwithnopass@www.host.com/path", TFeature::FeatureAuthSupported);
                UNIT_ASSERT_VALUES_EQUAL(st, TState::ParsedOK);
                UNIT_ASSERT_EQUAL(parsedUrl.GetField(TField::FieldHost), "www.host.com");
                UNIT_ASSERT_EQUAL(parsedUrl.GetField(TField::FieldUser), "loginwithnopass");
                UNIT_ASSERT_EQUAL(parsedUrl.GetField(TField::FieldPass), "");
            }

            {
                TUri parsedUrl;
                TState::EParsed st = parsedUrl.Parse("http://login:pass@www.host.com/path", TFeature::FeatureAuthSupported);
                UNIT_ASSERT_VALUES_EQUAL(st, TState::ParsedOK);
                UNIT_ASSERT_EQUAL(parsedUrl.GetField(TField::FieldHost), "www.host.com");
                UNIT_ASSERT_EQUAL(parsedUrl.GetField(TField::FieldUser), "login");
                UNIT_ASSERT_EQUAL(parsedUrl.GetField(TField::FieldPass), "pass");
            }
        }

        Y_UNIT_TEST(test01) {
            TTest test = {
                "user:pass@host:8080", TFeature::FeaturesAll, TState::ParsedRootless, "user", "", "", "", 0, "", "", "", ""};
            TUri url;
            URL_TEST(url, test);
        }

        Y_UNIT_TEST(test02) {
            TTest test = {
                "http://host", TFeature::FeaturesAll, TState::ParsedOK, "http", "", "", "host", 80, "/", "", "", ""};
            TUri url;
            URL_TEST(url, test);
        }

        Y_UNIT_TEST(test03) {
            TTest test = {
                "https://host", TFeature::FeatureSchemeFlexible | TFeature::FeatureAllowHostIDN, TState::ParsedOK, "https", "", "", "host", 443, "/", "", "", ""};
            TUri url;
            URL_TEST(url, test);
        }

        Y_UNIT_TEST(test04) {
            TTest test = {
                "user:pass@host:8080", TFeature::FeaturesAll | TFeature::FeatureNoRelPath | TFeature::FeatureAllowRootless, TState::ParsedOK, "user", "", "", "", 0, "pass@host:8080", "", "", ""};
            TUri url;
            URL_TEST(url, test);
            TUri url2(url);
            CMP_URL(url2, test);
            URL_EQ(url, url2);
        }

        Y_UNIT_TEST(test05) {
            TTest test = {
                "host:8080", TFeature::FeaturesAll | TFeature::FeatureNoRelPath | TFeature::FeatureAllowRootless, TState::ParsedOK, "host", "", "", "", 0, "8080", "", "", ""};
            TUri url;
            URL_TEST(url, test);
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "host:8080");
        }

        Y_UNIT_TEST(test06) {
            TTest test = {
                "http://user:pass@host?q", TFeature::FeaturesAll, TState::ParsedOK, "http", "user", "pass", "host", 80, "/", "q", "", ""};
            TUri url;
            URL_TEST(url, test);
            url.FldMemSet(TField::FieldScheme, "https");
            UNIT_ASSERT(!url.FldIsDirty());
            UNIT_ASSERT_VALUES_EQUAL(url.GetField(TField::FieldScheme), "https");
            UNIT_ASSERT_VALUES_EQUAL(url.GetPort(), 443);

            // test copying
            TUri url2(url);
            // make sure strings are equal...
            UNIT_ASSERT_VALUES_EQUAL(
                url.GetField(TField::FieldUser),
                url2.GetField(TField::FieldUser));
            // ... and memory locations are the same
            UNIT_ASSERT_EQUAL(
                url.GetField(TField::FieldUser),
                url2.GetField(TField::FieldUser));
            // and urls compare the same
            URL_EQ(url, url2);

            // cause a dirty field
            url.FldMemSet(TField::FieldUser, "use"); // it is now shorter
            UNIT_ASSERT(!url.FldIsDirty());
            url.FldMemSet(TField::FieldUser, TStringBuf("user"));
            UNIT_ASSERT(url.FldIsDirty());

            // copy again
            url2 = url;
            UNIT_ASSERT(url.FldIsDirty());
            UNIT_ASSERT(!url2.FldIsDirty());
            URL_EQ(url, url2);
            // make sure strings are equal...
            UNIT_ASSERT_VALUES_EQUAL(
                url.GetField(TField::FieldUser),
                url2.GetField(TField::FieldUser));
            // ... but memory locations are different
            UNIT_ASSERT_UNEQUAL(
                url.GetField(TField::FieldUser).data(),
                url2.GetField(TField::FieldUser).data());
            URL_EQ(url, url2);

            // make query empty
            url.FldMemSet(TField::FieldQuery, "");
            url2 = url;
            URL_EQ(url, url2);
            // set query to null value (should clear it)
            url2.FldMemSet(TField::FieldQuery, TStringBuf());
            // make sure they are no longer equal
            URL_NEQ(url, url2);
            // reset query
            url.FldClr(TField::FieldQuery);
            // equal again
            URL_EQ(url, url2);
            // reset port and set the other to default
            url.FldClr(TField::FieldPort);
            url2.FldMemSet(TField::FieldPort, "443");
            URL_EQ(url, url2);
        }

        Y_UNIT_TEST(test07) {
            {
                TTest test = {
                    "http://host/path//", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "http", "", "", "host", 80, "/path/", "", "", ""};
                TUri url;
                URL_TEST(url, test);
                url.FldMemSet(TField::FieldScheme, "HTTPs");
                UNIT_ASSERT_EQUAL(TScheme::SchemeHTTPS, url.GetScheme());
                UNIT_ASSERT_EQUAL("https", url.GetField(TField::FieldScheme));
                url.FldMemSet(TField::FieldScheme, "HtTP");
                UNIT_ASSERT_EQUAL(TScheme::SchemeHTTP, url.GetScheme());
                UNIT_ASSERT_EQUAL("http", url.GetField(TField::FieldScheme));
            }

            {
                const TString scheme = "http";
                const TString host = "host.com";
                const TString urlstr = scheme + "://" + host;
                TTest test = {
                    urlstr, TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, scheme, "", "", host, 80, "/", "", "", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), urlstr + "/");
            }
        }

        Y_UNIT_TEST(test08) {
            {
                TTest test = {
                    "mailto://user@host.com", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "mailto", "user", "", "host.com", 0, "", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "host:/path/.path/.", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "host", "", "", "", 0, "/path/.path/", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "host:1/path/.path/.", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "", "", "", "host", 1, "/path/.path/", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "host:1/path/.path/.", TFeature::FeaturesAll | TFeature::FeatureAllowRootless, TState::ParsedOK, "host", "", "", "", 0, "1/path/.path/.", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "/[foo]:bar", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "", "", "", "", 0, "/[foo]:bar", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    ".", TFeature::FeaturesAll, TState::ParsedOK, "", "", "", "", 0, "", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    ".", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "", "", "", "", 0, "", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "././.", TFeature::FeaturesAll, TState::ParsedOK, "", "", "", "", 0, "", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "././.", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "", "", "", "", 0, "", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "./path", TFeature::FeaturesAll, TState::ParsedOK, "", "", "", "", 0, "path", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "./path", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "", "", "", "", 0, "path", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "../path", TFeature::FeaturesAll, TState::ParsedOK, "", "", "", "", 0, "../path", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "../path", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "", "", "", "", 0, "../path", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "/../path", TFeature::FeaturesAll, TState::ParsedOK, "", "", "", "", 0, "/path", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
        }

        Y_UNIT_TEST(test09) {
            {
                TTest test = {
                    "mailto:user@host.com", TFeature::FeaturesAll | TFeature::FeatureAllowRootless, TState::ParsedOK, "mailto", "", "", "", 0, "user@host.com", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "scheme:", TFeature::FeaturesAll | TFeature::FeatureNoRelPath | TFeature::FeatureAllowRootless, TState::ParsedOK, "scheme", "", "", "", 0, "", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "scheme:", TFeature::FeaturesAll | TFeature::FeatureAllowRootless, TState::ParsedOK, "scheme", "", "", "", 0, "", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
        }

        Y_UNIT_TEST(test10) {
            // test some escaping madness, note the ehost vs host
            {
                TString host = "президент.рф";
                TString ehost = "%D0%BF%D1%80%D0%B5%D0%B7%D0%B8%D0%B4%D0%B5%D0%BD%D1%82.%D1%80%D1%84";
                const TString urlstr = TString::Join("http://", host, "/");
                TTest test = {
                    urlstr, TFeature::FeatureEncodeExtendedASCII | TFeature::FeaturesDefault | TFeature::FeatureCheckHost, TState::ParsedBadHost, "http", "", "", ehost, 80, "/", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }

            {
                TString host = "%D0%BF%D1%80%D0%B5%D0%B7%D0%B8%D0%B4%D0%B5%D0%BD%D1%82.%D1%80%D1%84";
                const TString urlstr = TString::Join("http://", host, "/");
                TTest test = {
                    urlstr, TFeature::FeatureEncodeExtendedASCII | TFeature::FeaturesDefault | TFeature::FeatureCheckHost, TState::ParsedBadHost, "http", "", "", host, 80, "/", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }

            {
                TString host = "Фilip.ru";
                TString ehost = "%D0%A4ilip.ru";
                const TString urlstr = TString::Join("http://", host);
                TTest test = {
                    urlstr, TFeature::FeatureEncodeExtendedASCII | TFeature::FeaturesDefault, TState::ParsedBadHost, "http", "", "", ehost, 80, "/", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }

            {
                TString host = "%D0%A4ilip.ru";
                const TString urlstr = TString::Join("http://", host);
                TTest test = {
                    urlstr, TFeature::FeatureEncodeExtendedASCII | TFeature::FeaturesDefault, TState::ParsedBadHost, "http", "", "", host, 80, "/", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }

            {
                TString host = "Filip%90.rЯ";
                TString ehost = "Filip%90.r%D0%AF";
                const TString urlstr = TString::Join(host, ":8080");
                TTest test = {
                    urlstr, TFeature::FeatureEncodeExtendedASCII | TFeature::FeatureDecodeAllowed | TFeature::FeaturesDefault | TFeature::FeatureNoRelPath, TState::ParsedBadHost, "", "", "", ehost, 8080, "", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }

            {
                TString host = "Filip%90.r%D0%AF";
                const TString urlstr = TString::Join(host, ":8080");
                TTest test = {
                    urlstr, TFeature::FeatureEncodeExtendedASCII | TFeature::FeatureDecodeAllowed | TFeature::FeaturesDefault | TFeature::FeatureNoRelPath, TState::ParsedBadHost, "", "", "", host, 8080, "", "", "", ""};
                TUri url;
                URL_TEST(url, test);
            }
        }

        Y_UNIT_TEST(test11) {
            {
                TTest test = {
                    "HtTp://HoSt/%50aTh/?Query#Frag", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedOK, "http", "", "", "host", 80, "/PaTh/", "Query", "Frag", ""};
                TUri url;
                URL_TEST(url, test);
            }

            {
                TTest test = {
                    "HtTp://HoSt/%50a%54h/?Query#Frag", TParseFlags(TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TFeature::FeatureToLower), TState::ParsedOK, "http", "", "", "host", 80, "/path/", "query", "frag", ""};
                TUri url;
                URL_TEST(url, test);
            }
        }

        Y_UNIT_TEST(test12) {
            // test characters which are not always safe
            {
#define RAW "/:"
#define DEC "%2F:"
#define ENC "%2F%3A"
                TTest test = {
                    "http://" ENC ":" ENC "@host/" ENC "?" ENC "#" ENC, TFeature::FeaturesAll, TState::ParsedOK, "http", RAW, RAW, "host", 80, "/" DEC, RAW, RAW, ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://" ENC ":" ENC "@host/" DEC "?" RAW "#" RAW);
#undef RAW
#undef DEC
#undef ENC
            }
            {
#define RAW "?@"
#define DEC "%3F@"
#define ENC "%3F%40"
                TTest test = {
                    "http://" ENC ":" ENC "@host/" ENC "?" ENC "#" ENC, TFeature::FeaturesAll, TState::ParsedOK, "http", RAW, RAW, "host", 80, "/" DEC, RAW, RAW, ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://" ENC ":" ENC "@host/" DEC "?" RAW "#" RAW);
#undef RAW
#undef DEC
#undef ENC
            }
            {
#define RAW "%&;="
#define DEC "%25&;="
#define ENC "%25%26%3B%3D"
                TTest test = {
                    "http://" ENC ":" ENC "@host/" ENC "?" ENC "#" ENC, TFeature::FeaturesAll, TState::ParsedOK, "http", RAW, RAW, "host", 80, "/" ENC, ENC, ENC, ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://" ENC ":" ENC "@host/" ENC "?" ENC "#" ENC);
#undef RAW
#undef DEC
#undef ENC
            }
            {
#define RAW "!$'()*,"
#define DEC "!$%27()*,"
#define ENC "%21%24%27%28%29%2A%2C"
                TTest test = {
                    "http://" ENC ":" ENC "@host/" ENC "?" ENC "#" ENC, TFeature::FeaturesAll, TState::ParsedOK, "http", RAW, RAW, "host", 80, "/" ENC, DEC, DEC, ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://" ENC ":" ENC "@host/" ENC "?" DEC "#" DEC);
#undef RAW
#undef DEC
#undef ENC
            }
            {
#define DEC "Череповец。рф"
#define ENC "%D0%A7%D0%B5%D1%80%D0%B5%D0%BF%D0%BE%D0%B2%D0%B5%D1%86%E3%80%82%D1%80%D1%84"
// punycode corresponds to lowercase
#define PNC "xn--b1afab7bff7cb.xn--p1ai"
                TTest test = {
                    "http://" ENC "/" ENC "?" ENC "#" ENC, TParseFlags(TFeature::FeaturesAll | TFeature::FeatureAllowHostIDN, TFeature::FeatureDecodeExtendedASCII), TState::ParsedOK, "http", "", "", DEC, 80, "/" ENC, ENC, ENC, ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.GetField(TField::FieldHostAscii), PNC);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://" DEC "/" ENC "?" ENC "#" ENC);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(TField::FlagHostAscii), "http://" PNC "/" ENC "?" ENC "#" ENC);
#undef PNC
#undef DEC
#undef ENC
            }
            {
#define DEC "Череповец。рф"
#define ENC "%D0%A7%D0%B5%D1%80%D0%B5%D0%BF%D0%BE%D0%B2%D0%B5%D1%86%E3%80%82%D1%80%D1%84"
// punycode corresponds to lowercase
#define PNC "xn--b1afab7bff7cb.xn--p1ai"
                TTest test = {
                    "http://" DEC "/" DEC "?" DEC "#" DEC, TParseFlags(TFeature::FeaturesRobot | TFeature::FeatureEncodeExtendedASCII), TState::ParsedOK, "http", "", "", PNC, 80, "/" ENC, ENC, ENC, ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://" PNC "/" ENC "?" ENC "#" ENC);
#undef PNC
#undef DEC
#undef ENC
            }
            {
#define DEC "независимая-экспертиза-оценка-ущерба-авто-дтп.рф"
#define PNC "xn--------3veabbbbjgk5abecc3afsad2cg8bvq2alouolqf5brd3a4jzftgqd.xn--p1ai"
                TTest test = {
                    "http://" DEC "/", TParseFlags(TFeature::FeaturesRobot), TState::ParsedOK, "http", "", "", PNC, 80, "/", "", "", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://" PNC "/");
#undef PNC
#undef DEC
            }
        }

        Y_UNIT_TEST(testFlexibleAuthority) {
            TUri uri;
            UNIT_ASSERT_EQUAL(uri.Parse("http://hello_world", TFeature::FeatureCheckHost), TState::ParsedBadHost);
            UNIT_ASSERT_EQUAL(uri.Parse("http://hello_world", TFeature::FeatureSchemeFlexible), TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(uri.GetHost(), "hello_world");

            UNIT_ASSERT_EQUAL(uri.Parse("httpzzzzz://)(*&^$!\\][';<>`~,q?./index.html", TFeature::FeatureSchemeFlexible), TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(uri.GetHost(), ")(*&^$!\\][';<>`~,q");
            UNIT_ASSERT_VALUES_EQUAL(uri.GetField(TField::FieldPath), "");
            UNIT_ASSERT_VALUES_EQUAL(uri.GetField(TField::FieldQuery), "./index.html");

            UNIT_ASSERT_EQUAL(uri.Parse("htttttttp://)(*&^%45$!\\][';<>`~,.q/index.html", TFeature::FeatureSchemeFlexible), TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(uri.GetHost(), ")(*&^e$!\\][';<>`~,.q");
            UNIT_ASSERT_VALUES_EQUAL(uri.GetField(TField::FieldPath), "/index.html");
            UNIT_ASSERT_VALUES_EQUAL(uri.GetField(TField::FieldQuery), "");
        }

        Y_UNIT_TEST(testSpecialChar) {
            // test characters which are not always allowed
            {
                TTest test = {
                    "http://host/pa th", TFeature::FeaturesAll | TFeature::FeatureEncodeSpace, TState::ParsedOK, "http", "", "", "host", 80, "/pa%20th", "", "", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://host/pa%20th");
            }
            {
                TTest test = {
                    "http://host/pa th", TFeature::FeaturesAll, TState::ParsedBadFormat, "http", "", "", "host", 80, "/pa th", "", "", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://host/pa th");
            }
            {
                TTest test = {
                    "http://host/pa%th%41", TFeature::FeaturesAll | TFeature::FeatureEncodePercent, TState::ParsedOK, "http", "", "", "host", 80, "/pa%25thA", "", "", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://host/pa%25thA");
            }
            {
                TTest test = {
                    "http://host/invalid_second_char%az%1G", TFeature::FeaturesAll | TFeature::FeatureEncodePercent, TState::ParsedOK, "http", "", "", "host", 80, "/invalid_second_char%25az%251G", "", "", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://host/invalid_second_char%25az%251G");
            }
            {
                TTest test = {
                    "http://host/border%2", TFeature::FeaturesAll | TFeature::FeatureEncodePercent, TState::ParsedOK, "http", "", "", "host", 80, "/border%252", "", "", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://host/border%252");
            }
            {
                TTest test = {
                    "http://host/pa%th%41", TFeature::FeaturesAll, TState::ParsedBadFormat, "http", "", "", "host", 80, "/pa%thA", "", "", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://host/pa%thA");
            }
        }

        Y_UNIT_TEST(testIPv6) {
            {
#define RAW "[1080:0:0:0:8:800:200C:417A]"
#define DEC "[1080:0:0:0:8:800:200c:417a]"
                TTest test = {
                    "http://" RAW "/" RAW "?" RAW "#" RAW, TParseFlags(TFeature::FeaturesAll), TState::ParsedOK, "http", "", "", DEC, 80, "/" RAW, RAW, RAW, ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://" DEC "/" RAW "?" RAW "#" RAW);
#undef DEC
#undef RAW
            }
        }

        Y_UNIT_TEST(testEscapedFragment) {
            {
                TTest test = {
                    "http://host.com#!a=b&c=d#e+g%41%25", TParseFlags(TFeature::FeaturesAll | TFeature::FeatureHashBangToEscapedFragment), TState::ParsedOK, "http", "", "", "host.com", 80, "/", "_escaped_fragment_=a=b%26c=d%23e%2BgA%2525", "", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://host.com/?_escaped_fragment_=a=b%26c=d%23e%2BgA%2525");
            }
            {
                TTest test = {
                    "http://host.com?_escaped_fragment_=a=b%26c=d%23e%2bg%2525", TParseFlags(TFeature::FeaturesAll | TFeature::FeatureEscapedToHashBangFragment), TState::ParsedOK, "http", "", "", "host.com", 80, "/", "", "!a=b&c=d#e+g%25", ""};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://host.com/#!a=b&c=d#e+g%25");
            }
        }

        Y_UNIT_TEST(testHashBang) {
            {
                TTest test = {
                "http://host.com#!?a=b&c=d#e+g%41%25", TParseFlags(TFeature::FeaturesAll | TFeature::FeatureFragmentToHashBang), TState::ParsedOK, "http", "", "", "host.com", 80, "/", "", "", "%3Fa=b%26c=d%23e%2BgA%25"};
                TUri url;
                URL_TEST(url, test);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://host.com/#!%3Fa=b%26c=d%23e%2BgA%25");
            }
        }

        Y_UNIT_TEST(testReEncode) {
            {
                TStringStream out;
                TUri::ReEncode(out, "foo bar");
                UNIT_ASSERT_VALUES_EQUAL(out.Str(), "foo%20bar");
            }
        }

        static const TStringBuf NonRfcUrls[] = {
            "http://deshevle.ru/price/price=&SrchTp=1&clID=24&BL=SrchTp=0|clID=24&frmID=75&SortBy=P&PreSort=&NmDir=0&VndDir=0&PrDir=0&SPP=44",
            "http://secure.rollerwarehouse.com/skates/aggressive/skates/c/11[03]/tx/$$$+11[03][a-z]",
            "http://secure.rollerwarehouse.com/skates/aggressive/skates/tx/$$$+110[a-z]",
            "http://translate.google.com/translate_t?langpair=en|ru",
            "http://www.garnier.com.ru/_ru/_ru/our_products/products_trade.aspx?tpcode=OUR_PRODUCTS^PRD_BODYCARE^EXTRA_SKIN^EXTRA_SKIN_BENEFITS",
            "http://www.km.ru/magazin/view_print.asp?id={1846295A-223B-41DC-9F51-90D5D6236C49}",
            "http://www.manutd.com/default.sps?pagegid={78F24B85-702C-4DC8-A5D4-2F67252C28AA}&itype=12977&pagebuildpageid=2716&bg=1",
            "http://www.pokupay.ru/price/price=&SrchTp=1&clID=24&BL=SrchTp=0|clID=24&frmID=75&SPP=35&SortBy=N&PreSort=V&NmDir=0&VndDir=1&PrDir=0",
            "http://www.rodnoyspb.ru/rest/plager/page[0].html",
            "http://www.trinity.by/?section_id=46,47,48&cat=1&filters[]=2^_^Sony",
            "http://translate.yandex.net/api/v1/tr.json/translate?lang=en-ru&text=>",
            nullptr};

        Y_UNIT_TEST(test_NonRfcUrls) {
            TUri url;
            const long flags = TFeature::FeaturesRobot;
            for (size_t i = 0;; ++i) {
                const TStringBuf& buf = NonRfcUrls[i];
                if (!buf.IsInited())
                    break;
                UNIT_ASSERT_VALUES_EQUAL(TState::ParsedOK, url.Parse(buf, flags));
            }
        }

        static const TStringBuf CheckParseException[] = {
            "http://www.'>'.com/?.net/",
            nullptr};

        Y_UNIT_TEST(test_CheckParseException) {
            TUri url;
            const long flags = TFeature::FeaturesRobot | TFeature::FeaturesEncode;
            for (size_t i = 0;; ++i) {
                const TStringBuf& buf = CheckParseException[i];
                if (!buf.IsInited())
                    break;
                TString what;
                try {
                    // we care only about exceptions, not whether it parses correctly
                    url.Parse(buf, flags);
                    continue;
                } catch (const std::exception& exc) {
                    what = exc.what();
                } catch (...) {
                    what = "exception thrown";
                }
                ythrow yexception() << "failed to parse URL [" << buf << "]: " << what;
            }
        }

        Y_UNIT_TEST(test_PrintPort) {
            TUri uri;
            {
                uri.Parse("http://srv.net:9100/print", TFeature::FeaturesRecommended);
                TString s = uri.PrintS(TUri::FlagPort);
                Cdbg << uri.PrintS() << ',' << uri.PrintS(TUri::FlagPort) << Endl;
                UNIT_ASSERT_VALUES_EQUAL(9100, FromString<ui32>(s));
            }
            {
                uri.Parse("http://srv.net:80/print", TFeature::FeaturesRecommended);
                TString s = uri.PrintS(TUri::FlagPort);
                Cdbg << uri.PrintS() << ',' << uri.PrintS(TUri::FlagPort) << Endl;
                UNIT_ASSERT(s.Empty());
            }
        }

        Y_UNIT_TEST(test_ParseFailures) {
            {
                TTest test = {
                    "http://host:port", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedBadFormat, "", "", "", "", Max<ui16>(), "", "", "", ""};
                TUri url(-1);
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "http://javascript:alert(hi)", TFeature::FeaturesRobot, TState::ParsedBadFormat, "", "", "", "", Max<ui16>(), "", "", "", ""};
                TUri url(-1);
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "http://host::0", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedBadFormat, "", "", "", "", Max<ui16>(), "", "", "", ""};
                TUri url(-1);
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "http://host ", TFeature::FeaturesAll, TState::ParsedBadFormat, "", "", "", "", Max<ui16>(), "", "", "", ""};
                TUri url(-1);
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "http:00..03", TFeature::FeaturesAll | TFeature::FeatureNoRelPath, TState::ParsedBadFormat, "", "", "", "", Max<ui16>(), "", "", "", ""};
                TUri url(-1);
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "host:00..03", TFeature::FeaturesAll, TState::ParsedRootless, "host", "", "", "", 0, "", "", "", ""};
                TUri url(-1);
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "http://roduct;isbn,0307371549;at,aid4c00179ab018www.mcnamarasband.wordpress.com/", TFeature::FeaturesAll, TState::ParsedBadHost, "http", "", "", "roduct;isbn,0307371549;at,aid4c00179ab018www.mcnamarasband.wordpress.com", 80, "/", "", "", ""};
                TUri url(-1);
                URL_TEST(url, test);
            }
            {
                TTest test = {
                    "invalid url", TFeature::FeaturesDefault, TState::ParsedBadFormat, "", "", "", "", 0, "invalid url", "", "", ""};
                TUri url(-1);
                URL_TEST(url, test);
            }
        }
        Y_UNIT_TEST(test_scheme_related_url) {
            TUri url;
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("//www.hostname.ru/path", TFeature::FeaturesRobot), TState::ParsedOK);
            UNIT_ASSERT_EQUAL(url.GetScheme(), TScheme::SchemeEmpty);
            UNIT_ASSERT_VALUES_EQUAL(url.GetHost(), "www.hostname.ru");
            UNIT_ASSERT_VALUES_EQUAL(url.GetField(TField::FieldPath), "/path");

           TUri baseUrl;
           UNIT_ASSERT_VALUES_EQUAL(baseUrl.Parse("https://trololo.com", TFeature::FeaturesRobot), TState::ParsedOK);
           UNIT_ASSERT_EQUAL(baseUrl.GetScheme(), TScheme::SchemeHTTPS);
           url.Merge(baseUrl);
           UNIT_ASSERT_EQUAL(url.GetScheme(), TScheme::SchemeHTTPS);
           UNIT_ASSERT_VALUES_EQUAL(url.GetHost(), "www.hostname.ru");
           UNIT_ASSERT_VALUES_EQUAL(url.GetField(TField::FieldPath), "/path");
        }
    }

    Y_UNIT_TEST_SUITE(TInvertDomainTest) {
        Y_UNIT_TEST(TestInvert) {
            TString a;
            UNIT_ASSERT_EQUAL(InvertDomain(a), "");
            TString aa(".:/foo");
            UNIT_ASSERT_EQUAL(InvertDomain(aa), ".:/foo");
            TString aaa("/foo.bar:");
            UNIT_ASSERT_EQUAL(InvertDomain(aaa), "/foo.bar:");
            TString b("ru");
            UNIT_ASSERT_EQUAL(InvertDomain(b), "ru");
            TString c(".ru");
            UNIT_ASSERT_EQUAL(InvertDomain(c), "ru.");
            TString d("ru.");
            UNIT_ASSERT_EQUAL(InvertDomain(d), ".ru");
            TString e("www.yandex.ru:80/yandsearch?text=foo");
            UNIT_ASSERT_EQUAL(InvertDomain(e), "ru.yandex.www:80/yandsearch?text=foo");
            TString f("www.yandex.ru:80/yandsearch?text=foo");
            InvertDomain(f.begin(), f.begin() + 10);
            UNIT_ASSERT_EQUAL(f, "yandex.www.ru:80/yandsearch?text=foo");
            TString g("https://www.yandex.ru:80//");
            UNIT_ASSERT_EQUAL(InvertDomain(g), "https://ru.yandex.www:80//");
            TString h("www.yandex.ru:8080/redir.pl?url=https://google.com/");
            UNIT_ASSERT_EQUAL(InvertDomain(h), "ru.yandex.www:8080/redir.pl?url=https://google.com/");
        }
    }

    TQueryArg::EProcessed ProcessQargs(TString url, TString& processed, TQueryArgFilter filter = 0, void* filterData = 0) {
        TUri uri;
        uri.Parse(url, NUri::TFeature::FeaturesRecommended);

        TQueryArgProcessing processing(TQueryArg::FeatureSortByName | (filter ? TQueryArg::FeatureFilter : 0) | TQueryArg::FeatureRewriteDirty, filter, filterData);
        auto result = processing.Process(uri);
        processed = uri.PrintS();
        return result;
    }

    TString SortQargs(TString url) {
        TString r;
        ProcessQargs(url, r);
        return r;
    }

    bool QueryArgsFilter(const TQueryArg& arg, void* filterData) {
        const char* skipName = static_cast<const char*>(filterData);
        return arg.Name != skipName;
    }

    TString FilterQargs(TString url, const char* name) {
        TString r;
        ProcessQargs(url, r, &QueryArgsFilter, const_cast<char*>(name));
        return r;
    }

    Y_UNIT_TEST_SUITE(QargsTest) {
        Y_UNIT_TEST(TestSorting) {
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/"), "http://ya.ru/");
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?"), "http://ya.ru/?");
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?some=value"), "http://ya.ru/?some=value");
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?b=1&a=2"), "http://ya.ru/?a=2&b=1");
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?b=1&a=2&a=3"), "http://ya.ru/?a=2&a=3&b=1");

            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?aaa=3&b=b&a=1&aa=2"), "http://ya.ru/?a=1&aa=2&aaa=3&b=b");

            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?a=1&b=1&c=1"), "http://ya.ru/?a=1&b=1&c=1");
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?b=1&a=1&c=1"), "http://ya.ru/?a=1&b=1&c=1");
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?c=1&a=1&b=1"), "http://ya.ru/?a=1&b=1&c=1");

            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?c=1&a=1&a=1&b=1&c=1&b=1"), "http://ya.ru/?a=1&a=1&b=1&b=1&c=1&c=1");

            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?b==&a=&&c="), "http://ya.ru/?a=&b==&c=");
        }

        Y_UNIT_TEST(TestParsingCorners) {
            TString s;

            UNIT_ASSERT_EQUAL(ProcessQargs("http://ya.ru/?=", s), TQueryArg::ProcessedOK);
            UNIT_ASSERT_EQUAL(ProcessQargs("http://ya.ru/?some", s), TQueryArg::ProcessedOK);
            UNIT_ASSERT_EQUAL(ProcessQargs("http://ya.ru/?some=", s), TQueryArg::ProcessedOK);
            UNIT_ASSERT_EQUAL(ProcessQargs("http://ya.ru/", s), TQueryArg::ProcessedOK);
            UNIT_ASSERT_EQUAL(ProcessQargs("http://ya.ru/?&", s), TQueryArg::ProcessedOK);
            UNIT_ASSERT_EQUAL(ProcessQargs("http://ya.ru/?&&", s), TQueryArg::ProcessedOK);
            UNIT_ASSERT_EQUAL(ProcessQargs("http://ya.ru/?some=", s), TQueryArg::ProcessedOK);
            UNIT_ASSERT_EQUAL(ProcessQargs("http://ya.ru/?some==", s), TQueryArg::ProcessedOK);
            UNIT_ASSERT_EQUAL(ProcessQargs("http://ya.ru/?some=&&", s), TQueryArg::ProcessedOK);

            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?="), "http://ya.ru/?=");
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?some=="), "http://ya.ru/?some==");
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?&&"), "http://ya.ru/?&&");
            UNIT_ASSERT_STRINGS_EQUAL(SortQargs("http://ya.ru/?a"), "http://ya.ru/?a");
        }

        Y_UNIT_TEST(TestFiltering) {
            UNIT_ASSERT_STRINGS_EQUAL(FilterQargs("http://ya.ru/?some=value", "missing"), "http://ya.ru/?some=value");
            UNIT_ASSERT_STRINGS_EQUAL(FilterQargs("http://ya.ru/?b=1&a=2", "b"), "http://ya.ru/?a=2");
            UNIT_ASSERT_STRINGS_EQUAL(FilterQargs("http://ya.ru/?b=1&a=2&a=3", "a"), "http://ya.ru/?b=1");
            UNIT_ASSERT_STRINGS_EQUAL(FilterQargs("http://ya.ru/?some=&another=", "another"), "http://ya.ru/?some=");
        }

        Y_UNIT_TEST(TestRemoveEmptyFeature) {
            TUri uri;
            uri.Parse("http://ya.ru/?", NUri::TFeature::FeaturesRecommended);

            TQueryArgProcessing processing(TQueryArg::FeatureRemoveEmptyQuery | TQueryArg::FeatureRewriteDirty);
            auto result = processing.Process(uri);
            UNIT_ASSERT_EQUAL(result, TQueryArg::ProcessedOK);
            UNIT_ASSERT_STRINGS_EQUAL(uri.PrintS(), "http://ya.ru/");
        }

        Y_UNIT_TEST(TestNoRemoveEmptyFeature) {
            TUri uri;
            uri.Parse("http://ya.ru/?", NUri::TFeature::FeaturesRecommended);

            TQueryArgProcessing processing(0);
            auto result = processing.Process(uri);
            UNIT_ASSERT_EQUAL(result, TQueryArg::ProcessedOK);
            UNIT_ASSERT_STRINGS_EQUAL(uri.PrintS(), "http://ya.ru/?");
        }
    }
}
