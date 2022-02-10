#include "tld.h"

#include <library/cpp/digest/lower_case/hash_ops.h>

#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>

namespace NTld {
    namespace {
#include <library/cpp/tld/tld.inc>

        using TCiHash = THashSet<TStringBuf, TCIOps, TCIOps>;

        struct TTLDHash: public TCiHash {
            TTLDHash() {
                for (auto tld = GetTlds(); *tld; ++tld) {
                    insert(*tld);
                }
            }
        };

        struct TVeryGoodTld: public TCiHash {
            TVeryGoodTld() {
                auto domains = {
                    "am", "az", "biz", "by", "com", "cz", "de", "ec", "fr", "ge", "gov",
                    "gr", "il", "info", "kg", "kz", "mobi", "net", "nu", "org", "lt", "lv",
                    "md", "ru", "su", "tr", "ua", "uk", "uz", "ws", "xn--p1ai", "рф"};

                for (auto d : domains) {
                    insert(d);
                }
            }
        };
    }

    const char* const* GetTlds() {
        return TopLevelDomains;
    }

    bool IsTld(const TStringBuf& s) {
        return Default<TTLDHash>().contains(s);
    }

    bool IsVeryGoodTld(const TStringBuf& s) {
        return Default<TVeryGoodTld>().contains(s);
    }

}
