// Regenerates pg_collation_icu.generated.h - the checked-in list of ICU collation locale
// names known to pg_catalog. Run manually and commit the result whenever the vendored ICU
// (contrib/libs/icu) is updated:
//
//   ya make yql/essentials/parser/pg_catalog/gen_icu_collations
//   yql/essentials/parser/pg_catalog/gen_icu_collations/gen_icu_collations \
//       yql/essentials/parser/pg_catalog/pg_collation_icu.generated.h
//
// The output is append-only: existing entries are read back first and kept in place, and
// only locales not already present are appended at the end. This keeps each locale's
// position in the file - and hence the collation oid pg_catalog derives from that position
// - stable across regenerations, even if the set of locales ICU knows about changes,
// exactly like AllStaticTablesRaw/pg_class.generated.h and library/cpp/type_info/tz do for
// their own generated tables.

#include <unicode/uloc.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

#include <cstdio>

namespace {

// Canonicalizes an ICU locale id (e.g. "de_DE") to its BCP47 language tag (e.g. "de-DE").
// Returns an empty string if ICU can't parse it at all.
TString CanonicalizeIcuLocale(const TString& locale) {
    UErrorCode status = U_ZERO_ERROR;
    std::array<char, ULOC_FULLNAME_CAPACITY> tag;
    auto len = uloc_toLanguageTag(locale.c_str(), tag.data(), tag.size(), /* strict */ true, &status);
    if (U_FAILURE(status)) {
        return {};
    }

    return TString(tag.data(), len);
}

TVector<TString> ReadExistingLocales(const TString& path) {
    TVector<TString> result;
    if (!NFs::Exists(path)) {
        return result;
    }

    TFileInput input(path);
    TString line;
    while (input.ReadLine(line)) {
        auto endQuote = line.find('"', 1);
        Y_ENSURE(line.size() > 2 && line[0] == '"' && endQuote != TString::npos, "Malformed line in " << path << ": " << line);
        result.push_back(line.substr(1, endQuote - 1));
    }

    return result;
}

TVector<TString> DiscoverIcuLocales() {
    TVector<TString> result;
    if (auto root = CanonicalizeIcuLocale(""); !root.empty()) {
        result.push_back(root);
    }

    for (i32 i = 0; i < uloc_countAvailable(); ++i) {
        if (auto tag = CanonicalizeIcuLocale(uloc_getAvailable(i)); !tag.empty()) {
            result.push_back(tag);
        }
    }

    Sort(result.begin(), result.end());
    result.erase(Unique(result.begin(), result.end()), result.end());
    return result;
}

} // namespace

int main(int argc, char** argv) {
    Y_ENSURE(argc == 2, "usage: gen_icu_collations <path-to-pg_collation_icu.generated.h>");
    TString outputPath = argv[1];

    auto existing = ReadExistingLocales(outputPath);
    THashSet<TString> seen(existing.begin(), existing.end());

    TVector<TString> toAppend;
    for (auto& tag : DiscoverIcuLocales()) {
        if (seen.insert(tag).second) {
            toAppend.push_back(tag);
        }
    }

    {
        TFileOutput output(outputPath);
        for (auto& tag : existing) {
            output << "\"" << tag << "\",\n";
        }

        for (auto& tag : toAppend) {
            output << "\"" << tag << "\",\n";
        }
    }

    Cerr << "Wrote " << (existing.size() + toAppend.size()) << " ICU collation locales ("
         << toAppend.size() << " new) to " << outputPath << Endl;
    return 0;
}
