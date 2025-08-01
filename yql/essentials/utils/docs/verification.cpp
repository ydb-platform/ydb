#include "verification.h"

#include "name.h"

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/case_insensitive_string/case_insensitive_string.h>

#include <util/string/builder.h>

namespace NYql::NDocs {

    auto Fames = {
        EFame::BadLinked,
        EFame::Unknown,
        EFame::Mentioned,
        EFame::Documented,
    };

    bool IsLikelyDocumentedAt(TString text, TString name) {
        SubstGlobal(text, "_", "");

        TVector<TStringBuf> tokens;
        Split(name, ":.", tokens);

        for (TStringBuf token : tokens) {
            YQL_ENSURE(!token.Empty());

            TMaybe<TString> normalized = NormalizedName(TString(token));
            YQL_ENSURE(normalized, "Unable to normalize " << token);

            if (TCaseInsensitiveAsciiString(text).Contains(*normalized)) {
                return true;
            }
        }
        return false;
    }

    void Verify(const TLinks& links, const TPages& pages, TString name, TFameReport& report) {
        TMaybe<TLinkTarget> target = Lookup(links, name);
        if (!target) {
            report[EFame::Unknown][std::move(name)] = "Unknown";
            return;
        }

        const TMarkdownPage* page = pages.FindPtr(target->RelativePath);
        if (!page) {
            report[EFame::BadLinked][std::move(name)] =
                TStringBuilder()
                << "Page '" << target->RelativePath << "' not found";
            return;
        }

        if (!target->Anchor && !IsLikelyDocumentedAt(page->Text, name)) {
            report[EFame::BadLinked][std::move(name)] =
                TStringBuilder()
                << "Absent at '" << target->RelativePath << "'";
            return;
        }

        if (!target->Anchor) {
            report[EFame::Mentioned][std::move(name)] =
                TStringBuilder()
                << "Mentioned at '" << target->RelativePath << "'";
            return;
        }

        const TMarkdownSection* section = page->SectionsByAnchor.FindPtr(*target->Anchor);
        if (!section) {
            report[EFame::BadLinked][std::move(name)] =
                TStringBuilder()
                << "Section '" << *target->Anchor << "' not found "
                << "at '" << target->RelativePath << "'";
            return;
        }

        if (!IsLikelyDocumentedAt(section->Header.Content, name) &&
            !IsLikelyDocumentedAt(section->Body, name)) {
            report[EFame::BadLinked][std::move(name)] =
                TStringBuilder()
                << "Absent at section '" << target << "'";
            return;
        }

        report[EFame::Documented][std::move(name)] =
            TStringBuilder()
            << "Documented at '" << target << "'";
    }

    void ExamineShortHands(TFameReport& report, const TMap<TString, TString>& shortHands) {
        for (const auto& [shorten, qualified] : shortHands) {
            report[EFame::BadLinked].erase(shorten);
            for (EFame fame : Fames) {
                auto it = report[fame].find(qualified);
                if (it != report[fame].end()) {
                    report[fame][shorten] = it->second;
                }
            }
        }
    }

    TFameReport Verify(TVerificationInput input) {
        TFameReport report;
        for (TString name : input.Names) {
            Verify(input.Links, input.Pages, std::move(name), report);
        }
        ExamineShortHands(report, input.ShortHands);
        return report;
    }

    double Coverage(const TFameReport& report, const TVector<TString>& names) {
        if (!report.contains(EFame::Documented)) {
            return 0;
        }

        const TStatusesByName& documented = report.at(EFame::Documented);

        size_t covered = 0;
        for (const TString& name : names) {
            covered += documented.contains(name) ? 1 : 0;
        }

        return static_cast<double>(covered) / names.size();
    }

} // namespace NYql::NDocs

template <>
void Out<NYql::NDocs::EFame>(IOutputStream& out, NYql::NDocs::EFame fame) {
    switch (fame) {
        case NYql::NDocs::EFame::BadLinked:
            out << "BadLinked";
            break;
        case NYql::NDocs::EFame::Unknown:
            out << "Unknown";
            break;
        case NYql::NDocs::EFame::Mentioned:
            out << "Mentioned";
            break;
        case NYql::NDocs::EFame::Documented:
            out << "Documented";
            break;
    }
}
