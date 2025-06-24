#include "abstract.h"
#include <util/string/join.h>

namespace NKikimr::NKqp {

TConclusionStatus ICommand::DeserializeFromString(const TString& description) {
    try {
        auto lines = StringSplitter(description).SplitBySet("\n").ToList<TString>();
        const std::set<TString> props = GetCommandProperties();
        TString currentProperty;
        std::vector<TString> freeArguments;
        THashMap<TString, TString> properties;
        for (auto&& l : lines) {
            l = Strip(l);
            if (!l) {
                continue;
            }
            for (auto&& c : props) {
                if (l.StartsWith(c)) {
                    currentProperty = c;
                    l = Strip(l.substr(c.size()));
                }
                if (l.StartsWith(":") || l.StartsWith("=")) {
                    l = Strip(l.substr(1));
                }
            }
            if (!l) {
                continue;
            }
            if (!currentProperty) {
                freeArguments.emplace_back(l);
            } else {
                properties[currentProperty] += l;
            }
        }

        TPropertiesCollection collection(freeArguments, properties);
        auto result = DeserializeProperties(collection);
        if (result.IsFail()) {
            return TConclusionStatus::Fail(result.GetErrorMessage() + ":\n" + collection.DebugString());
        }
        return TConclusionStatus::Success();
    } catch (...) {
        return TConclusionStatus::Fail("exception on ICommand::DeserializeFromString: " + CurrentExceptionMessage());
    }
}

TString TPropertiesCollection::DebugString() const {
    TStringBuilder sb;
    sb << "FREE_ARGUMENTS(" << FreeArguments.size() << "):" << Endl;
    for (auto&& i : FreeArguments) {
        sb << "    " << i << Endl;
    }

    sb << "PROPERTIES(" << Properties.size() << "):" << Endl;
    for (auto&& i : Properties) {
        sb << "    " << i.first << ":" << i.second << Endl;
    }
    return sb;
}

TString TPropertiesCollection::JoinFreeArguments(const TString& delimiter /*= "\n"*/) const {
    return JoinSeq(delimiter, FreeArguments);
}

}   // namespace NKikimr::NKqp
