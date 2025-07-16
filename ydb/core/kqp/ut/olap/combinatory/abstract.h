#pragma once
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

class TPropertiesCollection {
private:
    std::vector<TString> FreeArguments;
    THashMap<TString, TString> Properties;

public:
    TString DebugString() const;

    TString JoinFreeArguments(const TString& delimiter = "\n") const;

    TPropertiesCollection(const std::vector<TString>& freeArgs, const THashMap<TString, TString>& props)
        : FreeArguments(freeArgs)
        , Properties(props) {
    }

    std::optional<TString> GetOptional(const TString& propertyName) const {
        auto it = Properties.find(propertyName);
        if (it == Properties.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    TString GetVerified(const TString& propertyName) const {
        auto it = Properties.find(propertyName);
        AFL_VERIFY(it != Properties.end())("name", propertyName)("props", DebugString());
        return it->second;
    }

    ui32 GetFreeArgumentsCount() const {
        return FreeArguments.size();
    }

    std::optional<TString> GetFreeArgumentOptional(const ui32 idx) const {
        if (idx < FreeArguments.size()) {
            return std::nullopt;
        }
        return FreeArguments[idx];
    }

    TString GetFreeArgumentVerified(const ui32 idx) const {
        AFL_VERIFY(idx < FreeArguments.size())("idx", idx)("props", DebugString());
        return FreeArguments[idx];
    }
};

class ICommand {
private:
    virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) = 0;
    virtual std::set<TString> DoGetCommandProperties() const {
        return {};
    }
    virtual TConclusionStatus DoDeserializeProperties(const TPropertiesCollection& /*props*/) {
        return TConclusionStatus::Success();
    }
    TConclusionStatus DeserializeProperties(const TPropertiesCollection& props) {
        return DoDeserializeProperties(props);
    }

public:
    virtual ~ICommand() = default;

    std::set<TString> GetCommandProperties() const {
        return DoGetCommandProperties();
    }

    TConclusionStatus DeserializeFromString(const TString& description);

    TConclusionStatus Execute(TKikimrRunner& kikimr) {
        return DoExecute(kikimr);
    }
};

}   // namespace NKikimr::NKqp
