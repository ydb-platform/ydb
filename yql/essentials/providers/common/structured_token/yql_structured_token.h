#pragma once

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYql {

class TStructuredToken {
public:
    explicit TStructuredToken(TMap<TString, TString>&& data = {});
    TStructuredToken(TStructuredToken&&) = default;
    TStructuredToken(const TStructuredToken&) = default;

    TString GetField(const TString& name) const;
    TString GetFieldOrDefault(const TString& name, const TString& defaultValue) const;
    TMaybe<TString> FindField(const TString& name) const;
    bool HasField(const TString& name) const;
    TStructuredToken& SetField(const TString& name, const TString& value);
    TStructuredToken& ClearField(const TString& name);
    TString ToJson() const;

private:
    TMap<TString, TString> Data;
};

// is used for backward compatibility when content contains just token
bool IsStructuredTokenJson(const TString& content);
TStructuredToken ParseStructuredToken(const TString& content);

} // namespace NYql
