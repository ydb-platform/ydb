#pragma once

#include <util/generic/string.h>
#include <util/generic/typetraits.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>

class TKeyValuePrinter {
private:
    TString Sep;
    TVector<TString> Keys;
    TVector<TString> Values;
    TVector<bool> AlignLefts;

public:
    TKeyValuePrinter(const TString& sep = TString(": "));
    ~TKeyValuePrinter();

    template <typename TKey, typename TValue>
    void AddRow(const TKey& key, const TValue& value, bool leftAlign = !std::is_integral<TValue>::value) {
        return AddRowImpl(ToString(key), ToString(value), leftAlign);
    }

    TString PrintToString() const;

private:
    void AddRowImpl(const TString& key, const TString& value, bool leftAlign);
};
