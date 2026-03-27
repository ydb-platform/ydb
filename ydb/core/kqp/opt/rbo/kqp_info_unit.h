#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr {
namespace NKqp {

/**
 * Info Unit is a reference to a column in the plan
 * Currently we only record the name and alias of the column, but we will extend it in the future
 */
struct TInfoUnit {
    TInfoUnit(const TString& alias, const TString& column, bool subplanContext = false)
        : Alias(alias)
        , ColumnName(column)
        , SubplanContext(subplanContext) {
    }

    TInfoUnit(const TString& name, bool subplanContext = false) : SubplanContext(subplanContext) {
        if (auto idx = name.find('.'); idx != TString::npos) {
            Alias = name.substr(0, idx);
            if (Alias.StartsWith("_alias_")) {
                Alias = Alias.substr(7);
            }
            ColumnName = name.substr(idx + 1);
        } else {
            Alias = "";
            ColumnName = name;
        }
    }
    TInfoUnit() = default;
    ~TInfoUnit() = default;

    TString GetFullName() const {
        return (Alias != "" ? Alias + "." : "") + ColumnName;
    }

    TString GetAlias() const { return Alias; }
    TString GetColumnName() const { return ColumnName; }
    bool IsSubplanContext() const { return SubplanContext; }
    void SetSubplanContext(bool subplanContext) { SubplanContext = subplanContext; }
    void AddDependencies(TVector<TInfoUnit> deps) { 
        SubplanDependencies.insert(SubplanDependencies.end(), deps.begin(), deps.end());
    }
    TVector<TInfoUnit> GetDependencies() const { return SubplanDependencies; }

    bool operator==(const TInfoUnit& other) const {
        return Alias == other.Alias && ColumnName == other.ColumnName;
    }

    struct THashFunction {
        size_t operator()(const TInfoUnit& c) const {
            return THash<TString>{}(c.Alias) ^ THash<TString>{}(c.ColumnName);
        }
    };

private:
    TString Alias;
    TString ColumnName;
    bool SubplanContext{false};
    TVector<TInfoUnit> SubplanDependencies;
};

}
}