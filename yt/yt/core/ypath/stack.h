#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>

#include <variant>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

class TYPathStack
{
public:
    using TEntry = std::variant<
        TString,
        int>;

    DEFINE_BYREF_RO_PROPERTY(std::vector<TEntry>, Items);

public:
    void Push(TStringBuf key);
    void Push(int index);
    void IncreaseLastIndex();
    void Pop();
    bool IsEmpty() const;
    const TYPath& GetPath() const;
    TString GetHumanReadablePath() const;
    std::optional<TString> TryGetStringifiedLastPathToken() const;

    void Reset();

private:
    std::vector<size_t> PreviousPathLengths_;
    TYPath Path_;

    static TString ToString(const TEntry& entry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
