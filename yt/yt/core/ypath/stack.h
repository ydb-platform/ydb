#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>

#include <variant>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

class TYPathStack
{
public:
    using TEntry = std::variant<std::string, int>;

    DEFINE_BYREF_RO_PROPERTY(std::vector<TEntry>, Items);

public:
    void Push(TStringBuf key);
    void PushLiteral(std::string key);
    void Push(int index);
    void IncreaseLastIndex();
    void Pop();
    bool IsEmpty() const;
    const TYPath& GetPath() const;
    std::string GetHumanReadablePath() const;
    std::optional<std::string> TryGetStringifiedLastPathToken() const;

    void Reset();

private:
    mutable std::vector<size_t> PreviousPathLengths_;
    mutable TYPath Path_;
    mutable bool PathMaterialized_ = false;

    static std::string ToString(const TEntry& entry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
