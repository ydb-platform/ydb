#pragma once

#include "fwd.h"

#include <library/cpp/yson/node/node.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

const TNode::TMapType& GetDynamicConfiguration(const IClientPtr& client, const TString& configProfile);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

// |NYT::TConfig| field supporting per-client overriding with cluster configuration.
// |T| must be one of |TNode| value types.
template <typename T>
class TPatchableField
{
public:
    static constexpr char ConfigProfile[] = "default";

    TPatchableField(TString name, T defaultValue)
        : Name_(std::move(name))
        , Value_(std::move(defaultValue))
    { }

    const T& Get(const IClientPtr& client)
    {
        if (!Patched_) {
            const auto& clusterConfig = NDetail::GetDynamicConfiguration(client, ConfigProfile);
            auto iter = clusterConfig.find(Name_);
            if (!iter.IsEnd()) {
                if (iter->second.IsOfType<T>()) {
                    Value_ = iter->second.As<T>();
                } else {
                    ythrow yexception() << "operation_link_pattern must be string";
                }
            }
        }
        return Value_;
    }

    void Set(const T& value)
    {
        Value_ = value;
        Patched_ = true;
    }

private:
    TString Name_;
    T Value_;
    bool Patched_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
