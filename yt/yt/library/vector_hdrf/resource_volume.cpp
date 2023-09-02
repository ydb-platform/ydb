#include "resource_volume.h"

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

using std::round;

TResourceVolume::TResourceVolume(const TJobResources& jobResources, TDuration duration)
{
    auto seconds = duration.SecondsFloat();

    #define XX(name, Name) Name##_ = static_cast<decltype(Name##_)>(jobResources.Get##Name() * seconds);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

double TResourceVolume::GetMinResourceRatio(const TJobResources& denominator) const
{
    double result = std::numeric_limits<double>::max();
    bool updated = false;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            result = std::min(result, static_cast<double>(a) / static_cast<double>(b));
            updated = true;
        }
    };
    #define XX(name, Name) update(Get##Name(), denominator.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return updated ? result : 0.0;
}

bool TResourceVolume::IsZero() const
{
    bool result = true;
    TResourceVolume::ForEachResource([&] (EJobResourceType /*resourceType*/, auto TResourceVolume::* resourceDataMember) {
        result = result && this->*resourceDataMember == 0;
    });
    return result;
}

TResourceVolume Max(const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    TResourceVolume result;
    #define XX(name, Name) result.Set##Name(std::max(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TResourceVolume Min(const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    TResourceVolume result;
    #define XX(name, Name) result.Set##Name(std::min(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

bool operator == (const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() == rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

TResourceVolume& operator += (TResourceVolume& lhs, const TResourceVolume& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() + rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TResourceVolume& operator -= (TResourceVolume& lhs, const TResourceVolume& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() - rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TResourceVolume& operator *= (TResourceVolume& lhs, double rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() * rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TResourceVolume& operator /= (TResourceVolume& lhs, double rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() / rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TResourceVolume operator + (const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    TResourceVolume result = lhs;
    result += rhs;
    return result;
}

TResourceVolume operator - (const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    TResourceVolume result = lhs;
    result -= rhs;
    return result;
}

TResourceVolume operator * (const TResourceVolume& lhs, double rhs)
{
    TResourceVolume result = lhs;
    result *= rhs;
    return result;
}

TResourceVolume operator / (const TResourceVolume& lhs, double rhs)
{
    TResourceVolume result = lhs;
    result /= rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

