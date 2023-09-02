#include "job_resources.h"

namespace NYT::NVectorHdrf {

using std::round;

////////////////////////////////////////////////////////////////////////////////

TJobResources TJobResources::Infinite()
{
    TJobResources result;
#define XX(name, Name) result.Set##Name(std::numeric_limits<decltype(result.Get##Name())>::max() / 4);
    ITERATE_JOB_RESOURCES(XX)
#undef XX
    return result;
}

////////////////////////////////////////////////////////////////////////////////

EJobResourceType GetDominantResource(
    const TJobResources& demand,
    const TJobResources& limits)
{
    auto maxType = EJobResourceType::Cpu;
    double maxRatio = 0.0;
    auto update = [&] (auto a, auto b, EJobResourceType type) {
        if (static_cast<double>(b) > 0.0) {
            double ratio = static_cast<double>(a) / static_cast<double>(b);
            if (ratio > maxRatio) {
                maxRatio = ratio;
                maxType = type;
            }
        }
    };
    #define XX(name, Name) update(demand.Get##Name(), limits.Get##Name(), EJobResourceType::Name);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return maxType;
}

double GetDominantResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits)
{
    double maxRatio = 0.0;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            double ratio = static_cast<double>(a) / static_cast<double>(b);
            if (ratio > maxRatio) {
                maxRatio = ratio;
            }
        }
    };
    #define XX(name, Name) update(usage.Get##Name(), limits.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return maxRatio;
}

double GetResource(const TJobResources& resources, EJobResourceType type)
{
    switch (type) {
        #define XX(name, Name) \
            case EJobResourceType::Name: \
                return static_cast<double>(resources.Get##Name());
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
        default:
            Y_FAIL();
    }
}

void SetResource(TJobResources& resources, EJobResourceType type, double value)
{
    switch (type) {
        #define XX(name, Name) \
            case EJobResourceType::Name: \
                resources.Set##Name(value); \
                break;
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
        default:
            Y_FAIL();
    }
}

double GetMinResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator)
{
    double result = std::numeric_limits<double>::max();
    bool updated = false;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            result = std::min(result, static_cast<double>(a) / static_cast<double>(b));
            updated = true;
        }
    };
    #define XX(name, Name) update(nominator.Get##Name(), denominator.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return updated ? result : 0.0;
}

double GetMaxResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator)
{
    double result = 0.0;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            result = std::max(result, static_cast<double>(a) / static_cast<double>(b));
        }
    };
    #define XX(name, Name) update(nominator.Get##Name(), denominator.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources operator + (const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(lhs.Get##Name() + rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources& operator += (TJobResources& lhs, const TJobResources& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() + rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources operator - (const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(lhs.Get##Name() - rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources& operator -= (TJobResources& lhs, const TJobResources& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() - rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources operator * (const TJobResources& lhs, i64 rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(lhs.Get##Name() * rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources operator * (const TJobResources& lhs, double rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(static_cast<decltype(lhs.Get##Name())>(round(lhs.Get##Name() * rhs)));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources& operator *= (TJobResources& lhs, i64 rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() * rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources& operator *= (TJobResources& lhs, double rhs)
{
    #define XX(name, Name) lhs.Set##Name(static_cast<decltype(lhs.Get##Name())>(round(lhs.Get##Name() * rhs)));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources  operator - (const TJobResources& resources)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(-resources.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

bool operator == (const TJobResources& lhs, const TJobResources& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() == rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

bool operator != (const TJobResources& lhs, const TJobResources& rhs)
{
    return !(lhs == rhs);
}

bool Dominates(const TJobResources& lhs, const TJobResources& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() >= rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

bool StrictlyDominates(const TJobResources& lhs, const TJobResources& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() > rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

TJobResources Max(const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(std::max(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources Min(const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(std::min(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool TJobResourcesConfig::IsNonTrivial()
{
    bool isNonTrivial = false;
    ForEachResource([this, &isNonTrivial] (auto TJobResourcesConfig::* resourceDataMember, EJobResourceType /*resourceType*/) {
        isNonTrivial = isNonTrivial || (this->*resourceDataMember).has_value();
    });
    return isNonTrivial;
}

bool TJobResourcesConfig::IsEqualTo(const TJobResourcesConfig& other)
{
    bool result = true;
    ForEachResource([this, &result, &other] (auto TJobResourcesConfig::* resourceDataMember, EJobResourceType /*resourceType*/) {
        result = result && (this->*resourceDataMember == other.*resourceDataMember);
    });
    return result;
}

TJobResourcesConfig& TJobResourcesConfig::operator+=(const TJobResourcesConfig& addend)
{
    ForEachResource([this, &addend] (auto TJobResourcesConfig::* resourceDataMember, EJobResourceType /*resourceType*/) {
        if (!(addend.*resourceDataMember).has_value()) {
            return;
        }
        if ((this->*resourceDataMember).has_value()) {
            *(this->*resourceDataMember) += *(addend.*resourceDataMember);
        } else {
            this->*resourceDataMember = addend.*resourceDataMember;
        }
    });
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

