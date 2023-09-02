#pragma once

#include <yt/yt/library/vector_hdrf/job_resources.h>

#include <yt/yt/library/numeric/binary_search.h>
#include <yt/yt/library/numeric/double_array.h>
#include <yt/yt/library/numeric/piecewise_linear_function.h>

#include <util/generic/cast.h>

#include <cmath>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

static constexpr double RatioComputationPrecision = 1e-9;
static constexpr double RatioComparisonPrecision = 1e-4;
static constexpr double InfiniteResourceAmount = 1e10;

////////////////////////////////////////////////////////////////////////////////

inline constexpr int GetResourceCount() noexcept
{
    int res = 0;
    #define XX(name, Name) do { res += 1; } while(false);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return res;
}

static constexpr int ResourceCount = GetResourceCount();
static_assert(TEnumTraits<EJobResourceType>::GetDomainSize() == ResourceCount);

class TResourceVector
    : public TDoubleArrayBase<ResourceCount, TResourceVector>
{
private:
    using TBase = TDoubleArrayBase<ResourceCount, TResourceVector>;

public:
    using TBase::TDoubleArrayBase;
    using TBase::operator[];

    Y_FORCE_INLINE double& operator[](EJobResourceType resourceType)
    {
        static_assert(TEnumTraits<EJobResourceType>::GetDomainSize() == ResourceCount);
        return (*this)[GetIdByResourceType(resourceType)];
    }

    Y_FORCE_INLINE const double& operator[](EJobResourceType resourceType) const
    {
        static_assert(TEnumTraits<EJobResourceType>::GetDomainSize() == ResourceCount);
        return (*this)[GetIdByResourceType(resourceType)];
    }

    static TResourceVector FromJobResources(
        const TJobResources& resources,
        const TJobResources& totalLimits,
        double zeroDivByZero = 0.0,
        double oneDivByZero = 0.0);

    static constexpr TResourceVector SmallEpsilon()
    {
        return FromDouble(RatioComputationPrecision);
    }

    static constexpr TResourceVector Epsilon()
    {
        return FromDouble(RatioComparisonPrecision);
    }

    static constexpr TResourceVector Infinity()
    {
        return FromDouble(InfiniteResourceAmount);
    }

    Y_FORCE_INLINE static constexpr int GetIdByResourceType(EJobResourceType resourceType)
    {
        return static_cast<int>(resourceType);
    }

    Y_FORCE_INLINE static constexpr EJobResourceType GetResourceTypeById(int resourceId)
    {
        return static_cast<EJobResourceType>(resourceId);
    }
};

inline TJobResources operator*(const TJobResources& lhs, const TResourceVector& rhs)
{
    using std::round;

    TJobResources result;
    #define XX(name, Name) do { \
        auto newValue = round(lhs.Get##Name() * rhs[EJobResourceType::Name]); \
        result.Set##Name(static_cast<decltype(lhs.Get##Name())>(newValue)); \
    } while (false);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

////////////////////////////////////////////////////////////////////////////////

using TVectorPiecewiseSegment = TPiecewiseSegment<TResourceVector>;
using TScalarPiecewiseSegment = TPiecewiseSegment<double>;
using TVectorPiecewiseLinearFunction = TPiecewiseLinearFunction<TResourceVector>;
using TScalarPiecewiseLinearFunction = TPiecewiseLinearFunction<double>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

