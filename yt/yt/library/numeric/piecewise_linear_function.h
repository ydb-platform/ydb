#pragma once

#include "algorithm_helpers.h"
#include "binary_search.h"

#include <library/cpp/testing/gtest/friend.h>

#include <util/system/yassert.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

#include <algorithm>
#include <optional>
#include <random>
#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static_assert(std::numeric_limits<double>::is_iec559, "We assume IEEE 754 floating point implementation");

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TPiecewiseSegment
{
public:
    using TSegment = TPiecewiseSegment<TValue>;

public:
    TPiecewiseSegment(std::pair<double, TValue> leftPoint, std::pair<double, TValue> rightPoint);

    double LeftBound() const;

    double RightBound() const;

    const TValue& LeftValue() const;

    const TValue& RightValue() const;

    bool IsDefinedAt(double x) const;

    bool IsDefinedOn(double left, double right) const;

    TValue LeftLimitAt(double x) const;

    TValue RightLimitAt(double x) const;

    std::pair<TValue, TValue> LeftRightLimitAt(double x) const;

    TValue ValueAt(double x) const;

    // Returns true when |LeftBound() == RightBound() && LeftValue() != RightValue()|.
    bool IsVertical() const;

    // Returns true when |LeftBound() != RightBound() && LeftValue() == RightValue()|.
    bool IsHorizontal() const;

    // Returns true when |LeftBound() == RightBound() && LeftValue() == RightValue()|.
    bool IsPoint() const;

    // Returns true when |LeftBound() != RightBound() && LeftValue() != RightValue()|.
    bool IsTilted() const;

    TPiecewiseSegment Transpose() const;

    // See: |TPiecewiseLinearFunction<TValue>::ScaleArgument|.
    TPiecewiseSegment ScaleArgument(double scale) const;

    TPiecewiseSegment Shift(double deltaBound, const TValue& deltaValue) const;

public:
    double LeftBound_;
    TValue LeftValue_;
    double RightBound_;
    TValue RightValue_;

private:
    TValue InterpolateAt(double x) const;
    TValue InterpolateNormalized(double t) const;

    FRIEND_TEST(TPiecewiseLinearFunctionTest, TestInterpolationProperties);
};

template <class TValue>
bool operator ==(const TPiecewiseSegment<TValue>& lhs, const TPiecewiseSegment<TValue>& rhs);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TPiecewiseLinearFunction;

template <class TValue>
void ExtractCriticalPointsFromFunction(const TPiecewiseLinearFunction<TValue>& func, std::vector<double>* out);

template <class TValue>
void ExtractCriticalPointsFromFunctionWithValues(
    const TPiecewiseLinearFunction<TValue>& func,
    std::vector<std::pair<double, TValue>>* out);

template <class TValue>
void ExtractDiscontinuityPointsFromFunction(
    const TPiecewiseLinearFunction<TValue>& func,
    std::vector<double>* out);

template <class TValue>
void PushSegmentImpl(std::vector<TPiecewiseSegment<TValue>>* vec, TPiecewiseSegment<TValue> segment);

template <class TValue>
TPiecewiseLinearFunction<TValue> PointwiseMin(
    const TPiecewiseLinearFunction<TValue>& lhs,
    const TPiecewiseLinearFunction<TValue>& rhs);

template <class TValue>
TPiecewiseLinearFunction<TValue> PointwiseMin(const std::vector<TPiecewiseLinearFunction<TValue>>& funcs);

void ClearAndSortCriticalPoints(std::vector<double>* vec, double leftBound, double rightBound);

////////////////////////////////////////////////////////////////////////////////

template <class>
struct TIsPiecewiseLinearFunction : std::false_type {};

template <class TValue>
struct TIsPiecewiseLinearFunction<TPiecewiseLinearFunction<TValue>> : std::true_type {};

template <class TValue>
constexpr inline bool IsPiecewiseLinearFunction = TIsPiecewiseLinearFunction<TValue>::value;

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TPiecewiseLinearFunctionBuilder
{
public:
    using TPoint = std::pair<double, TValue>;
    using TSegment = TPiecewiseSegment<TValue>;
    using TFunction = TPiecewiseLinearFunction<TValue>;

public:
    TPiecewiseLinearFunctionBuilder() = default;

    void AddPoint(TPoint point);

    void PushSegment(const TSegment& segment);

    void PushSegment(std::pair<double, TValue> leftPoint, std::pair<double, TValue> rightPoint);

    TFunction Finish();

private:
    std::optional<TPoint> RightPoint_;
    std::vector<TSegment> Segments_;
};

////////////////////////////////////////////////////////////////////////////////

// Represents a (potentially discontinuous) piecewise linear function defined on a closed interval of real numbers.
// The function can be sampled using methods: |ValueAt(x)|, |LeftLimitAt(x)|, |RightLimitAt(x)|, |LeftRightLimitAt(x)|.
// New functions can be created using |TPiecewiseLinearFunctionBuilder| class.
// Currently only left-continuous functions are supported, i.e. for all functions |ValueAt(x) == LeftLimitAt(x)|.
//
// The results of all sampling functions preserve monotonicity.
// I.e. if the function is non-decreasing, the following properties are satisfied for all valid |x|, |x1| and |x2|:
// (1) Left-hand limit at a point is not greater than the right-hand limit.
//      I.e. |LeftLimitAt(x) <= RightLimitAt(x)|.
// (2) |Get|, |LeftLimitAt| and |RightLimitAt| are non-decreasing.
//      I.e. if |x1 <= x2|, then |ValueAt(x1) <= ValueAt(x2)|, |LeftLimitAt(x1) <= LeftLimitAt(x2)| and |RightLimitAt(x1) <= RightLimitAt(x2)|.
// (3) if |x1 < x2|, then |RightLimitAt(x1) <= LeftLimitAt(x2)|.
//
// A function is represented by its plot.
// Discontinuity points are represented by vertical segments in the plot.
template <class TValue>
class TPiecewiseLinearFunction
{
public:
    using TValueType = TValue;
    using TSelf = TPiecewiseLinearFunction<TValue>;
    using TBuilder = TPiecewiseLinearFunctionBuilder<TValue>;
    using TSegment = TPiecewiseSegment<TValue>;

public:
    explicit TPiecewiseLinearFunction(std::vector<TSegment> segments);

    TPiecewiseLinearFunction(const TPiecewiseLinearFunction& other) = default;
    TPiecewiseLinearFunction(TPiecewiseLinearFunction&& other) noexcept = default;

    TPiecewiseLinearFunction& operator=(const TPiecewiseLinearFunction& other) = default;
    TPiecewiseLinearFunction& operator=(TPiecewiseLinearFunction&& other) noexcept = default;

    // Creates a new piecewise linear function from given sample function and a set of critical points.
    // It is guaranteed that |sample| will be called in ascending order of arguments.
    // The total complexity is linear if |criticalPoints| is sorted and the complexity of |sample| is constant.
    template <class TSampleFunction>
    static TSelf Create(
        TSampleFunction&& sample,
        double leftBound,
        double rightBound,
        std::vector<double> criticalPoints);

    static TSelf Linear(double leftBound, TValue leftValue, double rightBound, TValue rightValue);

    static TSelf Constant(double leftBound, double rightBound, TValue value);

    // Returns left-hand limit of the function at point |x|.
    TValue LeftLimitAt(double x) const;

    // Returns right-hand limit of the function at point |x|.
    TValue RightLimitAt(double x) const;

    // Returns both left-hand and right-hand limit of the function at point |x|.
    // They might differ if |x| is a discontinuity point.
    std::pair<TValue, TValue> LeftRightLimitAt(double x) const;

    // Returns the value of the function at point |x|.
    // It must be equal to either left-hand limit or right-hand limit.
    TValue ValueAt(double x) const;

    // Returns a const reference to the vector of all segments of the function plot.
    const std::vector<TSegment>& Segments() const;

    // Returns minimal valid argument for the function.
    double LeftFunctionBound() const;

    // Returns maximal valid argument for the function.
    double RightFunctionBound() const;

    // Returns minimal valid argument for the function.
    TValue LeftFunctionValue() const;

    // Returns the function value at |RightFunctionBound()|.
    // This function is not defined if there is a discontinuity point.
    TValue RightFunctionValue() const;

    // Return true iff the function is defined at point |x|.
    bool IsDefinedAt(double x) const;

    // Returns true iff the function has no discontinuity points.
    bool IsContinuous() const;

    // Returns true iff the function is nondecreasing.
    // I.e. for all |x1|, |x2|: if |x1 <= x2|, then |ValueAt(x1) <= ValueAt(x2)|.
    // NB: This function is only defined when |operator<=| is defined for |TValue|.
    bool IsNondecreasing() const;

    // See: |Trim|.
    bool IsTrimmed() const;

    // See: |TrimLeft|.
    bool IsTrimmedLeft() const;

    // See: |TrimRight|.
    bool IsTrimmedRight() const;

    // Returns the leftmost segment that contains |x|.
    // |LeftSegmentAt(x).LeftLimitAt(x)| is guaranteed to be equal to |LeftLimitAt(x)|.
    const TSegment& LeftSegmentAt(double x, int* segmentIndex = nullptr) const;

    // Returns the rightmost segment that contains |x|.
    // |RightSegmentAt(x).RightLimitAt(x)| is guaranteed to be equal to |RightLimitAt(x)|.
    const TSegment& RightSegmentAt(double x, int* segmentIndex = nullptr) const;

    // Returns a segment that has both left limit and right limit for |x|.
    // I.e. |SegmentAt(x).LeftRightLimitAt(x)| is guaranteed to be equal to |LeftRightLimitAt(x)|.
    // It might differ from both |LeftSegmentAt| and |RightSegmentAt| when |x| is a discontinuity point.
    // The returned segment is vertical when |x| is a discontinuity point.
    const TSegment& SegmentAt(double x, int* segmentIndex = nullptr) const;

    // Adds segment to the right of the function plot.
    // The left point of |segment| must be equal to the right point of
    // the last already added segment (i.e. |Segments().back()|.
    void PushSegment(const TSegment& segment);

    // Transposition can be used to obtain the inverse function.
    // Returns a reference to |*this| for chaining.
    TSelf& TransposeInplace();

    // See: |TransposeInplace|.
    [[nodiscard]] TSelf Transpose() const &;

    // See: |TransposeInplace|.
    // Rvalue version is used for efficient chaining.
    [[nodiscard]] TSelf Transpose() &&;

    // Removes a discontinuity point at the left bound if there is one.
    // Returns a reference to |*this| for chaining.
    TSelf& TrimLeftInplace();

    // See: |TrimLeftInplace|.
    [[nodiscard]] TSelf TrimLeft() const &;

    // See: |TrimLeftInplace|.
    // Rvalue version is used for efficient chaining.
    [[nodiscard]] TSelf TrimLeft() &&;

    // Removes a discontinuity point at the right bound if there is one.
    // Returns a reference to |*this| for chaining.
    TSelf& TrimRightInplace();

    // See: |TrimRightInplace|.
    [[nodiscard]] TSelf TrimRight() const &;

    // See: |TrimRightInplace|.
    // Rvalue version is used for efficient chaining.
    [[nodiscard]] TSelf TrimRight() &&;

    // Removes non-sensible discontinuity points at the bounds of the functions.
    // Returns a reference to |*this| for chaining.
    TSelf& TrimInplace();

    // See: |TrimInplace|.
    [[nodiscard]] TSelf Trim() const &;

    // See: |TrimInplace|.
    // Rvalue version is used for efficient chaining.
    [[nodiscard]] TSelf Trim() &&;

    // Returns new function |g| s.t. |g.ValueAt(x) == this->ValueAt(x * scale)|.
    // NOTE: This means that the inner representation of the function (as well as its domain) will be scaled by (1.0 / |scale|).
    [[nodiscard]] TSelf ScaleArgument(double scale) const;

    [[nodiscard]] TSelf Shift(double deltaArgument, const TValue& deltaValue = TValue{}) const;


    // Narrows the domain of the function to the segment [|newLeftBound|, |newRightBound|].
    // Returns a reference to |*this| for chaining.
    TSelf& NarrowInplace(double newLeftBound, double newRightBound);

    // See: |NarrowInplace|.
    [[nodiscard]] TSelf Narrow(double newLeftBound, double newRightBound) const;

    // Extends the domain of the function to the segment [|newLeftBound|, |newRightBound|].
    // The function value is |newLeftValue| on the segment [|newLeftBound|, |LeftFunctionBound()|],
    // and |newRightValue| on the segment [|RightFunctionBound()|, |newRightBound|].
    // Returns a reference to |*this| for chaining.
    TSelf& ExtendInplace(
        double newLeftBound,
        const TValue& newLeftValue,
        double newRightBound,
        const TValue& newRightValue);

    // See: |ExtendInplace|.
    [[nodiscard]] TSelf Extend(
        double newLeftBound,
        const TValue& newLeftValue,
        double newRightBound,
        const TValue& newRightValue) const;

    // Equivalent to |Extend(newLeftBound, Segments().front().LeftValue(), newRightBound, Segments_.back().RightValue())|.
    [[nodiscard]] TSelf Extend(double newLeftBound, double newRightBound) const;

    // Equivalent to |Extend(LeftFunctionBound(), LeftFunctionValue(), newRightBound, newRightValue)|
    [[nodiscard]] TSelf ExtendRight(double newRightBound, const TValue& newRightValue) const;

    // Equivalent to |ExtendRight(newRightBound, Segments().back().RightValue())|
    [[nodiscard]] TSelf ExtendRight(double newRightBound) const;

    // Returns function |g| such that |g.ValueAt(x) == this->ValueAt(x) + other.ValueAt(x)| for all |x| in the intersection of
    // the domains of |*this| and |other|.
    [[nodiscard]] TSelf operator+(const TSelf& other) const;

    TSelf& operator+=(const TSelf& other);

    // Returns function |h| such that |h.ValueAt(x) == sum(funcs[i].ValueAt(x))| for all |x| in the intersections of
    // the domains of the functions in |funcs|.
    [[nodiscard]] static TSelf Sum(const std::vector<TPiecewiseLinearFunction<TValue>>& funcs);

    // Returns left composition of function |f=*this| and |g=other|,
    // i.e. a function |h| such that |h.ValueAt(x) = f.ValueAt(g.ValueAt(x))| for all |x| in the domain of |g|.
    // |other| must be non-decreasing.
    // If |*this| is non-decreasing, the result is guaranteed to be non-decreasing.
    template <class TOther>
    [[nodiscard]] TSelf Compose(const TOther& other) const;

public:
    // Helper class that can be used for efficient amortized access to |TPiecewiseLinearFunction|.
    // The behaviour is undefined if the function changes during traversal or if the requests are not monotonous.
    class TLeftToRightTraverser
    {
    public:
        TLeftToRightTraverser(const TPiecewiseLinearFunction& function, int segmentIndex = 0);

        // See: |TPiecewiseLinearFunction::LeftSegmentAt|.
        // If |y > x|, |LeftSegmentAt(x)| cannot be called after |LeftSegmentAt(y)|.
        // If |y >= x|, |LeftSegmentAt(x)| cannot be called after |SegmentAt(y)| or |RightSegmentAt(y)|.
        const TSegment& LeftSegmentAt(double x);

        // See: |TPiecewiseLinearFunction::RightSegmentAt|.
        // If |y > x|, |RightSegmentAt(x)| cannot be called after |LeftSegmentAt(y)|, |SegmentAt(y)|, or |RightSegmentAt(y)|.
        const TSegment& RightSegmentAt(double x);

        // See: |TPiecewiseLinearFunction::SegmentAt|.
        // If |y > x|, |SegmentAt(x)| cannot be called after |LeftSegmentAt(y)| or |SegmentAt(x)|.
        // If |y >= x|, |LeftSegmentAt(x)| cannot be called after |RightSegmentAt(x)|.
        const TSegment& SegmentAt(double x);

        // See: |TPiecewiseLinearFunction::ValueAt|.
        // Since the function is assumed to be left-continuous, equivalent to |LeftLimitAt(x)|.
        TValue ValueAt(double x);

        // See: |TPiecewiseLinearFunction::LeftLimitAt|.
        // Equivalent to |LeftSegmentAt(x).LeftLimitAt(x)|.
        TValue LeftLimitAt(double x);

        // See: |TPiecewiseLinearFunction::RightLimitAt|.
        // Equivalent to |RightSegmentAt(x).RightLimitAt(x)|.
        TValue RightLimitAt(double x);

        // See: |TPiecewiseLinearFunction::LeftRightLimitAt|.
        // Equivalent to |SegmentAt(x).LeftRightLimitAt(x)|.
        std::pair<TValue, TValue> LeftRightLimitAt(double x);

    private:
        const TPiecewiseLinearFunction* Function_;

        typename std::vector<TPiecewiseSegment<TValue>>::const_iterator Cur_;

        typename std::vector<TPiecewiseSegment<TValue>>::const_iterator End_;
    };

    TLeftToRightTraverser GetLeftToRightTraverser(int segmentIndex = 0) const;

private:
    std::vector<TSegment> Segments_;

    template <class TOperation>
    static TSelf ApplyBinaryOperation(
        const TSelf& lhs,
        const TSelf& rhs,
        const TOperation& operation);
};

template <class TValue>
bool operator==(const TPiecewiseLinearFunction<TValue>& lhs, const TPiecewiseLinearFunction<TValue>& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PIECEWISE_LINEAR_FUNCTION_INL_H_
#include "piecewise_linear_function-inl.h"
#undef PIECEWISE_LINEAR_FUNCTION_INL_H_

