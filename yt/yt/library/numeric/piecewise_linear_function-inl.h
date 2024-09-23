#ifndef PIECEWISE_LINEAR_FUNCTION_INL_H_
#error "Direct inclusion of this file is not allowed, include piecewise_linear_function.h"
// For the sake of sane code completion.
#include "piecewise_linear_function.h"
#endif

#include <library/cpp/yt/string/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TPiecewiseSegment<TValue>::TPiecewiseSegment(std::pair<double, TValue> leftPoint, std::pair<double, TValue> rightPoint)
    : LeftBound_(leftPoint.first)
    , LeftValue_(leftPoint.second)
    , RightBound_(rightPoint.first)
    , RightValue_(rightPoint.second)
{
    YT_VERIFY(LeftBound_ <= RightBound_);
}

template <class TValue>
double TPiecewiseSegment<TValue>::LeftBound() const
{
    return LeftBound_;
}

template <class TValue>
double TPiecewiseSegment<TValue>::RightBound() const
{
    return RightBound_;
}

template <class TValue>
const TValue& TPiecewiseSegment<TValue>::LeftValue() const
{
    return LeftValue_;
}

template <class TValue>
const TValue& TPiecewiseSegment<TValue>::RightValue() const
{
    return RightValue_;
}

template <class TValue>
bool TPiecewiseSegment<TValue>::IsDefinedAt(double x) const
{
    return x >= LeftBound_ && x <= RightBound_;
}

template <class TValue>
bool TPiecewiseSegment<TValue>::IsDefinedOn(double left, double right) const
{
    return IsDefinedAt(left) && IsDefinedAt(right);
}

template <class TValue>
TValue TPiecewiseSegment<TValue>::LeftLimitAt(double x) const
{
    YT_VERIFY(IsDefinedAt(x));
    return LeftRightLimitAt(x).first;
}

template <class TValue>
TValue TPiecewiseSegment<TValue>::RightLimitAt(double x) const
{
    YT_VERIFY(IsDefinedAt(x));
    return LeftRightLimitAt(x).second;
}

template <class TValue>
std::pair<TValue, TValue> TPiecewiseSegment<TValue>::LeftRightLimitAt(double x) const
{
    YT_VERIFY(IsDefinedAt(x));
    if (RightBound() == LeftBound()) {
        return {LeftValue_, RightValue_};
    } else {
        TValue res = InterpolateAt(x);
        return {res, res};
    }
}

template <class TValue>
TValue TPiecewiseSegment<TValue>::ValueAt(double x) const
{
    YT_VERIFY(IsDefinedAt(x));
    // NB: We currently assume all functions to be left-continuous.
    return LeftLimitAt(x);
}

template <class TValue>
bool TPiecewiseSegment<TValue>::IsVertical() const
{
    return LeftBound_ == RightBound_ && LeftValue_ != RightValue_;
}

template <class TValue>
bool TPiecewiseSegment<TValue>::IsHorizontal() const
{
    return LeftBound_ != RightBound_ && LeftValue_ == RightValue_;
}

template <class TValue>
bool TPiecewiseSegment<TValue>::IsPoint() const
{
    return LeftBound_ == RightBound_ && LeftValue_ == RightValue_;
}

template <class TValue>
bool TPiecewiseSegment<TValue>::IsTilted() const
{
    return LeftBound_ != RightBound_ && LeftValue_ != RightValue_;
}

template <class TValue>
TPiecewiseSegment<TValue> TPiecewiseSegment<TValue>::Transpose() const
{
    static_assert(std::is_same_v<TValue, double>);
    return TPiecewiseSegment({LeftValue_, LeftBound_}, {RightValue_, RightBound_});
}

template <class TValue>
TPiecewiseSegment<TValue> TPiecewiseSegment<TValue>::ScaleArgument(double scale) const
{
    return TPiecewiseSegment({LeftBound_ / scale, LeftValue_}, {RightBound_ / scale, RightValue_});
}

template <class TValue>
TPiecewiseSegment<TValue> TPiecewiseSegment<TValue>::Shift(double deltaBound, const TValue& deltaValue) const
{
    return TPiecewiseSegment(
        {LeftBound_ + deltaBound, LeftValue_ + deltaValue},
        {RightBound_ + deltaBound, RightValue_ + deltaValue});
}

// The result of interpolation must satisfy the following properties:
// (1) Match the endpoints.
//      I.e. |InterpolateAt(LeftBound()) == LeftValue()| and |InterpolateAt(RightBound()) == RightValue()|.
// (2) Monotonicity.
//      I.e. if |LeftValue() <= RightValue()| and |x1 <= x2|, then |InterpolateAt(x1) <= InterpolateAt(x2)|.
// (3) Bounded.
//      I.e. |InterpolateAt(x) >= LeftValue()| and |InterpolateAt(x) <= RightValue()|.
//      This follows from properties (1) and (2).
// (4) Deterministic.
//      I.e. |InterpolateAt(x) == InterpolateAt(x)|.
template <class TValue>
TValue TPiecewiseSegment<TValue>::InterpolateAt(double x) const
{
    Y_DEBUG_ABORT_UNLESS(IsDefinedAt(x));
    Y_DEBUG_ABORT_UNLESS(LeftBound_ != RightBound_);

    // The value of t is monotonic and is exact at bounds.
    double t = (x - LeftBound()) / (RightBound() - LeftBound());

    Y_DEBUG_ABORT_UNLESS(x != LeftBound() || t == 0);
    Y_DEBUG_ABORT_UNLESS(x != RightBound() || t == 1);

    return InterpolateNormalized(t);
}

template <class TValue>
TValue TPiecewiseSegment<TValue>::InterpolateNormalized(double t) const
{
    // The used method is from https://math.stackexchange.com/a/1798323
    // It obviously matches the endpoints and is obviously monotonic in both halves.
    // It turns out that it is also monotonic in the middle.
    if (t < 0.5) {
        return LeftValue() + (RightValue() - LeftValue()) * t;
    } else {
        return RightValue() - (RightValue() - LeftValue()) * (1 - t);
    }
}

template <class TValue>
bool operator ==(const TPiecewiseSegment<TValue>& lhs, const TPiecewiseSegment<TValue>& rhs) {
    return lhs.LeftBound() == rhs.LeftBound() && lhs.RightBound() == rhs.RightBound() &&
        lhs.LeftValue() == rhs.LeftValue() && lhs.RightValue() == rhs.RightValue();
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
void ExtractCriticalPointsFromFunction(const TPiecewiseLinearFunction<TValue>& func, std::vector<double>* out)
{
    out->push_back(func.LeftFunctionBound());
    double lastAdded = func.LeftFunctionBound();

    for (const auto& segment : func.Segments()) {
        if (segment.RightBound() > lastAdded) {
            out->push_back(segment.RightBound());
            lastAdded = segment.RightBound();
        }
    }
}

template <class TValue>
void ExtractCriticalPointsFromFunctionWithValues(
    const TPiecewiseLinearFunction<TValue>& func,
    std::vector<std::pair<double, TValue>>* out)
{
    const auto& firstSegment = func.Segments().front();
    out->emplace_back(firstSegment.LeftBound(), firstSegment.LeftValue());

    for (const auto& segment : func.Segments()) {
        out->emplace_back(segment.RightBound(), segment.RightValue());
    }
}

template <class TValue>
void ExtractDiscontinuityPointsFromFunction(const TPiecewiseLinearFunction<TValue>& func, std::vector<double>* out)
{
    for (const auto& segment : func.Segments()) {
        if (segment.IsVertical()) {
            out->push_back(segment.LeftBound());
        }
    }
}

template <class TValue>
void PushSegmentImpl(std::vector<TPiecewiseSegment<TValue>>* vec, TPiecewiseSegment<TValue> segment)
{
    if (!vec->empty()) {
        // NB: Strict equality is required in both cases.
        YT_VERIFY(vec->back().RightBound() == segment.LeftBound());
        YT_VERIFY(vec->back().RightValue() == segment.LeftValue());

        // Try to merge two segments.
        const auto& leftSegment = vec->back();
        const auto& rightSegment = segment;
        auto mergedSegment = TPiecewiseSegment<TValue>(
            {leftSegment.LeftBound(), leftSegment.LeftValue()},
            {rightSegment.RightBound(), rightSegment.RightValue()});

        if (rightSegment.IsPoint()) {
            return;
        }
        if (leftSegment.IsPoint()) {
            vec->back() = rightSegment;
            return;
        }
        if (leftSegment.IsVertical() && rightSegment.IsVertical()) {
            YT_VERIFY(mergedSegment.IsVertical());
            vec->back() = mergedSegment;
            return;
        }
        if (leftSegment.IsHorizontal() && rightSegment.IsHorizontal()) {
            YT_VERIFY(mergedSegment.IsHorizontal());
            vec->back() = mergedSegment;
            return;
        }
    }

    vec->push_back(segment);
}

template <class TValue>
TPiecewiseLinearFunction<TValue> PointwiseMin(
    const TPiecewiseLinearFunction<TValue>& lhs,
    const TPiecewiseLinearFunction<TValue>& rhs)
{
    double resultLeftBound = std::max(lhs.LeftFunctionBound(), rhs.LeftFunctionBound());
    double resultRightBound = std::min(lhs.RightFunctionBound(), rhs.RightFunctionBound());
    YT_VERIFY(resultLeftBound <= resultRightBound);

    auto sampleResult = [
            lhsTraverser = lhs.GetLeftToRightTraverser(),
            rhsTraverser = rhs.GetLeftToRightTraverser()
        ] (double x) mutable -> std::pair<double, double> {

        auto [lhsLeftLimit, lhsRightLimit] = lhsTraverser.LeftRightLimitAt(x);
        auto [rhsLeftLimit, rhsRightLimit] = rhsTraverser.LeftRightLimitAt(x);

        return {
            std::min(lhsLeftLimit, rhsLeftLimit),
            std::min(lhsRightLimit, rhsRightLimit)
        };
    };

    // Add critical points from the original functions.
    std::vector<double> criticalPoints;
    ExtractCriticalPointsFromFunction(lhs, &criticalPoints);
    ExtractCriticalPointsFromFunction(rhs, &criticalPoints);

    ClearAndSortCriticalPoints(&criticalPoints, resultLeftBound, resultRightBound);

    // Add critical points on function intersections.
    int criticalPointsInitialSize = criticalPoints.size();

    auto lhsTraverser = lhs.GetLeftToRightTraverser();
    auto rhsTraverser = rhs.GetLeftToRightTraverser();

    // NB(antonkikh): We ignore intersections of vertical segments here since those points are already in |criticalPoints|.
    for (int i = 1; i < criticalPointsInitialSize; i++) {
        double leftBound = criticalPoints[i - 1];
        double rightBound = criticalPoints[i];
        YT_VERIFY(leftBound < rightBound);

        // NB: Cannot use structure bindings here because it is not compatible with lambda capturing until C++20.
        auto pair = MinMaxBy(
            lhsTraverser.RightSegmentAt(leftBound),
            rhsTraverser.RightSegmentAt(leftBound),
            /* getKey */ [&] (const auto& segment) { return segment.ValueAt(leftBound); });
        auto segmentLo = pair.first;
        auto segmentHi = pair.second;

        YT_VERIFY(segmentLo.IsDefinedOn(leftBound, rightBound));
        YT_VERIFY(segmentHi.IsDefinedOn(leftBound, rightBound));

        if (segmentLo.ValueAt(leftBound) < segmentHi.ValueAt(leftBound)
            && segmentLo.ValueAt(rightBound) > segmentHi.ValueAt(rightBound))
        {
            criticalPoints.push_back(FloatingPointInverseLowerBound(
                /* lo */ leftBound,
                /* hi */ rightBound,
                [&] (double mid) { return segmentLo.ValueAt(mid) <= segmentHi.ValueAt(mid); }));
        }
    }

    // Build the result.
    auto result = TPiecewiseLinearFunction<TValue>::Create(
        sampleResult,
        resultLeftBound,
        resultRightBound,
        std::move(criticalPoints));
    return result;
}

template <class TValue>
TPiecewiseLinearFunction<TValue> PointwiseMin(const std::vector<TPiecewiseLinearFunction<TValue>>& funcs)
{
    YT_VERIFY(!funcs.empty());
    return std::accumulate(
        begin(funcs) + 1,
        end(funcs),
        funcs[0],
        [] (const auto& f1, const auto& f2) { return PointwiseMin(f1, f2); });
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
void TPiecewiseLinearFunctionBuilder<TValue>::AddPoint(TPoint point)
{
    if (RightPoint_) {
        PushSegmentImpl(&Segments_, TSegment(*RightPoint_, point));
    }

    RightPoint_ = point;
}

template <class TValue>
void TPiecewiseLinearFunctionBuilder<TValue>::PushSegment(const TSegment& segment)
{
    if (RightPoint_) {
        YT_VERIFY(RightPoint_->first == segment.LeftBound());
        YT_VERIFY(RightPoint_->second == segment.LeftValue());
    }
    PushSegmentImpl(&Segments_, segment);
    RightPoint_ = TPoint(segment.RightBound(), segment.RightValue());
}

template <class TValue>
void TPiecewiseLinearFunctionBuilder<TValue>::PushSegment(
    std::pair<double, TValue> leftPoint,
    std::pair<double, TValue> rightPoint)
{
    PushSegment(TSegment(leftPoint, rightPoint));
}

template <class TValue>
auto TPiecewiseLinearFunctionBuilder<TValue>::Finish() -> TFunction
{
    return TFunction(std::move(Segments_));
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
template <class TFunction>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Create(
    TFunction&& sampleFunction,
    double leftBound,
    double rightBound,
    std::vector<double> criticalPoints)
{
    // sampleFunction(x) should return a pair of values: left-hand and right-hand limit for this argument.
    static_assert(std::is_same_v<decltype(sampleFunction(1.0L)), std::pair<TValue, TValue>>);

    ClearAndSortCriticalPoints(&criticalPoints, leftBound, rightBound);
    YT_VERIFY(!criticalPoints.empty());
    if (criticalPoints.front() != leftBound) {
        throw yexception()
            << "Left bound of the function must be its first critical point (CriticalPoints: ["
            << NYT::ToString(criticalPoints)
            << "], LeftBound: " << leftBound << ")";
    }
    YT_VERIFY(criticalPoints.front() == leftBound);
    YT_VERIFY(criticalPoints.back() == rightBound);

    TBuilder builder;

    for (double criticalPoint : criticalPoints) {
        auto [leftLimit, rightLimit] = sampleFunction(criticalPoint);

        builder.AddPoint({criticalPoint, leftLimit});
        builder.AddPoint({criticalPoint, rightLimit});
    }

    TSelf result = builder.Finish();

    Y_DEBUG_ABORT_UNLESS(result.LeftFunctionBound() == leftBound);
    Y_DEBUG_ABORT_UNLESS(result.RightFunctionBound() == rightBound);

    return result;
}

template <class TValue>
TPiecewiseLinearFunction<TValue>::TPiecewiseLinearFunction(std::vector<TPiecewiseSegment<TValue>> segments)
    : Segments_(std::move(segments))
{
    YT_VERIFY(!Segments_.empty());
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Linear(
    double leftBound,
    TValue leftValue,
    double rightBound,
    TValue rightValue)
{
    TBuilder builder;
    builder.PushSegment({leftBound, leftValue}, {rightBound, rightValue});
    return builder.Finish();
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Constant(
    double leftBound,
    double rightBound,
    TValue value)
{
    return Linear(leftBound, value, rightBound, value);
}

template <class TValue>
TValue TPiecewiseLinearFunction<TValue>::LeftLimitAt(double x) const
{
    return LeftSegmentAt(x).LeftLimitAt(x);
}

template <class TValue>
TValue TPiecewiseLinearFunction<TValue>::RightLimitAt(double x) const
{
    return RightSegmentAt(x).RightLimitAt(x);
}

template <class TValue>
std::pair<TValue, TValue> TPiecewiseLinearFunction<TValue>::LeftRightLimitAt(double x) const
{
    return SegmentAt(x).LeftRightLimitAt(x);
}

template <class TValue>
TValue TPiecewiseLinearFunction<TValue>::ValueAt(double x) const
{
    // NB: We currently assume all functions to be left-continuous.
    return LeftLimitAt(x);
}

template <class TValue>
const std::vector<TPiecewiseSegment<TValue>>& TPiecewiseLinearFunction<TValue>::Segments() const
{
    return Segments_;
}

template <class TValue>
double TPiecewiseLinearFunction<TValue>::LeftFunctionBound() const
{
    return Segments_.front().LeftBound();
}

template <class TValue>
double TPiecewiseLinearFunction<TValue>::RightFunctionBound() const
{
    return Segments_.back().RightBound();
}

template <class TValue>
TValue TPiecewiseLinearFunction<TValue>::LeftFunctionValue() const
{
    return Segments_.front().LeftValue();
}

template <class TValue>
TValue TPiecewiseLinearFunction<TValue>::RightFunctionValue() const
{
    YT_VERIFY(!Segments_.back().IsVertical());
    return Segments_.back().RightValue();
}

template <class TValue>
bool TPiecewiseLinearFunction<TValue>::IsDefinedAt(double x) const
{
    return x >= LeftFunctionBound() && x <= RightFunctionBound();
}

template <class TValue>
bool TPiecewiseLinearFunction<TValue>::IsContinuous() const
{
    return std::all_of(begin(Segments_), end(Segments_), [] (const auto& segment) {
        return !segment.IsVertical();
    });
}

template <class TValue>
bool TPiecewiseLinearFunction<TValue>::IsNondecreasing() const
{
    return std::all_of(begin(Segments_), end(Segments_), [] (const auto& segment) {
        return segment.LeftValue() <= segment.RightValue();
    });
}

template <class TValue>
bool TPiecewiseLinearFunction<TValue>::IsTrimmed() const
{
    return IsTrimmedRight();
}

template <class TValue>
bool TPiecewiseLinearFunction<TValue>::IsTrimmedLeft() const
{
    return !Segments_.front().IsVertical();
}

template <class TValue>
bool TPiecewiseLinearFunction<TValue>::IsTrimmedRight() const
{
    // Since we assume all functions to be left-continuous, only the last segment needs to be checked.
    return !Segments_.back().IsVertical();
}

template <class TValue>
const TPiecewiseSegment<TValue>& TPiecewiseLinearFunction<TValue>::LeftSegmentAt(double x, int* segmentIndex) const
{
    Y_DEBUG_ABORT_UNLESS(IsDefinedAt(x));

    // Finds first element with |RightBound() >= x|.
    auto it = LowerBoundBy(
        begin(Segments_),
        end(Segments_),
        /* value */ x,
        [] (const auto& segment) { return segment.RightBound(); });
    Y_DEBUG_ABORT_UNLESS(it != end(Segments_));

    if (segmentIndex != nullptr) {
        *segmentIndex = it - begin(Segments_);
    }
    return *it;
}

template <class TValue>
const TPiecewiseSegment<TValue>& TPiecewiseLinearFunction<TValue>::RightSegmentAt(double x, int* segmentIndex) const
{
    Y_DEBUG_ABORT_UNLESS(IsDefinedAt(x));

    // Returns first element with LeftBound() > x.
    auto it = UpperBoundBy(
        begin(Segments_),
        end(Segments_),
        /* value */ x,
        [] (const auto& segment) { return segment.LeftBound(); });
    YT_VERIFY(it != begin(Segments_));
    // We need the last segment with LeftBound() <= x.
    --it;

    if (segmentIndex != nullptr) {
        *segmentIndex = it - begin(Segments_);
    }
    return *it;
}

template <class TValue>
const TPiecewiseSegment<TValue>& TPiecewiseLinearFunction<TValue>::SegmentAt(double x, int* segmentIndex) const
{
    Y_DEBUG_ABORT_UNLESS(IsDefinedAt(x));

    // Finds first element with |RightBound() >= x|.
    auto it = LowerBoundBy(
        begin(Segments_),
        end(Segments_),
        /* value */ x,
        [] (const auto& segment) { return segment.RightBound(); });
    Y_DEBUG_ABORT_UNLESS(it != end(Segments_));

    auto next = it + 1;
    if (it->RightBound() == x && next != end(Segments_) && next->IsVertical()) {
        Y_DEBUG_ABORT_UNLESS(next->LeftBound() == x);
        it = next;
    }

    if (segmentIndex != nullptr) {
        *segmentIndex = it - begin(Segments_);
    }
    return *it;
}

template <class TValue>
void TPiecewiseLinearFunction<TValue>::PushSegment(const TPiecewiseSegment<TValue>& segment)
{
    PushSegmentImpl(&Segments_, segment);
}

template <class TValue>
TPiecewiseLinearFunction<TValue>& TPiecewiseLinearFunction<TValue>::TransposeInplace()
{
    for (auto& segment : Segments_) {
        segment = segment.Transpose();
    }
    return *this;
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Transpose() const&
{
    auto res = *this;
    res.TransposeInplace();
    return res;
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Transpose()&&
{
    auto res = std::move(*this);
    return res.TransposeInplace();
}

template <class TValue>
TPiecewiseLinearFunction<TValue>& TPiecewiseLinearFunction<TValue>::TrimLeftInplace()
{
    if (Segments_.front().IsVertical()) {
        if (Segments_.size() > 1) {
            // TODO(antonkikh): Consider implementing it efficiently by replacing Segments_ with an |std::dequeue|.
            Segments_.erase(Segments_.begin());
        } else {
            // Replace the segment with a single point.
            double argument = Segments_.front().LeftBound();
            TValue value = Segments_.front().RightValue();
            Segments_.front() = TSegment({argument, value}, {argument, value});
        }

        YT_VERIFY(!Segments_.front().IsVertical());
    }

    return *this;
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::TrimLeft() const&
{
    auto res = *this;
    return res.TrimLeftInplace();
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::TrimLeft()&&
{
    auto res = std::move(*this);
    return res.TrimLeftInplace();
}

template <class TValue>
TPiecewiseLinearFunction<TValue>& TPiecewiseLinearFunction<TValue>::TrimRightInplace()
{
    // Since we assume all functions to be left-continuous, only the last segment needs to be checked.
    if (Segments_.back().IsVertical()) {
        if (Segments_.size() > 1) {
            Segments_.pop_back();
        } else {
            // Replace the segment with a single point.
            double argument = Segments_.back().LeftBound();
            TValue value = Segments_.back().ValueAt(argument);
            Segments_.back() = TSegment({argument, value}, {argument, value});
        }

        YT_VERIFY(!Segments_.back().IsVertical());
    }

    return *this;
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::TrimRight() const&
{
    auto res = *this;
    res.TrimRightInplace();
    return res;
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::TrimRight()&&
{
    auto res = std::move(*this);
    return res.TrimRightInplace();
}

template <class TValue>
TPiecewiseLinearFunction<TValue>& TPiecewiseLinearFunction<TValue>::TrimInplace()
{
    // Since we assume all functions to be left-continuous, only the discontinuity point at the right bound is non-sensible.
    return TrimRightInplace();
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Trim() const&
{
    return TrimRight();
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Trim()&&
{
    return std::move(*this).TrimRight();
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::ScaleArgument(double scale) const
{
    TBuilder builder;

    for (const auto& segment : Segments_) {
        builder.PushSegment(segment.ScaleArgument(scale));
    }

    return builder.Finish();
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Shift(
    double deltaArgument,
    const TValue& deltaValue) const
{
    TBuilder builder;

    for (const auto& segment : Segments_) {
        builder.PushSegment(segment.Shift(deltaArgument, deltaValue));
    }

    return builder.Finish();
}

template <class TValue>
TPiecewiseLinearFunction<TValue>& TPiecewiseLinearFunction<TValue>::NarrowInplace(
    double newLeftBound,
    double newRightBound)
{
    // TODO(antonkikh): Consider implementing it efficiently by replacing Segments_ with an |std::deque|.
    *this = Narrow(newLeftBound, newRightBound);
    return *this;
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Narrow(
    double newLeftBound,
    double newRightBound) const
{
    YT_VERIFY(IsDefinedAt(newLeftBound));
    YT_VERIFY(IsDefinedAt(newRightBound));
    YT_VERIFY(newLeftBound <= newRightBound);

    return *this + Constant(newLeftBound, newRightBound, TValue{});
}

template <class TValue>
TPiecewiseLinearFunction<TValue>& TPiecewiseLinearFunction<TValue>::ExtendInplace(
    double newLeftBound,
    const TValue& newLeftValue,
    double newRightBound,
    const TValue& newRightValue)
{
    // TODO(antonkikh): Consider implementing it efficiently by replacing Segments_ with an |std::deque|.
    *this = Extend(newLeftBound, newLeftValue, newRightBound, newRightValue);
    return *this;
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Extend(
    double newLeftBound,
    const TValue& newLeftValue,
    double newRightBound,
    const TValue& newRightValue) const
{
    YT_VERIFY(newLeftBound <= LeftFunctionBound());

    TBuilder builder;

    if (newLeftBound < LeftFunctionBound()) {
        builder.PushSegment({newLeftBound, newLeftValue}, {LeftFunctionBound(), newLeftValue});

        TValue leftFunctionValue = Segments_.front().LeftValue();
        if (newLeftValue != leftFunctionValue) {
            builder.PushSegment({LeftFunctionBound(), newLeftValue}, {LeftFunctionBound(), leftFunctionValue});
        }
    }

    for (const auto& segment : Segments_) {
        builder.PushSegment(segment);
    }

    if (RightFunctionBound() < newRightBound) {
        // NB(antonkikh): cannot use RightFunctionValue(), since the function might not be trimmed.
        TValue rightFunctionValue = Segments_.back().RightValue();
        if (newRightValue != rightFunctionValue) {
            builder.PushSegment({RightFunctionBound(), rightFunctionValue}, {RightFunctionBound(), newRightValue});
        }

        builder.PushSegment({RightFunctionBound(), newRightValue}, {newRightBound, newRightValue});
    }

    return builder.Finish();
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Extend(
    double newLeftBound,
    double newRightBound) const
{
    // NB(antonkikh): cannot use |RightFunctionValue()|, since the function might not be trimmed.
    return Extend(
        newLeftBound,
        Segments_.front().LeftValue(),
        newRightBound,
        Segments_.back().RightValue());
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::ExtendRight(
    double newRightBound,
    const TValue& newRightValue) const
{
    return Extend(
        LeftFunctionBound(),
        LeftFunctionValue(),
        newRightBound,
        newRightValue);
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::ExtendRight(double newRightBound) const
{
    // NB(antonkikh): cannot use |RightFunctionValue()|, since the function might not be trimmed.
    return ExtendRight(newRightBound, Segments_.back().RightValue());
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::operator+(
    const TPiecewiseLinearFunction<TValue>& other) const
{
    return ApplyBinaryOperation(*this, other, std::plus<>());
}

template <class TValue>
TPiecewiseLinearFunction<TValue>& TPiecewiseLinearFunction<TValue>::operator+=(
    const TPiecewiseLinearFunction<TValue>& other)
{
    *this = *this + other;
    return *this;
}

template <class TValue>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Sum(
    const std::vector<TPiecewiseLinearFunction<TValue>>& funcs)
{
    YT_VERIFY(!funcs.empty());

    double resultLeftBound = std::numeric_limits<double>::lowest();
    double resultRightBound = std::numeric_limits<double>::max();
    for (const auto& func : funcs) {
        resultLeftBound = std::max(resultLeftBound, func.LeftFunctionBound());
        resultRightBound = std::min(resultRightBound, func.RightFunctionBound());
    }
    YT_VERIFY(resultLeftBound <= resultRightBound);

    std::vector<double> criticalPoints;
    for (const auto& func : funcs) {
        // TODO(antonkikh): |SortOrMerge| optimization won't work well here if |funcs.size()| is larger than a constant.
        // Consider optimizing this place manually or modifying |SortOrMerge|.
        ExtractCriticalPointsFromFunction(func, &criticalPoints);
    }

    std::vector<TPiecewiseLinearFunction<TValue>::TLeftToRightTraverser> traversers;
    for (const auto& func : funcs) {
        traversers.emplace_back(func.GetLeftToRightTraverser());
    }

    auto sampleResult = [&] (double x) mutable -> std::pair<TValue, TValue> {
        TValue leftLimit = {};
        TValue rightLimit = {};

        for (int i = 0; i < std::ssize(funcs); i++) {
            auto [childLeftLimit, childRightLimit] = traversers[i].LeftRightLimitAt(x);
            leftLimit += childLeftLimit;
            rightLimit += childRightLimit;
        }

        return {leftLimit, rightLimit};
    };

    return Create(
        sampleResult,
        /* leftBound */ resultLeftBound,
        /* rightBound */ resultRightBound,
        std::move(criticalPoints));
}

template <class TValue>
template <class TOther>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::Compose(const TOther& other) const
{
    // Verify input parameters.
    static_assert(IsPiecewiseLinearFunction<TOther>);
    static_assert(std::is_same_v<double, typename TOther::TValueType>);

    if (!other.IsNondecreasing()) {
        throw yexception() << "Composition is only supported for non-decreasing functions";
    }
    YT_VERIFY(IsDefinedAt(other.LeftLimitAt(other.LeftFunctionBound())));
    YT_VERIFY(IsDefinedAt(other.RightLimitAt(other.RightFunctionBound())));

    // Prepare critical points with the expected values of the rhs function at these points.
    std::vector<std::pair<double, double>> criticalPoints;
    ExtractCriticalPointsFromFunctionWithValues(other, &criticalPoints);

    {
        auto inverseOther = other.Transpose();

        std::vector<double> lhsCriticalPoints;
        ExtractCriticalPointsFromFunction(*this, &lhsCriticalPoints);

        for (double rhsValue : lhsCriticalPoints) {
            if (inverseOther.IsDefinedAt(rhsValue)) {
                auto rhsInverseSegment = inverseOther.SegmentAt(rhsValue);
                auto rhsSegment = rhsInverseSegment.Transpose();

                // If this condition is not met, this critical point is sufficiently covered by the critical points of |other|.
                if (rhsSegment.IsTilted() && rhsValue != rhsSegment.LeftValue() && rhsValue != rhsSegment.RightValue()) {
                    criticalPoints.emplace_back(rhsInverseSegment.ValueAt(rhsValue), rhsValue);
                }
            }
        }
    }

    Sort(begin(criticalPoints), end(criticalPoints));
    Y_DEBUG_ABORT_UNLESS(IsSortedBy(begin(criticalPoints), end(criticalPoints), [] (const auto& pair) { return pair.second; }));
    Y_DEBUG_ABORT_UNLESS(Unique(begin(criticalPoints), end(criticalPoints)) == end(criticalPoints));

    // Finally, build the resulting function.
    TBuilder builder;

    for (int i = 0; i < std::ssize(criticalPoints); i++) {
        const auto &[criticalPoint, rhsValue] = criticalPoints[i];

        // If this condition is not satisfied, the left-hand limit at this point was already calculated.
        if (i == 0 || criticalPoints[i - 1].first != criticalPoint) {
            TValue resultLeftLimit;
            if (i > 0 && criticalPoints[i - 1].second < rhsValue) {
                resultLeftLimit = LeftLimitAt(rhsValue);
            } else {
                resultLeftLimit = ValueAt(rhsValue);
            }

            builder.AddPoint({criticalPoint, resultLeftLimit});
        }

        // If this condition is not satisfied, the right-hand limit at this point will be calculated later.
        if (i + 1 == std::ssize(criticalPoints) || criticalPoints[i + 1].first != criticalPoint) {
            TValue resultRightLimit;
            if (i + 1 < std::ssize(criticalPoints) && criticalPoints[i + 1].second > rhsValue) {
                resultRightLimit = RightLimitAt(rhsValue);
            } else {
                resultRightLimit = ValueAt(rhsValue);
            }

            builder.AddPoint({criticalPoint, resultRightLimit});
        }
    }

    TSelf result = builder.Finish().Trim();

    YT_VERIFY(result.LeftFunctionBound() == other.LeftFunctionBound());
    YT_VERIFY(result.RightFunctionBound() == other.RightFunctionBound());

    return result;
}

template <class TValue>
template <class TOperation>
TPiecewiseLinearFunction<TValue> TPiecewiseLinearFunction<TValue>::ApplyBinaryOperation(
    const TPiecewiseLinearFunction<TValue>& lhs,
    const TPiecewiseLinearFunction<TValue>& rhs,
    const TOperation& operation)
{
    double resultLeftBound = std::max(lhs.LeftFunctionBound(), rhs.LeftFunctionBound());
    double resultRightBound = std::min(lhs.RightFunctionBound(), rhs.RightFunctionBound());

    auto sampleResult = [
            &operation,
            lhsTraverser = lhs.GetLeftToRightTraverser(),
            rhsTraverser = rhs.GetLeftToRightTraverser()
        ] (double x) mutable -> std::pair<TValue, TValue> {

        auto [lhsLeftLimit, lhsRightLimit] = lhsTraverser.LeftRightLimitAt(x);
        auto [rhsLeftLimit, rhsRightLimit] = rhsTraverser.LeftRightLimitAt(x);

        return {
            operation(lhsLeftLimit, rhsLeftLimit),
            operation(lhsRightLimit, rhsRightLimit)
        };
    };

    std::vector<double> criticalPoints;
    ExtractCriticalPointsFromFunction(lhs, &criticalPoints);
    ExtractCriticalPointsFromFunction(rhs, &criticalPoints);

    return Create(sampleResult, resultLeftBound, resultRightBound, std::move(criticalPoints));
}

template <class TValue>
auto TPiecewiseLinearFunction<TValue>::GetLeftToRightTraverser(int segmentIndex) const -> TLeftToRightTraverser
{
    return TLeftToRightTraverser(*this, segmentIndex);
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TPiecewiseLinearFunction<TValue>::TLeftToRightTraverser::TLeftToRightTraverser(
    const TPiecewiseLinearFunction& function,
    int segmentIndex)
    : Function_(&function)
    , Cur_(Function_->Segments().begin() + segmentIndex)
    , End_(Function_->Segments().end())
{
    Y_DEBUG_ABORT_UNLESS(segmentIndex < std::ssize(Function_->Segments()));
}

template <class TValue>
const TPiecewiseSegment<TValue>& TPiecewiseLinearFunction<TValue>::TLeftToRightTraverser::LeftSegmentAt(double x)
{
    Y_DEBUG_ABORT_UNLESS(Function_->IsDefinedAt(x));
    Y_DEBUG_ABORT_UNLESS(Cur_ == Function_->Segments().begin() || (Cur_ - 1)->RightBound() < x);

    // Note that since |Function_->IsDefinedAt(x)| holds, we do not need to check that |Cur_ != End_|.
    while (Cur_->RightBound() < x) {
        ++Cur_;
    }

    // Return the first segment with |RightBound() >= x|.
    return *Cur_;
}

template <class TValue>
const TPiecewiseSegment<TValue>& TPiecewiseLinearFunction<TValue>::TLeftToRightTraverser::RightSegmentAt(double x)
{
    Y_DEBUG_ABORT_UNLESS(Function_->IsDefinedAt(x));
    Y_DEBUG_ABORT_UNLESS(Cur_->LeftBound() <= x);

    while (true) {
        auto next = Cur_ + 1;

        if (next != End_ && next->LeftBound() <= x) {
            Cur_ = next;
        } else {
            break;
        }
    }

    // Return the last segment with |LeftBound() <= x|.
    return *Cur_;
}

template <class TValue>
const TPiecewiseSegment<TValue>& TPiecewiseLinearFunction<TValue>::TLeftToRightTraverser::SegmentAt(double x)
{
    Y_DEBUG_ABORT_UNLESS(Function_->IsDefinedAt(x));
    Y_DEBUG_ABORT_UNLESS(Cur_->LeftBound() <= x);
    // Note that since |Function_->IsDefinedAt(x)| holds, we do not need to check that |Cur_ != End_|.
    while (Cur_->RightBound() < x) {
        ++Cur_;
    }
    // |Cur_->RightBound() >= x|.

    auto next = Cur_ + 1;
    if (Cur_->RightBound() == x && next != End_ && next->IsVertical()) {
        Y_DEBUG_ABORT_UNLESS(next->LeftBound() == x);
        Cur_ = next;
    }

    return *Cur_;
}

template <class TValue>
TValue TPiecewiseLinearFunction<TValue>::TLeftToRightTraverser::ValueAt(double x)
{
    return LeftLimitAt(x);
}

template <class TValue>
TValue TPiecewiseLinearFunction<TValue>::TLeftToRightTraverser::LeftLimitAt(double x)
{
    return LeftSegmentAt(x).LeftLimitAt(x);
}

template <class TValue>
TValue TPiecewiseLinearFunction<TValue>::TLeftToRightTraverser::RightLimitAt(double x)
{
    return LeftSegmentAt(x).RightLimitAt(x);
}

template <class TValue>
std::pair<TValue, TValue> TPiecewiseLinearFunction<TValue>::TLeftToRightTraverser::LeftRightLimitAt(double x)
{
    return SegmentAt(x).LeftRightLimitAt(x);
}

template <class TValue>
bool operator==(const TPiecewiseLinearFunction<TValue>& lhs, const TPiecewiseLinearFunction<TValue>& rhs)
{
    return lhs.Segments() == rhs.Segments();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
