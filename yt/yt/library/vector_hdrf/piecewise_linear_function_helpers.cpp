#include "piecewise_linear_function_helpers.h"

namespace NYT::NVectorHdrf::NDetail {

////////////////////////////////////////////////////////////////////////////////

TScalarPiecewiseLinearFunction ExtractComponent(int resourceIndex, const TVectorPiecewiseLinearFunction& vecFunc)
{
    TScalarPiecewiseLinearFunction::TBuilder builder;

    for (const auto& segment : vecFunc.Segments()) {
        builder.PushSegment(decltype(builder)::TSegment(
            {segment.LeftBound(), segment.LeftValue()[resourceIndex]},
            {segment.RightBound(), segment.RightValue()[resourceIndex]}));
    }

    return builder.Finish();
}

TScalarPiecewiseSegment ExtractComponent(int resourceIndex, const TVectorPiecewiseSegment& vecSegment)
{
    return TScalarPiecewiseSegment{
        {vecSegment.LeftBound(),  vecSegment.LeftValue()[resourceIndex]},
        {vecSegment.RightBound(), vecSegment.RightValue()[resourceIndex]}
    };
}

TUnpackedVectorPiecewiseSegment UnpackVectorSegment(const TVectorPiecewiseSegment& vecSegment)
{
    TUnpackedVectorPiecewiseSegment result;

    for (int r = 0; r < ResourceCount; ++r) {
        result.push_back(ExtractComponent(r, vecSegment));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

int CompareSegments(const TScalarPiecewiseSegment& firstSegment, const TScalarPiecewiseSegment& secondSegment)
{
    double firstDeltaBound = firstSegment.RightBound() - firstSegment.LeftBound();
    double firstDeltaValue = firstSegment.RightValue() - firstSegment.LeftValue();
    double secondDeltaBound = secondSegment.RightBound() - secondSegment.LeftBound();
    double secondDeltaValue = secondSegment.RightValue() - secondSegment.LeftValue();
    double crossProduct = firstDeltaBound * secondDeltaValue - secondDeltaBound * firstDeltaValue;

    if (crossProduct > 0) {
        return 1;
    }
    if (crossProduct < 0) {
        return -1;
    }
    return 0;
}

TUnpackedVectorPiecewiseSegmentBounds GetBounds(const TUnpackedVectorPiecewiseSegment& segments, double epsilon)
{
    TUnpackedVectorPiecewiseSegment topBounds;
    TUnpackedVectorPiecewiseSegment bottomBounds;

    for (const auto& segment : segments) {
        topBounds.push_back({
            {segment.LeftBound(),  segment.LeftValue()},
            {segment.RightBound(), segment.RightValue() + epsilon}});
        bottomBounds.push_back({
            {segment.LeftBound(),  segment.LeftValue()},
            {segment.RightBound(), segment.RightValue() - epsilon}});
    }

    return {topBounds, bottomBounds};
}

TVectorPiecewiseLinearFunction CompressFunction(const TVectorPiecewiseLinearFunction& vecFunc, double epsilon)
{
    const auto& functionSegments = vecFunc.Segments();
    Y_VERIFY(!functionSegments.empty());

    TVectorPiecewiseLinearFunction::TBuilder builder;

    // For an interval of function's segments, |accumulatedSegment| is the segment that connects
    // the start of the leftmost segment of the interval with the end of the rightmost segment.
    auto accumulatedSegment = functionSegments.front();
    // We say that a segment is "feasible" if it is below the top bound and above the bottom bound for each resource.
    auto accumulatedBounds = GetBounds(UnpackVectorSegment(functionSegments.front()), epsilon);

    bool isFirst = true;
    for (const auto& currentSegment : functionSegments) {
        if (isFirst) {
            isFirst = false;
            continue;
        }

        auto newAccumulatedSegment = ConnectSegments(accumulatedSegment, currentSegment);
        auto unpackedNewAccumulatedSegment = UnpackVectorSegment(newAccumulatedSegment);
        auto currentBounds = GetBounds(unpackedNewAccumulatedSegment, epsilon);

        bool canExtendAccumulatedInterval = true;
        for (int r = 0; r < ResourceCount; ++r) {
            // If the accumulated top bound for resource |r| is above the top bound of current segment, then update the top bound.
            if (CompareSegments(currentBounds.Top[r], accumulatedBounds.Top[r]) > 0) {
               accumulatedBounds.Top[r] = currentBounds.Top[r];
            }
            // If the accumulated bottom bound for resource |r| is below the bottom bound of current segment, then update the bottom bound.
            if (CompareSegments(currentBounds.Bottom[r], accumulatedBounds.Bottom[r]) < 0) {
                accumulatedBounds.Bottom[r] = currentBounds.Bottom[r];
            }
            // If |accumulatedSegment| is infeasible, we cannot extend the interval of merged segments any further.
            if (CompareSegments(accumulatedBounds.Bottom[r], unpackedNewAccumulatedSegment[r]) <= 0
                || CompareSegments(accumulatedBounds.Top[r], unpackedNewAccumulatedSegment[r]) >= 0)
            {
                canExtendAccumulatedInterval = false;
                break;
            }
        }

        // If we can greedily extend the interval, do so, otherwise merge the accumulated interval and start a new one.
        if (canExtendAccumulatedInterval) {
            accumulatedSegment = newAccumulatedSegment;
        } else {
            builder.PushSegment(accumulatedSegment);
            accumulatedSegment = currentSegment;
            accumulatedBounds = GetBounds(UnpackVectorSegment(currentSegment), epsilon);
        }
    }

    // Finally, merge the last accumulated interval of segments.
    builder.PushSegment(accumulatedSegment);

    return builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf::NDetail
