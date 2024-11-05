#include "key_bound_compressor.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/logging/fluent_log.h>

namespace NYT::NTableClient {

using namespace NLogging;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TKeyBoundCompressor::TKeyBoundCompressor(const TComparator& comparator)
    : Comparator_(comparator)
{ }

void TKeyBoundCompressor::Add(TKeyBound keyBound)
{
    YT_VERIFY(keyBound);

    AddedKeyBounds_.insert(keyBound);
    AddedKeyBounds_.insert(keyBound.Invert());
}

void TKeyBoundCompressor::InitializeMapping()
{
    YT_VERIFY(!MappingInitialized_);
    MappingInitialized_ = true;

    SortedKeyBounds_.reserve(AddedKeyBounds_.size());
    SortedKeyBounds_.insert(SortedKeyBounds_.end(), AddedKeyBounds_.begin(), AddedKeyBounds_.end());
    std::sort(
        SortedKeyBounds_.begin(),
        SortedKeyBounds_.end(),
        [&] (const TKeyBound& lhs, const TKeyBound& rhs) {
            return Comparator_.CompareKeyBounds(lhs, rhs) < 0;
        });

    // Prepare images.
    for (const auto& keyBound : AddedKeyBounds_) {
        Mapping_[keyBound];
    }

    // Prepare component-wise image prefixes.
    ComponentWisePrefixes_.resize(SortedKeyBounds_.size());
    for (int index = 0; index < ssize(SortedKeyBounds_); ++index) {
        ComponentWisePrefixes_[index] = RowBuffer_->AllocateUnversioned(SortedKeyBounds_[index].Prefix.GetCount());
    }

    // First, calculate global images.
    // Recall that we are identifying (assuming comparator of length 2):
    // * >=[foo, 2], >[foo, 2], <=[foo, 2], <[foo, 2]
    // * >=[foo], <[foo]
    // * >[foo], <=[foo]
    // * but not >=[foo] with [foo].
    for (
        int beginIndex = 0, endIndex = 0, currentImage = 0;
        beginIndex < ssize(SortedKeyBounds_);
        beginIndex = endIndex, ++currentImage)
    {
        auto beginBound = SortedKeyBounds_[beginIndex];

        while (true) {
            if (endIndex >= ssize(SortedKeyBounds_)) {
                break;
            }
            auto endBound = SortedKeyBounds_[endIndex];
            if (beginBound != endBound &&
                beginBound.Invert() != endBound &&
                (beginBound.Prefix != endBound.Prefix ||
                static_cast<int>(beginBound.Prefix.GetCount()) != Comparator_.GetLength()))
            {
                break;
            }
            Mapping_[endBound].Global = currentImage;
            ++endIndex;
        }
    }

    // Second, calculate component-wise images.
    CalculateComponentWise(/*fromIndex*/ 0, /*toIndex*/ SortedKeyBounds_.size(), /*componentIndex*/ 0);
    for (int index = 0; index < ssize(SortedKeyBounds_); ++index) {
        const auto& keyBound = SortedKeyBounds_[index];
        auto& componentWiseImage = Mapping_[keyBound].ComponentWise;
        componentWiseImage.Prefix = ComponentWisePrefixes_[index];
        componentWiseImage.IsInclusive = SortedKeyBounds_[index].IsInclusive;
        componentWiseImage.IsUpper = SortedKeyBounds_[index].IsUpper;
    }
}

void TKeyBoundCompressor::CalculateComponentWise(int fromIndex, int toIndex, int componentIndex)
{
    if (fromIndex == toIndex || componentIndex == Comparator_.GetLength()) {
        return;
    }
    for (
        int beginIndex = fromIndex, endIndex = fromIndex, currentImage = 0;
        beginIndex < toIndex;
        beginIndex = endIndex, ++currentImage)
    {
        // Skip a bunch of key bounds that are too short to have currently processed component.
        while (true) {
            if (beginIndex >= toIndex) {
                break;
            }
            auto beginBound = SortedKeyBounds_[beginIndex];
            if (static_cast<int>(beginBound.Prefix.GetCount()) > componentIndex) {
                break;
            }
            ++beginIndex;
        }
        if (beginIndex >= toIndex) {
            break;
        }

        // Extract a contiguous segment of key bounds sharing the same value in current component.
        endIndex = beginIndex;
        auto beginBound = SortedKeyBounds_[beginIndex];
        while (true) {
            if (endIndex >= toIndex) {
                break;
            }
            auto endBound = SortedKeyBounds_[endIndex];
            if (static_cast<int>(endBound.Prefix.GetCount()) <= componentIndex) {
                break;
            }
            if (endBound.Prefix[componentIndex] != beginBound.Prefix[componentIndex]) {
                break;
            }
            ComponentWisePrefixes_[endIndex][componentIndex] = MakeUnversionedInt64Value(currentImage, componentIndex);
            ++endIndex;
        }

        CalculateComponentWise(beginIndex, endIndex, componentIndex + 1);
    }
}

TKeyBoundCompressor::TImage TKeyBoundCompressor::GetImage(TKeyBound keyBound) const
{
    YT_VERIFY(MappingInitialized_);
    return GetOrCrash(Mapping_, keyBound);
}

void TKeyBoundCompressor::Dump(const TLogger& logger)
{
    YT_VERIFY(MappingInitialized_);

    TStructuredLogBatcher batcher(logger);

    for (const auto& keyBound : SortedKeyBounds_) {
        auto image = GetImage(keyBound);
        batcher.AddItemFluently()
            .BeginList()
                .Item().Value(keyBound)
                .Item().Value(image.ComponentWise)
                .Item().Value(image.Global)
            .EndList();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
