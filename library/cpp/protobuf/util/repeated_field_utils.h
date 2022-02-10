#pragma once

#include <google/protobuf/repeated_field.h>
#include <util/generic/vector.h>

template <typename T>
void RemoveRepeatedPtrFieldElement(google::protobuf::RepeatedPtrField<T>* repeated, unsigned index) {
    google::protobuf::RepeatedPtrField<T> r;
    Y_ASSERT(index < (unsigned)repeated->size());
    for (unsigned i = 0; i < (unsigned)repeated->size(); ++i) {
        if (i == index) {
            continue;
        }
        r.Add()->Swap(repeated->Mutable(i));
    }
    r.Swap(repeated);
}

namespace NProtoBuf {
    /// Move item to specified position
    template <typename TRepeated>
    static void MoveRepeatedFieldItem(TRepeated* field, size_t indexFrom, size_t indexTo) {
        if (!field->size() || indexFrom >= static_cast<size_t>(field->size()) || indexFrom == indexTo)
            return;
        if (indexTo >= static_cast<size_t>(field->size()))
            indexTo = field->size() - 1;
        if (indexFrom > indexTo) {
            for (size_t i = indexFrom; i > indexTo; --i)
                field->SwapElements(i, i - 1);
        } else {
            for (size_t i = indexFrom; i < indexTo; ++i)
                field->SwapElements(i, i + 1);
        }
    }

    template <typename T>
    static T* InsertRepeatedFieldItem(NProtoBuf::RepeatedPtrField<T>* field, size_t index) {
        T* ret = field->Add();
        MoveRepeatedFieldItem(field, field->size() - 1, index);
        return ret;
    }

    template <typename TRepeated> // suitable both for RepeatedField and RepeatedPtrField
    static void RemoveRepeatedFieldItem(TRepeated* field, size_t index) {
        if ((int)index >= field->size())
            return;

        for (int i = index + 1; i < field->size(); ++i)
            field->SwapElements(i - 1, i);

        field->RemoveLast();
    }

    template <typename TRepeated, typename TPred> // suitable both for RepeatedField and RepeatedPtrField
    static void RemoveRepeatedFieldItemIf(TRepeated* repeated, TPred p) {
        auto last = std::remove_if(repeated->begin(), repeated->end(), p);
        if (last != repeated->end()) {
            size_t countToRemove = repeated->end() - last;
            while (countToRemove--)
                repeated->RemoveLast();
        }
    }

    namespace NImpl {
        template <typename TRepeated>
        static void ShiftLeft(TRepeated* field, int begIndex, int endIndex, size_t shiftSize) {
            Y_ASSERT(begIndex <= field->size());
            Y_ASSERT(endIndex <= field->size());
            size_t shiftIndex = (int)shiftSize < begIndex ? begIndex - shiftSize : 0;
            for (int i = begIndex; i < endIndex; ++i, ++shiftIndex)
                field->SwapElements(shiftIndex, i);
        }
    }

    // Remove several items at once, could be more efficient compared to calling RemoveRepeatedFieldItem several times
    template <typename TRepeated>
    static void RemoveRepeatedFieldItems(TRepeated* field, const TVector<size_t>& sortedIndices) {
        if (sortedIndices.empty())
            return;

        size_t shift = 1;
        for (size_t i = 1; i < sortedIndices.size(); ++i, ++shift)
            NImpl::ShiftLeft(field, sortedIndices[i - 1] + 1, sortedIndices[i], shift);
        NImpl::ShiftLeft(field, sortedIndices.back() + 1, field->size(), shift);

        for (; shift > 0; --shift)
            field->RemoveLast();
    }

    template <typename TRepeated>
    static void ReverseRepeatedFieldItems(TRepeated* field) {
        for (int i1 = 0, i2 = field->size() - 1; i1 < i2; ++i1, --i2)
            field->SwapElements(i1, i2);
    }

}
