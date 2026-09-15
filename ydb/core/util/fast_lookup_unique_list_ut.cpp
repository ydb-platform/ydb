#include "fast_lookup_unique_list.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

#include <algorithm>
#include <memory>

#include <util/stream/null.h>

using namespace NKikimr;

#define Ctest Cnull

Y_UNIT_TEST_SUITE(FastLookupUniqueList) {
    enum class EAction : ui8 {
        CopyAssign = 0,
        MoveAssign,
        CopyCreate,
        MoveCreate,
        Size,
        CheckRange,
        Back,
        PushBack,
        PopBack,
        ExtractBack,
        Front,
        PushFront,
        PopFront,
        ExtractFront,
        Contains,
        Erase,
        IsEmpty,
        Clear,
        __COUNT__,
    };

    TString PrintAction(EAction action) {
        switch (action) {
        case EAction::CopyAssign:
            return "CopyAssign";
        case EAction::MoveAssign:
            return "MoveAssign";
        case EAction::CopyCreate:
            return "CopyCreate";
        case EAction::MoveCreate:
            return "MoveCreate";
        case EAction::Size:
            return "Size";
        case EAction::CheckRange:
            return "CheckRange";
        case EAction::Back:
            return "Back";
        case EAction::PushBack:
            return "PushBack";
        case EAction::PopBack:
            return "PopBack";
        case EAction::ExtractBack:
            return "ExtractBack";
        case EAction::Front:
            return "Front";
        case EAction::PushFront:
            return "PushFront";
        case EAction::PopFront:
            return "PopFront";
        case EAction::ExtractFront:
            return "ExtractFront";
        case EAction::Contains:
            return "Contains";
        case EAction::Erase:
            return "Erase";
        case EAction::IsEmpty:
            return "IsEmpty";
        case EAction::Clear:
            return "Clear";
        default:
            Y_ABORT("Unknown action");
        }
    }

    Y_UNIT_TEST(Stress) {
        using T = ui32;
        std::vector<std::pair<EAction, T>> testHistory;
        std::unique_ptr<TFastLookupUniqueList<T>> data;
        std::unique_ptr<std::list<T>> reference;

        constexpr ui32 testSteps = 1000000;

        auto randomValue = [&] {
            return static_cast<T>(RandomNumber(testSteps / 20)); 
        };

        auto printDebugInfo = [&] {
            TStringStream str;
            str << "History# {" << Endl;
            for (const auto& [action, value] : testHistory) {
                str << PrintAction(action) << " " << static_cast<ui32>(action) << " " << value << ", ";
            }
            str << "}" << Endl;

            str << "Data# { ";
            for (const T& value : *data) {
                str << value << " ";
            }
            str << "}" << Endl;

            str << "Reference# { ";
            for (const T& value : *reference) {
                str << value << " ";
            }
            str << "}" << Endl;

            return str.Str();
        };

        auto removeFromReference = [&](const T& value) -> size_t {
            auto it = std::find(reference->begin(), reference->end(), value);
            if (it != reference->end()) {
                reference->erase(it);
                return 1;
            }
            return 0;
        };

        auto checkReference = [&] {
            auto it1 = data->begin();
            auto it2 = reference->begin();
            while (it1 != data->end()) {
                UNIT_ASSERT_C(it2 != reference->end(), printDebugInfo());
                UNIT_ASSERT_VALUES_EQUAL_C(*it1, *it2, printDebugInfo());
                ++it1;
                ++it2;
            }
            UNIT_ASSERT_C(it2 == reference->end(), printDebugInfo());
        };

        data.reset(new TFastLookupUniqueList<T>);
        reference.reset(new std::list<T>);

        for (ui32 step = 0; step < testSteps; ++step) {
            EAction action = static_cast<EAction>(RandomNumber(static_cast<ui32>(EAction::__COUNT__)));
            T value = randomValue();

            testHistory.emplace_back(action, value);

            Ctest << PrintAction(action) << " " << value << Endl;

            switch (action) {
            case EAction::CopyAssign: {
                std::unique_ptr<TFastLookupUniqueList<T>> newData(new TFastLookupUniqueList<T>);
                std::unique_ptr<std::list<T>> newReference(new std::list<T>);
                *newData = *data;
                *newReference = *reference;
                data.swap(newData);
                reference.swap(newReference);
                checkReference();
                break;
            }
            case EAction::MoveAssign: {
                std::unique_ptr<TFastLookupUniqueList<T>> newData(new TFastLookupUniqueList<T>);
                std::unique_ptr<std::list<T>> newReference(new std::list<T>);
                *newData = std::move(*data);
                *newReference = std::move(*reference);
                data.swap(newData);
                reference.swap(newReference);
                checkReference();
                break;
            }
            case EAction::CopyCreate: {
                std::unique_ptr<TFastLookupUniqueList<T>> newData(new TFastLookupUniqueList<T>(*data));
                std::unique_ptr<std::list<T>> newReference(new std::list<T>(*reference));
                data.swap(newData);
                reference.swap(newReference);
                checkReference();
                break;
            }
            case EAction::MoveCreate:{
                std::unique_ptr<TFastLookupUniqueList<T>> newData(new TFastLookupUniqueList<T>(std::move(*data)));
                std::unique_ptr<std::list<T>> newReference(new std::list<T>(std::move(*reference)));
                data.swap(newData);
                reference.swap(newReference);
                checkReference();
                break;
            }
            case EAction::Size: {
                UNIT_ASSERT_VALUES_EQUAL_C(data->Size(), reference->size(), printDebugInfo());
                break;
            }
            case EAction::CheckRange: {
                std::vector<T> valuesData;
                std::vector<T> valuesReference;
                for (const T& value : *data) {
                    valuesData.push_back(value);
                }
                for (const T& value : *reference) {
                    valuesReference.push_back(value);
                }
                UNIT_ASSERT_VALUES_EQUAL_C(valuesData.size(), valuesReference.size(), printDebugInfo());
                for (ui32 i = 0; i < valuesData.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL_C(valuesData[i], valuesReference[i], printDebugInfo());
                }
                break;
            }
            case EAction::Back: {
                if (reference->empty()) {
                    UNIT_ASSERT_C(data->IsEmpty(), printDebugInfo());
                    break;
                }
                UNIT_ASSERT_VALUES_EQUAL_C(data->Back(), reference->back(), printDebugInfo());
                break;
            }
            case EAction::PushBack:{
                data->PushBack(value);
                removeFromReference(value);
                reference->push_back(value);
                break;
            }
            case EAction::PopBack: {
                if (reference->empty()) {
                    UNIT_ASSERT_C(data->IsEmpty(), printDebugInfo());
                    break;
                }
                data->PopBack();
                reference->pop_back();
                break;
            }
            case EAction::ExtractBack: {
                if (reference->empty()) {
                    UNIT_ASSERT_C(data->IsEmpty(), printDebugInfo());
                    break;
                }
                T valueData = data->ExtractBack();
                T valueReference = reference->back();
                reference->pop_back();
                UNIT_ASSERT_VALUES_EQUAL_C(valueData, valueReference, printDebugInfo());
                break;
            }
            case EAction::Front: {
                if (reference->empty()) {
                    UNIT_ASSERT_C(data->IsEmpty(), printDebugInfo());
                    break;
                }
                UNIT_ASSERT_VALUES_EQUAL_C(data->Front(), reference->front(), printDebugInfo());
                break;
            }
            case EAction::PushFront: {
                data->PushFront(value);
                removeFromReference(value);
                reference->push_front(value);
                break;
            }
            case EAction::PopFront: {
                if (reference->empty()) {
                    UNIT_ASSERT_C(data->IsEmpty(), printDebugInfo());
                    break;
                }
                data->PopFront();
                reference->pop_front();
                break;
            }
            case EAction::ExtractFront: {
                if (reference->empty()) {
                    UNIT_ASSERT_C(data->IsEmpty(), printDebugInfo());
                    break;
                }
                T valueData = data->ExtractFront();
                T valueReference = reference->front();
                reference->pop_front();
                UNIT_ASSERT_VALUES_EQUAL_C(valueData, valueReference, printDebugInfo());
                break;
            }
            case EAction::Contains: {
                bool referenceContains = std::find(reference->begin(), reference->end(), value) != reference->end();
                UNIT_ASSERT_VALUES_EQUAL_C(data->Contains(value), referenceContains, printDebugInfo());
                break;
            }
            case EAction::Erase: {
                size_t erasedData = data->Erase(value);
                size_t erasedReference = removeFromReference(value);
                UNIT_ASSERT_VALUES_EQUAL_C(erasedData, erasedReference, printDebugInfo());
                break;
            }
            case EAction::IsEmpty: {
                UNIT_ASSERT_VALUES_EQUAL_C(data->IsEmpty(), reference->empty(), printDebugInfo());
                break;
            }
            case EAction::Clear: {
                data->Clear();
                reference->clear();
                break;
            }
            default: {
                UNIT_FAIL(printDebugInfo());
            }
            }
        }
    }
}
