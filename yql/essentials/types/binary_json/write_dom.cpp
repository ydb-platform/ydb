#include "write_internal.h"

#include <yql/essentials/minikql/dom/node.h>

namespace NKikimr::NBinaryJson {

using namespace NYql::NDom;
using namespace NYql::NUdf;

void DomToJsonIndex(const NUdf::TUnboxedValue& value, NJson::TJsonCallbacks& callbacks) {
    switch (GetNodeType(value)) {
        case ENodeType::String: {
            auto cleanValue = ClearUtf8Mark(value);
            callbacks.OnString(cleanValue.AsStringRef());
            break;
        }
        case ENodeType::Bool:
            callbacks.OnBoolean(value.Get<bool>());
            break;
        case ENodeType::Int64:
            callbacks.OnInteger(value.Get<i64>());
            break;
        case ENodeType::Uint64:
            callbacks.OnUInteger(value.Get<ui64>());
            break;
        case ENodeType::Double:
            callbacks.OnDouble(value.Get<double>());
            break;
        case ENodeType::Entity:
            callbacks.OnNull();
            break;
        case ENodeType::List: {
            callbacks.OnOpenArray();

            if (value.IsBoxed()) {
                const auto it = value.GetListIterator();
                TUnboxedValue current;
                while (it.Next(current)) {
                    DomToJsonIndex(current, callbacks);
                }
            }

            callbacks.OnCloseArray();
            break;
        }
        case ENodeType::Dict:
        case ENodeType::Attr: {
            callbacks.OnOpenMap();

            if (value.IsBoxed()) {
                const auto it = value.GetDictIterator();
                TUnboxedValue key;
                TUnboxedValue value;
                while (it.NextPair(key, value)) {
                    auto cleanKey = ClearUtf8Mark(key);
                    callbacks.OnMapKey(cleanKey.AsStringRef());
                    DomToJsonIndex(value, callbacks);
                }
            }

            callbacks.OnCloseMap();
            break;
        }
    }
}

TBinaryJson SerializeToBinaryJson(const NUdf::TUnboxedValue& value) {
    return SerializeToBinaryJson([&value](NJson::TJsonCallbacks& callbacks) {
        DomToJsonIndex(value, callbacks);
    });
}

} // namespace NKikimr::NBinaryJson
