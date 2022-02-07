#pragma once

#include "cyson_enums.h"
#include "scalar.h"

#include <util/generic/strbuf.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

namespace NYsonPull {
    //! A well-formed decoded YSON stream can be described by the following grammar:
    //!
    //! STREAM[node]          ::= begin_stream VALUE end_stream
    //! STREAM[list_fragment] ::= begin_stream LIST_FRAGMENT end_stream
    //! STREAM[map_fragment]  ::= begin_stream MAP_FRAGMENT end_stream
    //! LIST_FRAGMENT         ::= { VALUE; }
    //! MAP_FRAGMENT          ::= { KEY VALUE; }
    //! KEY                   ::= key(String)
    //! VALUE                 ::= VALUE_NOATTR | ATTRIBUTES VALUE_NOATTR
    //! ATTRIBUTES            ::= begin_attributes MAP_FRAGMENT end_attributes
    //! VALUE_NOATTR          ::= scalar(Scalar) | LIST | MAP
    //! LIST                  ::= begin_list LIST_FRAGMENT end_list
    //! MAP                   ::= begin_map MAP_FRAGMENT end_map

    //! \brief YSON event type tag. Corresponds to YSON grammar.
    enum class EEventType {
        BeginStream = YSON_EVENT_BEGIN_STREAM,
        EndStream = YSON_EVENT_END_STREAM,
        BeginList = YSON_EVENT_BEGIN_LIST,
        EndList = YSON_EVENT_END_LIST,
        BeginMap = YSON_EVENT_BEGIN_MAP,
        EndMap = YSON_EVENT_END_MAP,
        BeginAttributes = YSON_EVENT_BEGIN_ATTRIBUTES,
        EndAttributes = YSON_EVENT_END_ATTRIBUTES,
        Key = YSON_EVENT_KEY,
        Scalar = YSON_EVENT_SCALAR,
    };

    //! \brief YSON event variant type.
    class TEvent {
        EEventType Type_;
        TScalar Value_;

    public:
        //! \brief Construct a tag-only event.
        explicit constexpr TEvent(EEventType type = EEventType::BeginStream)
            : Type_{type} {
        }

        //! \brief Construct a tag+value event.
        //!
        //! Only \p EEventType::key is meaningful.
        constexpr TEvent(EEventType type, const TScalar& value)
            : Type_{type}
            , Value_{value} {
        }

        //! \brief Construct a \p EEventType::scalar event.
        explicit constexpr TEvent(const TScalar& value)
            : Type_{EEventType::Scalar}
            , Value_{value} {
        }

        EEventType Type() const {
            return Type_;
        }

        //! \brief Get TScalar value.
        //!
        //! Undefined behaviour when event type is not \p EEventType::scalar.
        const TScalar& AsScalar() const {
            Y_ASSERT(Type_ == EEventType::Scalar || Type_ == EEventType::Key);
            return Value_;
        }

        //! \brief Get string value.
        //!
        //! Undefined behaviour when event type is not \p EEventType::key.
        TStringBuf AsString() const {
            Y_ASSERT(Type_ == EEventType::Key || (Type_ == EEventType::Scalar && Value_.Type() == EScalarType::String));
            return Value_.AsString();
        }
    };

}
