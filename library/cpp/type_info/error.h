#pragma once

//! @file error.h
//!
//! All error classes that one can encounter when working with type info library.

#include <util/generic/yexception.h>

namespace NTi {
    /// Base class for all exceptions that arise when working with Type Info library.
    class TException: public yexception {
    };

    /// Type Info API used in an unintended way.
    class TApiException: public TException {
    };

    /// Attempting to create an illegal type.
    ///
    /// For example, this exception is raised when attempting to create a struct with non-unique item names.
    class TIllegalTypeException: public TException {
    };

    /// Type deserializer got an invalid input.
    ///
    /// See `TType::Serialize()` and `TType::Deserialize()` for more info on type serialization/deserialization.
    class TDeserializationException: public TException {
    };

    /// No such item in type.
    class TItemNotFound: public TException {
    };
}
