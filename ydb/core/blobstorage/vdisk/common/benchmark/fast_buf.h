#pragma once

#include "google/protobuf/message.h"
#include "google/protobuf/stubs/port.h"
#include "util/generic/vector.h"
#include "util/system/yassert.h"
#include <cstddef>
#include <limits>
#include <optional>

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/system/types.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NYdb::NFastBuf {

// TFastBuf is a tag class
// Save and Load methods should be overloaded
class TFastBuf {
public:
    // void Save(IOutputStream*) const {
    //     Y_FAIL("Not implemented");
    // }
    // void Load(IInputStream*) {
    //     Y_FAIL("Not implemented");
    // }
    // ui32 SerializedSize() const {
    //     Y_FAIL("Not implemented");
    //     return 0;
    // }
};

namespace detail {

template <typename T>
struct TTraits {
    static void Save(const T& value, IOutputStream& out) {
        if constexpr (std::is_base_of_v<TFastBuf, T>) {
            value.Save(&out);
        } else if constexpr (std::is_base_of_v<::google::protobuf::Message, T>) {
            value.Save(&out);
        } else {
            out.Write(&value, sizeof(T));
        }
    }

    static void Load(T& value, IInputStream& in) {
        if constexpr (std::is_base_of_v<TFastBuf, T>) {
            value.Load(&in);
        } else if constexpr (std::is_base_of_v<::google::protobuf::Message, T>) {
            value.Load(&in);
        } else {
            size_t read = in.Read(&value, sizeof(T));
            Y_ASSERT(read == sizeof(T));
        }
    }

    static ui32 SerializedSize(const T& value) {
        if constexpr (std::is_base_of_v<TFastBuf, T>) {
            return value.SerializedSize();
        } else if constexpr (std::is_base_of_v<::google::protobuf::Message, T>) {
            return value.ByteSize();
        } else {
            return sizeof(T);
        }
    }
};

template <typename T>
struct TTraits<std::optional<T>> {
    static void Save(const std::optional<T>& value, IOutputStream& out) {
        ui8 has_value = value.has_value() ? std::numeric_limits<ui8>::max() : 0;
        out.Write(&has_value, sizeof(has_value));
        if (has_value) {
            TTraits<T>::Save(*value, out);
        }
    }

    static void Load(std::optional<T>& value, IInputStream& in) {
        ui8 has_value = 0;
        in.Read(&has_value, sizeof(has_value));
        if (has_value) {
            T v;
            TTraits<T>::Load(v, in);
            value.emplace(std::move(v));
        }
    }

    static ui32 SerializedSize(const std::optional<T>& value) {
        if (value.has_value()) {
            return sizeof(ui8) + TTraits<T>::SerializedSize(*value);
        } else {
            return sizeof(ui8);
        }
    }
};

template <typename T, typename A>
struct TTraits<TVector<T, A>> {
    static void Save(const TVector<T, A>& value, IOutputStream& out) {
        arc_ui64 size = value.size();
        out.Write(&size, sizeof(size));
        for (auto& it : value) {
            TTraits<T>::Save(it, out);
        }
    }

    static void Load(TVector<T, A>& value, IInputStream& in) {
        arc_ui64 size = 0;
        in.Read(&size, sizeof(size));
        value.assign(size, {});
        for (auto& it : value) {
            TTraits<T>::Load(it, in);
        }
    }

    static ui32 SerializedSize(const TVector<T, A>& value) {
        ui32 answer = sizeof(arc_ui64);
        for (auto& it : value) {
            answer += TTraits<T>::SerializedSize(it);
        }
        return answer;
    }
};

template <>
struct TTraits<TProtoStringType> {
    static void Save(const TProtoStringType& value, IOutputStream& out) {
        arc_ui64 size = value.Size();
        out.Write(&size, sizeof(size));
        out.Write(value.Data(), value.Size());
    }

    static void Load(TProtoStringType& value, IInputStream& in) {
        arc_ui64 size = 0;
        in.Read(&size, sizeof(size));
        value.ReserveAndResize(size);
        in.Read(const_cast<char*>(value.Data()), size);
    }

    static ui32 SerializedSize(const TProtoStringType& value) {
        return sizeof(arc_ui64) + value.Size();
    }
};

} // namespace detail

template <typename T>
void Save(const T& value, IOutputStream* out) {
    if (out == nullptr) {
        return;
    }

    ::NYdb::NFastBuf::detail::TTraits<T>::Save(value, *out);
}

template <typename T>
void Load(T& value, IInputStream* in) {
    if (in == nullptr) {
        return;
    }

    ::NYdb::NFastBuf::detail::TTraits<T>::Load(value, *in);
}

template <typename T>
ui32 SerializedSize(const T& value) {
    return ::NYdb::NFastBuf::detail::TTraits<T>::SerializedSize(value);
}

} // namespace NYdb::NFastBuf
