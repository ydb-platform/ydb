#ifndef JSON5EncoderCpp_native
#define JSON5EncoderCpp_native

#include <array>
#include <cstdint>
#include <type_traits>
#include <utility>

namespace JSON5EncoderCpp {

template <class From>
constexpr std::uint32_t cast_to_uint32(
    const From &unsigned_from,
    typename std::enable_if<
        !std::is_signed<From>::value
    >::type* = nullptr
) {
    return static_cast<std::uint32_t>(unsigned_from);
}

template <class From>
constexpr std::uint32_t cast_to_uint32(
    const From &from,
    typename std::enable_if<
        std::is_signed<From>::value
    >::type* = nullptr
) {
    return cast_to_uint32(static_cast<typename std::make_unsigned<From>::type>(from));
}

template <class From>
constexpr std::int32_t cast_to_int32(const From &from) {
    return static_cast<std::int32_t>(cast_to_uint32(from));
}

struct AlwaysTrue {
    inline AlwaysTrue() = default;
    inline ~AlwaysTrue() = default;

    inline AlwaysTrue(const AlwaysTrue&) = default;
    inline AlwaysTrue(AlwaysTrue&&) = default;
    inline AlwaysTrue &operator =(const AlwaysTrue&) = default;
    inline AlwaysTrue &operator =(AlwaysTrue&&) = default;

    template <class T>
    inline AlwaysTrue(T&&) : AlwaysTrue() {}

    template <class T>
    inline bool operator ==(T&&) const { return true; }

    inline operator bool () const { return true; }
};

static inline bool obj_has_iter(const PyObject *obj) {
    const auto *cls = Py_TYPE(obj);
    return cls->tp_iter != nullptr;
}

constexpr char HEX[] = "0123456789abcdef";

struct EscapeDct {
    using Item = std::array<char, 8>;  // length, upto 6 characters, terminator (actually not needed)
    static constexpr std::size_t length = 0x100;
    using Items = Item[length];

    static const Items items;
    static const std::uint64_t is_escaped_lo;
    static const std::uint64_t is_escaped_hi;

    static inline bool is_escaped(std::uint32_t c) {
        if (c < 0x40) {
            return (is_escaped_lo & (static_cast<std::uint64_t>(1) << c)) != 0;
        } else if (c < 0x80) {
            return (is_escaped_hi & (static_cast<std::uint64_t>(1) << (c - 0x40))) != 0;
        } else {
            return true;
        }
    }

    template <class S>
    static inline std::size_t find_unescaped_range(const S *start, Py_ssize_t length) {
        Py_ssize_t index = 0;
        while ((index < length) && !is_escaped(start[index])) {
            ++index;
        }
        return index;
    }
};

static inline bool unicode_is_lo_surrogate(std::uint32_t ch) {
    return 0xDC00u <= ch && ch <= 0xDFFFu;
}

static inline bool unicode_is_hi_surrogate(std::uint32_t ch) {
    return 0xD800u <= ch && ch <= 0xDBFFu;
}

static inline std::uint32_t unicode_join_surrogates(std::uint32_t hi, std::uint32_t lo) {
    return (((hi & 0x03FFu) << 10) | (lo & 0x03FFu)) + 0x10000u;
}


template <class T>
struct VoidT_ {
    using Value = void*;
};


template <typename T>
struct has_ob_shash {
    template <typename C> static std::uint8_t test(typename VoidT_<decltype((std::declval<C>().ob_shash, true))>::Value);
    template <typename C> static std::uint64_t test(...);
    enum { value = sizeof(test<T>(0)) == sizeof(std::uint8_t) };
};

template <typename T>
struct has_hash {
    template <typename C> static std::uint8_t test(typename VoidT_<decltype((std::declval<C>().hash, true))>::Value);
    template <typename C> static std::uint64_t test(...);
    enum { value = sizeof(test<T>(0)) == sizeof(std::uint8_t) };
};

template<class T, bool ob_shash = has_ob_shash<T>::value, bool hash = has_hash<T>::value>
struct ResetHash_;

template<class T>
struct ResetHash_ <T, true, false> {
    static inline void reset(T *obj) {
        obj->ob_shash = -1;  // CPython: str
    }
};

template<class T>
struct ResetHash_ <T, false, true> {
    static inline void reset(T *obj) {
        obj->hash = -1;  // CPython: bytes
    }
};

template<class T>
struct ResetHash_ <T, false, false> {
    static inline void reset(T *obj) {
        (void) 0;  // PyPy
    }
};

template <class T>
static inline void reset_hash(T *obj) {
    ResetHash_<T>::reset(obj);
}


template <typename T>
struct has_wstr {
    template <typename C> static std::uint8_t test(typename VoidT_<decltype((std::declval<C>().wstr, true))>::Value);
    template <typename C> static std::uint64_t test(...);
    enum { value = sizeof(test<T>(0)) == sizeof(std::uint8_t) };
};

template<class T, bool hash = has_wstr<T>::value>
struct ResetWstr_;

template<class T>
struct ResetWstr_ <T, true> {
    static inline void reset(T *obj) {
        obj->wstr = nullptr;  // CPython >= 3.12: absent
    }
};

template<class T>
struct ResetWstr_ <T, false> {
    static inline void reset(T *) {
        (void) 0;
    }
};

template <class T>
static inline void reset_wstr(T *obj) {
    ResetWstr_<T>::reset(obj);
}

template <typename T>
struct has_ready {
    template <typename C> static std::uint8_t test(typename VoidT_<decltype((std::declval<C>().state.ready, true))>::Value);
    template <typename C> static std::uint64_t test(...);
    enum { value = sizeof(test<T>(0)) == sizeof(std::uint8_t) };
};

template<class T, bool hash = has_ready<T>::value>
struct SetReady_;

template<class T>
struct SetReady_ <T, true> {
    static inline void set(T *obj) {
        obj->state.ready = true;  // CPython >= 3.12: absent
    }
};

template<class T>
struct SetReady_ <T, false> {
    static inline void set(T *) {
        (void) 0;
    }
};

template <class T>
static inline void set_ready(T *obj) {
    SetReady_<T>::set(obj);
}


static int iter_next(PyObject *iterator, PyObject **value) {
    Py_XDECREF(*value);
    PyObject *v = PyIter_Next(iterator);
    *value = v;
    if (v) {
        return true;
    } else if (!PyErr_Occurred()) {
        return 0;
    } else {
        return -1;
    }
}

static inline AlwaysTrue exception_thrown() {
    return true;
}

// https://stackoverflow.com/a/65258501/416224
#ifdef __GNUC__ // GCC 4.8+, Clang, Intel and other compilers compatible with GCC (-std=c++0x or above)
    [[noreturn]] inline __attribute__((always_inline)) void unreachable() { __builtin_unreachable(); }
#elif defined(_MSC_VER) // MSVC
    [[noreturn]] __forceinline void unreachable() { __assume(false); }
#else // ???
    inline void unreachable() {}
#endif

#include "./_escape_dct.hpp"

const EscapeDct ESCAPE_DCT;

const char VERSION[] =
#   include "./VERSION.inc"
;
static constexpr std::size_t VERSION_LENGTH = sizeof(VERSION) - 1;

const char LONGDESCRIPTION[] =
#   include "./DESCRIPTION.inc"
;
static constexpr std::size_t LONGDESCRIPTION_LENGTH = sizeof(LONGDESCRIPTION) - 1;

#ifdef __GNUC__
#   define JSON5EncoderCpp_expect(cond, likely) __builtin_expect(!!(cond), !!(likely))
#else
#   define JSON5EncoderCpp_expect(cond, likely) !!(cond)
#endif

}

#endif
