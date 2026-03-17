/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  ISO19111:2019 implementation
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2018, Even Rouault <even dot rouault at spatialys dot com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

#ifndef UTIL_HH_INCLUDED
#define UTIL_HH_INCLUDED

#if !(__cplusplus >= 201103L || (defined(_MSC_VER) && _MSC_VER >= 1900))
#error Must have C++11 or newer.
#endif

// windows.h can conflict with Criterion::STRICT
#ifdef STRICT
#undef STRICT
#endif

#include <exception>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#ifndef NS_PROJ
/** osgeo namespace */
namespace osgeo {
/** osgeo.proj namespace */
namespace proj {}
} // namespace osgeo
#endif

//! @cond Doxygen_Suppress

#define PROJ_DLL

#ifndef PROJ_DLL
#if defined(_MSC_VER)
#ifdef PROJ_MSVC_DLL_EXPORT
#define PROJ_DLL __declspec(dllexport)
#else
#define PROJ_DLL __declspec(dllimport)
#endif
#elif defined(__GNUC__)
#define PROJ_DLL __attribute__((visibility("default")))
#else
#define PROJ_DLL
#endif
#endif

#ifndef PROJ_MSVC_DLL
#if defined(_MSC_VER)
#define PROJ_MSVC_DLL PROJ_DLL
#define PROJ_GCC_DLL
#define PROJ_INTERNAL
#elif defined(__GNUC__)
#define PROJ_MSVC_DLL
#define PROJ_GCC_DLL PROJ_DLL
#if !defined(__MINGW32__)
#define PROJ_INTERNAL __attribute__((visibility("hidden")))
#else
#define PROJ_INTERNAL
#endif
#else
#define PROJ_MSVC_DLL
#define PROJ_GCC_DLL
#define PROJ_INTERNAL
#endif
#define PROJ_FOR_TEST PROJ_DLL
#endif

#include "nn.hpp"

/* To allow customizing the base namespace of PROJ */
#ifdef PROJ_INTERNAL_CPP_NAMESPACE
#define NS_PROJ osgeo::internalproj
#define NS_PROJ_START                                                          \
    namespace osgeo {                                                          \
    namespace internalproj {
#define NS_PROJ_END                                                            \
    }                                                                          \
    }
#else
#ifndef NS_PROJ
#define NS_PROJ osgeo::proj
#define NS_PROJ_START                                                          \
    namespace osgeo {                                                          \
    namespace proj {
#define NS_PROJ_END                                                            \
    }                                                                          \
    }
#endif
#endif

// Private-implementation (Pimpl) pattern
#define PROJ_OPAQUE_PRIVATE_DATA                                               \
  private:                                                                     \
    struct PROJ_INTERNAL Private;                                              \
    std::unique_ptr<Private> d;                                                \
                                                                               \
  protected:                                                                   \
    PROJ_INTERNAL Private *getPrivate() noexcept { return d.get(); }           \
    PROJ_INTERNAL const Private *getPrivate() const noexcept {                 \
        return d.get();                                                        \
    }                                                                          \
                                                                               \
  private:

// To include in the protected/private section of a class definition,
// to be able to call make_shared on a protected/private constructor
#define INLINED_MAKE_SHARED                                                    \
    template <typename T, typename... Args>                                    \
    static std::shared_ptr<T> make_shared(Args &&...args) {                    \
        return std::shared_ptr<T>(new T(std::forward<Args>(args)...));         \
    }                                                                          \
    template <typename T, typename... Args>                                    \
    static util::nn_shared_ptr<T> nn_make_shared(Args &&...args) {             \
        return util::nn_shared_ptr<T>(                                         \
            util::i_promise_i_checked_for_null,                                \
            std::shared_ptr<T>(new T(std::forward<Args>(args)...)));           \
    }

// To include in the protected/private section of a class definition,
// to be able to call make_unique on a protected/private constructor
#define INLINED_MAKE_UNIQUE                                                    \
    template <typename T, typename... Args>                                    \
    static std::unique_ptr<T> make_unique(Args &&...args) {                    \
        return std::unique_ptr<T>(new T(std::forward<Args>(args)...));         \
    }

#ifdef DOXYGEN_ENABLED
#define PROJ_FRIEND(mytype)
#define PROJ_FRIEND_OPTIONAL(mytype)
#else
#define PROJ_FRIEND(mytype) friend class mytype
#define PROJ_FRIEND_OPTIONAL(mytype) friend class util::optional<mytype>
#endif

#ifndef PROJ_PRIVATE
#define PROJ_PRIVATE public
#endif

#if defined(__GNUC__)
#define PROJ_NO_INLINE __attribute__((noinline))
#define PROJ_NO_RETURN __attribute__((noreturn))
// Applies to a function that has no side effect.
#define PROJ_PURE_DECL const noexcept __attribute__((pure))
#else
#define PROJ_NO_RETURN
#define PROJ_NO_INLINE
#define PROJ_PURE_DECL const noexcept
#endif
#define PROJ_PURE_DEFN const noexcept

//! @endcond

NS_PROJ_START

//! @cond Doxygen_Suppress
namespace io {
class DatabaseContext;
using DatabaseContextPtr = std::shared_ptr<DatabaseContext>;
} // namespace io
//! @endcond

/** osgeo.proj.util namespace.
 *
 * \brief A set of base types from ISO 19103, \ref GeoAPI and other PROJ
 * specific classes.
 */
namespace util {

//! @cond Doxygen_Suppress
// Import a few classes from nn.hpp to expose them under our ::util namespace
// for conveniency.
using ::dropbox::oxygen::i_promise_i_checked_for_null;
using ::dropbox::oxygen::nn;
using ::dropbox::oxygen::nn_dynamic_pointer_cast;
using ::dropbox::oxygen::nn_make_shared;

// For return statements, to convert from derived type to base type
using ::dropbox::oxygen::nn_static_pointer_cast;

template <typename T> using nn_shared_ptr = nn<std::shared_ptr<T>>;

// Possible implementation of C++14 std::remove_reference_t
// (cf https://en.cppreference.com/w/cpp/types/remove_cv)
template <class T>
using remove_reference_t = typename std::remove_reference<T>::type;

// Possible implementation of C++14 std::remove_cv_t
// (cf https://en.cppreference.com/w/cpp/types/remove_cv)
template <class T> using remove_cv_t = typename std::remove_cv<T>::type;

// Possible implementation of C++20 std::remove_cvref
// (cf https://en.cppreference.com/w/cpp/types/remove_cvref)
template <class T> struct remove_cvref {
    typedef remove_cv_t<remove_reference_t<T>> type;
};

#define NN_NO_CHECK(p)                                                         \
    ::dropbox::oxygen::nn<                                                     \
        typename ::NS_PROJ::util::remove_cvref<decltype(p)>::type>(            \
        ::dropbox::oxygen::i_promise_i_checked_for_null, (p))

//! @endcond

// To avoid formatting differences between clang-format 3.8 and 7
#define PROJ_NOEXCEPT noexcept

//! @cond Doxygen_Suppress
// isOfExactType<MyType>(*p) checks that the type of *p is exactly MyType
template <typename TemplateT, typename ObjectT>
inline bool isOfExactType(const ObjectT &o) {
    return typeid(TemplateT).hash_code() == typeid(o).hash_code();
}
//! @endcond

/** \brief Loose transposition of [std::optional]
 * (https://en.cppreference.com/w/cpp/utility/optional) available from C++17. */
template <class T> class optional {
  public:
    //! @cond Doxygen_Suppress
    inline optional() : hasVal_(false) {}
    inline explicit optional(const T &val) : hasVal_(true), val_(val) {}
    inline explicit optional(T &&val)
        : hasVal_(true), val_(std::forward<T>(val)) {}

    inline optional(const optional &) = default;
    inline optional(optional &&other) PROJ_NOEXCEPT
        : hasVal_(other.hasVal_),
          // cppcheck-suppress functionStatic
          val_(std::forward<T>(other.val_)) {
        other.hasVal_ = false;
    }

    inline optional &operator=(const T &val) {
        hasVal_ = true;
        val_ = val;
        return *this;
    }
    inline optional &operator=(T &&val) noexcept {
        hasVal_ = true;
        val_ = std::forward<T>(val);
        return *this;
    }
    inline optional &operator=(const optional &) = default;
    inline optional &operator=(optional &&other) noexcept {
        hasVal_ = other.hasVal_;
        val_ = std::forward<T>(other.val_);
        other.hasVal_ = false;
        return *this;
    }

    inline T *operator->() { return &val_; }
    inline T &operator*() { return val_; }

    //! @endcond

    /** Returns a pointer to the contained value. */
    inline const T *operator->() const { return &val_; }

    /** Returns a reference to the contained value. */
    inline const T &operator*() const { return val_; }

    /** Return whether the optional has a value */
    inline explicit operator bool() const noexcept { return hasVal_; }

    /** Return whether the optional has a value */
    inline bool has_value() const noexcept { return hasVal_; }

  private:
    bool hasVal_;
    T val_{};
};

// ---------------------------------------------------------------------------

class BaseObject;
/** Shared pointer of BaseObject. */
using BaseObjectPtr = std::shared_ptr<BaseObject>;
#if 1
/** Non-null shared pointer of BaseObject. */
struct BaseObjectNNPtr : public util::nn<BaseObjectPtr> {
    // This trick enables to avoid inlining of the destructor.
    // This is mostly an alias of the base class.
    //! @cond Doxygen_Suppress
    template <class T>
    // cppcheck-suppress noExplicitConstructor
    BaseObjectNNPtr(const util::nn<std::shared_ptr<T>> &x)
        : util::nn<BaseObjectPtr>(x) {}

    template <class T>
    // cppcheck-suppress noExplicitConstructor
    BaseObjectNNPtr(util::nn<std::shared_ptr<T>> &&x) noexcept
        : util::nn<BaseObjectPtr>(NN_NO_CHECK(std::move(x.as_nullable()))) {}

    explicit BaseObjectNNPtr(::dropbox::oxygen::i_promise_i_checked_for_null_t,
                             BaseObjectPtr &&arg) noexcept
        : util::nn<BaseObjectPtr>(i_promise_i_checked_for_null,
                                  std::move(arg)) {}
    BaseObjectNNPtr(const BaseObjectNNPtr &) = default;
    BaseObjectNNPtr &operator=(const BaseObjectNNPtr &) = default;

    PROJ_DLL ~BaseObjectNNPtr();
    //! @endcond
};
#else
using BaseObjectNNPtr = util::nn<BaseObjectPtr>;
#endif

/** \brief Class that can be derived from, to emulate Java's Object behavior.
 */
class PROJ_GCC_DLL BaseObject {
  public:
    //! @cond Doxygen_Suppress
    virtual PROJ_DLL ~BaseObject();
    //! @endcond

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL BaseObjectNNPtr
        shared_from_this() const;
    //! @endcond

  protected:
    PROJ_INTERNAL BaseObject();
    PROJ_INTERNAL void assignSelf(const BaseObjectNNPtr &self);
    PROJ_INTERNAL BaseObject &operator=(BaseObject &&other);

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

/** \brief Interface for an object that can be compared to another.
 */
class PROJ_GCC_DLL IComparable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL virtual ~IComparable();
    //! @endcond

    /** \brief Comparison criterion. */
    enum class PROJ_MSVC_DLL Criterion {
        /** All properties are identical. */
        STRICT,

        /** The objects are equivalent for the purpose of coordinate
         * operations. They can differ by the name of their objects,
         * identifiers, other metadata.
         * Parameters may be expressed in different units, provided that the
         * value is (with some tolerance) the same once expressed in a
         * common unit.
         */
        EQUIVALENT,

        /** Same as EQUIVALENT, relaxed with an exception that the axis order
         * of the base CRS of a DerivedCRS/ProjectedCRS or the axis order of
         * a GeographicCRS is ignored. Only to be used
         * with DerivedCRS/ProjectedCRS/GeographicCRS */
        EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS,
    };

    PROJ_DLL bool
    isEquivalentTo(const IComparable *other,
                   Criterion criterion = Criterion::STRICT,
                   const io::DatabaseContextPtr &dbContext = nullptr) const;

    PROJ_PRIVATE :

        //! @cond Doxygen_Suppress
        PROJ_INTERNAL virtual bool
        _isEquivalentTo(
            const IComparable *other, Criterion criterion = Criterion::STRICT,
            const io::DatabaseContextPtr &dbContext = nullptr) const = 0;
    //! @endcond
};

// ---------------------------------------------------------------------------

/** \brief Encapsulate standard datatypes in an object.
 */
class BoxedValue final : public BaseObject {
  public:
    //! @cond Doxygen_Suppress
    /** Type of data stored in the BoxedValue. */
    enum class Type {
        /** a std::string */
        STRING,
        /** an integer */
        INTEGER,
        /** a boolean */
        BOOLEAN
    };
    //! @endcond

    // cppcheck-suppress noExplicitConstructor
    PROJ_DLL BoxedValue(const char *stringValueIn); // needed to avoid the bool
                                                    // constructor to be taken !
    // cppcheck-suppress noExplicitConstructor
    PROJ_DLL BoxedValue(const std::string &stringValueIn);
    // cppcheck-suppress noExplicitConstructor
    PROJ_DLL BoxedValue(int integerValueIn);
    // cppcheck-suppress noExplicitConstructor
    PROJ_DLL BoxedValue(bool booleanValueIn);

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL
        BoxedValue(const BoxedValue &other);

    PROJ_DLL ~BoxedValue() override;

    PROJ_INTERNAL const Type &type() const;
    PROJ_INTERNAL const std::string &stringValue() const;
    PROJ_INTERNAL int integerValue() const;
    PROJ_INTERNAL bool booleanValue() const;
    //! @endcond

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    BoxedValue &operator=(const BoxedValue &) = delete;

    PROJ_INTERNAL BoxedValue();
};

/** Shared pointer of BoxedValue. */
using BoxedValuePtr = std::shared_ptr<BoxedValue>;
/** Non-null shared pointer of BoxedValue. */
using BoxedValueNNPtr = util::nn<BoxedValuePtr>;

// ---------------------------------------------------------------------------

class ArrayOfBaseObject;
/** Shared pointer of ArrayOfBaseObject. */
using ArrayOfBaseObjectPtr = std::shared_ptr<ArrayOfBaseObject>;
/** Non-null shared pointer of ArrayOfBaseObject. */
using ArrayOfBaseObjectNNPtr = util::nn<ArrayOfBaseObjectPtr>;

/** \brief Array of BaseObject.
 */
class ArrayOfBaseObject final : public BaseObject {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~ArrayOfBaseObject() override;
    //! @endcond

    PROJ_DLL void add(const BaseObjectNNPtr &obj);

    PROJ_DLL static ArrayOfBaseObjectNNPtr create();

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        std::vector<BaseObjectNNPtr>::const_iterator
        begin() const;
    std::vector<BaseObjectNNPtr>::const_iterator end() const;
    bool empty() const;
    //! @endcond

  protected:
    ArrayOfBaseObject();
    INLINED_MAKE_SHARED

  private:
    ArrayOfBaseObject(const ArrayOfBaseObject &other) = delete;
    ArrayOfBaseObject &operator=(const ArrayOfBaseObject &other) = delete;
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

/** \brief Wrapper of a std::map<std::string, BaseObjectNNPtr> */
class PropertyMap {
  public:
    PROJ_DLL PropertyMap();
    //! @cond Doxygen_Suppress
    PROJ_DLL PropertyMap(const PropertyMap &other);
    PROJ_DLL ~PropertyMap();
    //! @endcond

    PROJ_DLL PropertyMap &set(const std::string &key,
                              const BaseObjectNNPtr &val);

    //! @cond Doxygen_Suppress
    template <class T>
    inline PropertyMap &set(const std::string &key,
                            const nn_shared_ptr<T> &val) {
        return set(
            key, BaseObjectNNPtr(i_promise_i_checked_for_null,
                                 BaseObjectPtr(val.as_nullable(), val.get())));
    }
    //! @endcond

    // needed to avoid the bool constructor to be taken !
    PROJ_DLL PropertyMap &set(const std::string &key, const char *val);

    PROJ_DLL PropertyMap &set(const std::string &key, const std::string &val);

    PROJ_DLL PropertyMap &set(const std::string &key, int val);

    PROJ_DLL PropertyMap &set(const std::string &key, bool val);

    PROJ_DLL PropertyMap &set(const std::string &key,
                              const std::vector<std::string> &array);

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        const BaseObjectNNPtr *
        get(const std::string &key) const;

    // throw(InvalidValueTypeException)
    bool getStringValue(const std::string &key, std::string &outVal) const;
    bool getStringValue(const std::string &key,
                        optional<std::string> &outVal) const;
    void unset(const std::string &key);

    static PropertyMap createAndSetName(const char *name);
    static PropertyMap createAndSetName(const std::string &name);
    //! @endcond

  private:
    PropertyMap &operator=(const PropertyMap &) = delete;

    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class LocalName;
/** Shared pointer of LocalName. */
using LocalNamePtr = std::shared_ptr<LocalName>;
/** Non-null shared pointer of LocalName. */
using LocalNameNNPtr = util::nn<LocalNamePtr>;

class NameSpace;
/** Shared pointer of NameSpace. */
using NameSpacePtr = std::shared_ptr<NameSpace>;
/** Non-null shared pointer of NameSpace. */
using NameSpaceNNPtr = util::nn<NameSpacePtr>;

class GenericName;
/** Shared pointer of GenericName. */
using GenericNamePtr = std::shared_ptr<GenericName>;
/** Non-null shared pointer of GenericName. */
using GenericNameNNPtr = util::nn<GenericNamePtr>;

// ---------------------------------------------------------------------------

/** \brief A sequence of identifiers rooted within the context of a namespace.
 *
 * \remark Simplified version of [GenericName]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/util/GenericName.html)
 * from \ref GeoAPI
 */
class GenericName : public BaseObject {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL virtual ~GenericName() override;
    //! @endcond

    /** \brief Return the scope of the object, possibly a global one. */
    PROJ_DLL virtual const NameSpacePtr scope() const = 0;

    /** \brief Return the LocalName as a string. */
    PROJ_DLL virtual std::string toString() const = 0;

    /** \brief Return a fully qualified name corresponding to the local name.
     *
     * The namespace of the resulting name is a global one.
     */
    PROJ_DLL virtual GenericNameNNPtr toFullyQualifiedName() const = 0;

  protected:
    GenericName();
    GenericName(const GenericName &other);

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    GenericName &operator=(const GenericName &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief A domain in which names given by strings are defined.
 *
 * \remark Simplified version of [NameSpace]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/util/NameSpace.html)
 * from \ref GeoAPI
 */
class NameSpace {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~NameSpace();
    //! @endcond

    PROJ_DLL bool isGlobal() const;
    PROJ_DLL const GenericNamePtr &name() const;

  protected:
    PROJ_FRIEND(NameFactory);
    PROJ_FRIEND(LocalName);
    explicit NameSpace(const GenericNamePtr &name);
    NameSpace(const NameSpace &other);
    NameSpaceNNPtr getGlobalFromThis() const;
    const std::string &separator() const;
    static const NameSpaceNNPtr GLOBAL;
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    NameSpace &operator=(const NameSpace &other) = delete;

    static NameSpaceNNPtr createGLOBAL();
};

// ---------------------------------------------------------------------------

/** \brief Identifier within a NameSpace for a local object.
 *
 * Local names are names which are directly accessible to and maintained by a
 * NameSpace within which they are local, indicated by the scope.
 *
 * \remark Simplified version of [LocalName]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/util/LocalName.html)
 * from \ref GeoAPI
 */
class LocalName : public GenericName {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~LocalName() override;
    //! @endcond

    PROJ_DLL const NameSpacePtr scope() const override;
    PROJ_DLL std::string toString() const override;
    PROJ_DLL GenericNameNNPtr toFullyQualifiedName() const override;

  protected:
    PROJ_FRIEND(NameFactory);
    PROJ_FRIEND(NameSpace);
    explicit LocalName(const std::string &nameIn);
    LocalName(const LocalName &other);
    LocalName(const NameSpacePtr &ns, const std::string &name);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    LocalName &operator=(const LocalName &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Factory for generic names.
 *
 * \remark Simplified version of [NameFactory]
 * (http://www.geoapi.org/3.0/javadoc/org.opengis.geoapi/org/opengis/util/NameFactory.html)
 * from \ref GeoAPI
 */
class NameFactory {
  public:
    PROJ_DLL static NameSpaceNNPtr
    createNameSpace(const GenericNameNNPtr &name,
                    const PropertyMap &properties);
    PROJ_DLL static LocalNameNNPtr createLocalName(const NameSpacePtr &scope,
                                                   const std::string &name);
    PROJ_DLL static GenericNameNNPtr
    createGenericName(const NameSpacePtr &scope,
                      const std::vector<std::string> &parsedNames);
};

// ---------------------------------------------------------------------------

/** \brief Abstract class to define an enumeration of values.
 */
class CodeList {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~CodeList();
    //! @endcond

    /** Return the CodeList item as a string. */
    // cppcheck-suppress functionStatic
    inline const std::string &toString() PROJ_PURE_DECL { return name_; }

    /** Return the CodeList item as a string. */
    inline operator std::string() PROJ_PURE_DECL { return toString(); }

    //! @cond Doxygen_Suppress
    inline bool operator==(const CodeList &other) PROJ_PURE_DECL {
        return name_ == other.name_;
    }
    inline bool operator!=(const CodeList &other) PROJ_PURE_DECL {
        return name_ != other.name_;
    }
    //! @endcond
  protected:
    explicit CodeList(const std::string &nameIn) : name_(nameIn) {}
    CodeList(const CodeList &) = default;
    CodeList &operator=(const CodeList &other);

  private:
    std::string name_{};
};

// ---------------------------------------------------------------------------

/** \brief Root exception class.
 */
class PROJ_GCC_DLL Exception : public std::exception {
    std::string msg_;

  public:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit Exception(const char *message);
    PROJ_INTERNAL explicit Exception(const std::string &message);
    PROJ_DLL Exception(const Exception &other);
    PROJ_DLL ~Exception() override;
    //! @endcond
    PROJ_DLL virtual const char *what() const noexcept override;
};

// ---------------------------------------------------------------------------

/** \brief Exception thrown when an invalid value type is set as the value of
 * a key of a PropertyMap.
 */
class PROJ_GCC_DLL InvalidValueTypeException : public Exception {
  public:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit InvalidValueTypeException(const char *message);
    PROJ_INTERNAL explicit InvalidValueTypeException(
        const std::string &message);
    PROJ_DLL InvalidValueTypeException(const InvalidValueTypeException &other);
    PROJ_DLL ~InvalidValueTypeException() override;
    //! @endcond
};

// ---------------------------------------------------------------------------

/** \brief Exception Thrown to indicate that the requested operation is not
 * supported.
 */
class PROJ_GCC_DLL UnsupportedOperationException : public Exception {
  public:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit UnsupportedOperationException(const char *message);
    PROJ_INTERNAL explicit UnsupportedOperationException(
        const std::string &message);
    PROJ_DLL
    UnsupportedOperationException(const UnsupportedOperationException &other);
    PROJ_DLL ~UnsupportedOperationException() override;
    //! @endcond
};

} // namespace util

NS_PROJ_END

#endif // UTIL_HH_INCLUDED
