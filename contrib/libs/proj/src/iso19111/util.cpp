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

#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

#include "proj/util.hpp"
#include "proj/io.hpp"

#include "proj/internal/internal.hpp"

#include <map>
#include <memory>
#include <string>

using namespace NS_PROJ::internal;

#if 0
namespace dropbox{ namespace oxygen {
template<> nn<NS_PROJ::util::BaseObjectPtr>::~nn() = default;
template<> nn<NS_PROJ::util::BoxedValuePtr>::~nn() = default;
template<> nn<NS_PROJ::util::ArrayOfBaseObjectPtr>::~nn() = default;
template<> nn<NS_PROJ::util::LocalNamePtr>::~nn() = default;
template<> nn<NS_PROJ::util::GenericNamePtr>::~nn() = default;
template<> nn<NS_PROJ::util::NameSpacePtr>::~nn() = default;
}}
#endif

NS_PROJ_START
namespace util {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct BaseObject::Private {
    // This is a manual implementation of std::enable_shared_from_this<> that
    // avoids publicly deriving from it.
    std::weak_ptr<BaseObject> self_{};
};
//! @endcond

// ---------------------------------------------------------------------------

BaseObject::BaseObject() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
BaseObject::~BaseObject() = default;

// ---------------------------------------------------------------------------

BaseObjectNNPtr::~BaseObjectNNPtr() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
// cppcheck-suppress operatorEqVarError
BaseObject &BaseObject::operator=(BaseObject &&) {
    d->self_.reset();
    return *this;
}

//! @endcond

// ---------------------------------------------------------------------------

/** Keep a reference to ourselves as an internal weak pointer. So that
 * extractGeographicBaseObject() can later return a shared pointer on itself.
 */
void BaseObject::assignSelf(const BaseObjectNNPtr &self) {
    assert(self.get() == this);
    d->self_ = self.as_nullable();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
BaseObjectNNPtr BaseObject::shared_from_this() const {
    // This assertion checks that in all code paths where we create a
    // shared pointer, we took care of assigning it to self_, by calling
    // assignSelf();
    return NN_CHECK_ASSERT(d->self_.lock());
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct BoxedValue::Private {
    BoxedValue::Type type_{BoxedValue::Type::INTEGER};
    std::string stringValue_{};
    int integerValue_{};
    bool booleanValue_{};

    explicit Private(const std::string &stringValueIn)
        : type_(BoxedValue::Type::STRING), stringValue_(stringValueIn) {}

    explicit Private(int integerValueIn)
        : type_(BoxedValue::Type::INTEGER), integerValue_(integerValueIn) {}

    explicit Private(bool booleanValueIn)
        : type_(BoxedValue::Type::BOOLEAN), booleanValue_(booleanValueIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

BoxedValue::BoxedValue() : d(std::make_unique<Private>(std::string())) {}

// ---------------------------------------------------------------------------

/** \brief Constructs a BoxedValue from a string.
 */
BoxedValue::BoxedValue(const char *stringValueIn)
    : d(std::make_unique<Private>(
          std::string(stringValueIn ? stringValueIn : ""))) {}

// ---------------------------------------------------------------------------

/** \brief Constructs a BoxedValue from a string.
 */
BoxedValue::BoxedValue(const std::string &stringValueIn)
    : d(std::make_unique<Private>(stringValueIn)) {}

// ---------------------------------------------------------------------------

/** \brief Constructs a BoxedValue from an integer.
 */
BoxedValue::BoxedValue(int integerValueIn)
    : d(std::make_unique<Private>(integerValueIn)) {}

// ---------------------------------------------------------------------------

/** \brief Constructs a BoxedValue from a boolean.
 */
BoxedValue::BoxedValue(bool booleanValueIn)
    : d(std::make_unique<Private>(booleanValueIn)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
BoxedValue::BoxedValue(const BoxedValue &other)
    : d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

BoxedValue::~BoxedValue() = default;

// ---------------------------------------------------------------------------

const BoxedValue::Type &BoxedValue::type() const { return d->type_; }

// ---------------------------------------------------------------------------

const std::string &BoxedValue::stringValue() const { return d->stringValue_; }

// ---------------------------------------------------------------------------

int BoxedValue::integerValue() const { return d->integerValue_; }

// ---------------------------------------------------------------------------

bool BoxedValue::booleanValue() const { return d->booleanValue_; }
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct ArrayOfBaseObject::Private {
    std::vector<BaseObjectNNPtr> values_{};
};
//! @endcond
// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ArrayOfBaseObject::ArrayOfBaseObject() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

ArrayOfBaseObject::~ArrayOfBaseObject() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Adds an object to the array.
 *
 * @param obj the object to add.
 */
void ArrayOfBaseObject::add(const BaseObjectNNPtr &obj) {
    d->values_.emplace_back(obj);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::vector<BaseObjectNNPtr>::const_iterator ArrayOfBaseObject::begin() const {
    return d->values_.begin();
}

// ---------------------------------------------------------------------------

std::vector<BaseObjectNNPtr>::const_iterator ArrayOfBaseObject::end() const {
    return d->values_.end();
}

// ---------------------------------------------------------------------------

bool ArrayOfBaseObject::empty() const { return d->values_.empty(); }
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a ArrayOfBaseObject.
 *
 * @return a new ArrayOfBaseObject.
 */
ArrayOfBaseObjectNNPtr ArrayOfBaseObject::create() {
    return ArrayOfBaseObject::nn_make_shared<ArrayOfBaseObject>();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct PropertyMap::Private {
    std::list<std::pair<std::string, BaseObjectNNPtr>> list_{};

    // cppcheck-suppress functionStatic
    void set(const std::string &key, const BoxedValueNNPtr &val) {
        for (auto &pair : list_) {
            if (pair.first == key) {
                pair.second = val;
                return;
            }
        }
        list_.emplace_back(key, val);
    }
};
//! @endcond

// ---------------------------------------------------------------------------

PropertyMap::PropertyMap() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
PropertyMap::PropertyMap(const PropertyMap &other)
    : d(std::make_unique<Private>(*(other.d))) {}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
PropertyMap::~PropertyMap() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const BaseObjectNNPtr *PropertyMap::get(const std::string &key) const {
    for (const auto &pair : d->list_) {
        if (pair.first == key) {
            return &(pair.second);
        }
    }
    return nullptr;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void PropertyMap::unset(const std::string &key) {
    auto &list = d->list_;
    for (auto iter = list.begin(); iter != list.end(); ++iter) {
        if (iter->first == key) {
            list.erase(iter);
            return;
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Set a BaseObjectNNPtr as the value of a key. */
PropertyMap &PropertyMap::set(const std::string &key,
                              const BaseObjectNNPtr &val) {
    for (auto &pair : d->list_) {
        if (pair.first == key) {
            pair.second = val;
            return *this;
        }
    }
    d->list_.emplace_back(key, val);
    return *this;
}

// ---------------------------------------------------------------------------

/** \brief Set a string as the value of a key. */
PropertyMap &PropertyMap::set(const std::string &key, const std::string &val) {
    d->set(key, util::nn_make_shared<BoxedValue>(val));
    return *this;
}

// ---------------------------------------------------------------------------

/** \brief Set a string as the value of a key. */
PropertyMap &PropertyMap::set(const std::string &key, const char *val) {
    d->set(key, util::nn_make_shared<BoxedValue>(val));
    return *this;
}

// ---------------------------------------------------------------------------

/** \brief Set a integer as the value of a key. */
PropertyMap &PropertyMap::set(const std::string &key, int val) {
    d->set(key, util::nn_make_shared<BoxedValue>(val));
    return *this;
}

// ---------------------------------------------------------------------------

/** \brief Set a boolean as the value of a key. */
PropertyMap &PropertyMap::set(const std::string &key, bool val) {
    d->set(key, util::nn_make_shared<BoxedValue>(val));
    return *this;
}

// ---------------------------------------------------------------------------

/** \brief Set a vector of strings as the value of a key. */
PropertyMap &PropertyMap::set(const std::string &key,
                              const std::vector<std::string> &arrayIn) {
    ArrayOfBaseObjectNNPtr array = ArrayOfBaseObject::create();
    for (const auto &str : arrayIn) {
        array->add(util::nn_make_shared<BoxedValue>(str));
    }
    return set(key, array);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool PropertyMap::getStringValue(
    const std::string &key,
    std::string &outVal) const // throw(InvalidValueTypeException)
{
    for (const auto &pair : d->list_) {
        if (pair.first == key) {
            auto genVal = dynamic_cast<const BoxedValue *>(pair.second.get());
            if (genVal && genVal->type() == BoxedValue::Type::STRING) {
                outVal = genVal->stringValue();
                return true;
            }
            throw InvalidValueTypeException("Invalid value type for " + key);
        }
    }
    return false;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool PropertyMap::getStringValue(
    const std::string &key,
    optional<std::string> &outVal) const // throw(InvalidValueTypeException)
{
    for (const auto &pair : d->list_) {
        if (pair.first == key) {
            auto genVal = dynamic_cast<const BoxedValue *>(pair.second.get());
            if (genVal && genVal->type() == BoxedValue::Type::STRING) {
                outVal = genVal->stringValue();
                return true;
            }
            throw InvalidValueTypeException("Invalid value type for " + key);
        }
    }
    return false;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct GenericName::Private {};
//! @endcond

// ---------------------------------------------------------------------------

GenericName::GenericName() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

GenericName::GenericName(const GenericName &other)
    : d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
GenericName::~GenericName() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct NameSpace::Private {
    GenericNamePtr name{};
    bool isGlobal{};
    std::string separator = std::string(":");
    std::string separatorHead = std::string(":");
};
//! @endcond

// ---------------------------------------------------------------------------

NameSpace::NameSpace(const GenericNamePtr &nameIn)
    : d(std::make_unique<Private>()) {
    d->name = nameIn;
}

// ---------------------------------------------------------------------------

NameSpace::NameSpace(const NameSpace &other)
    : d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
NameSpace::~NameSpace() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns whether this is a global namespace. */
bool NameSpace::isGlobal() const { return d->isGlobal; }

// ---------------------------------------------------------------------------

NameSpaceNNPtr NameSpace::getGlobalFromThis() const {
    NameSpaceNNPtr ns(NameSpace::nn_make_shared<NameSpace>(*this));
    ns->d->isGlobal = true;
    ns->d->name = LocalName::make_shared<LocalName>("global");
    return ns;
}

// ---------------------------------------------------------------------------

/** \brief Returns the name of this namespace. */
const GenericNamePtr &NameSpace::name() const { return d->name; }

// ---------------------------------------------------------------------------

const std::string &NameSpace::separator() const { return d->separator; }

// ---------------------------------------------------------------------------

NameSpaceNNPtr NameSpace::createGLOBAL() {
    NameSpaceNNPtr ns(NameSpace::nn_make_shared<NameSpace>(
        LocalName::make_shared<LocalName>("global")));
    ns->d->isGlobal = true;
    return ns;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct LocalName::Private {
    NameSpacePtr scope{};
    std::string name{};
};
//! @endcond

// ---------------------------------------------------------------------------

LocalName::LocalName(const std::string &name) : d(std::make_unique<Private>()) {
    d->name = name;
}

// ---------------------------------------------------------------------------

LocalName::LocalName(const NameSpacePtr &ns, const std::string &name)
    : d(std::make_unique<Private>()) {
    d->scope = ns ? ns : static_cast<NameSpacePtr>(NameSpace::GLOBAL);
    d->name = name;
}

// ---------------------------------------------------------------------------

LocalName::LocalName(const LocalName &other)
    : GenericName(other), d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
LocalName::~LocalName() = default;
//! @endcond

// ---------------------------------------------------------------------------

const NameSpacePtr LocalName::scope() const {
    if (d->scope)
        return d->scope;
    return NameSpace::GLOBAL;
}

// ---------------------------------------------------------------------------

GenericNameNNPtr LocalName::toFullyQualifiedName() const {
    if (scope()->isGlobal())
        return LocalName::nn_make_shared<LocalName>(*this);

    return LocalName::nn_make_shared<LocalName>(
        d->scope->getGlobalFromThis(),
        d->scope->name()->toFullyQualifiedName()->toString() +
            d->scope->separator() + d->name);
}

// ---------------------------------------------------------------------------

std::string LocalName::toString() const { return d->name; }

// ---------------------------------------------------------------------------

/** \brief Instantiate a NameSpace.
 *
 * @param name name of the namespace.
 * @param properties Properties. Allowed keys are "separator" and
 * "separator.head".
 * @return a new NameFactory.
 */
NameSpaceNNPtr NameFactory::createNameSpace(const GenericNameNNPtr &name,
                                            const PropertyMap &properties) {
    NameSpaceNNPtr ns(NameSpace::nn_make_shared<NameSpace>(name));
    properties.getStringValue("separator", ns->d->separator);
    properties.getStringValue("separator.head", ns->d->separatorHead);

    return ns;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a LocalName.
 *
 * @param scope scope.
 * @param name string of the local name.
 * @return a new LocalName.
 */
LocalNameNNPtr NameFactory::createLocalName(const NameSpacePtr &scope,
                                            const std::string &name) {
    return LocalName::nn_make_shared<LocalName>(scope, name);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GenericName.
 *
 * @param scope scope.
 * @param parsedNames the components of the name.
 * @return a new GenericName.
 */
GenericNameNNPtr
NameFactory::createGenericName(const NameSpacePtr &scope,
                               const std::vector<std::string> &parsedNames) {
    std::string name;
    const std::string separator(scope ? scope->separator()
                                      : NameSpace::GLOBAL->separator());
    bool first = true;
    for (const auto &str : parsedNames) {
        if (!first)
            name += separator;
        first = false;
        name += str;
    }
    return LocalName::nn_make_shared<LocalName>(scope, name);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CodeList::~CodeList() = default;
//! @endcond

// ---------------------------------------------------------------------------

CodeList &CodeList::operator=(const CodeList &other) {
    name_ = other.name_;
    return *this;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Exception::Exception(const char *message) : msg_(message) {}

// ---------------------------------------------------------------------------

Exception::Exception(const std::string &message) : msg_(message) {}

// ---------------------------------------------------------------------------

Exception::Exception(const Exception &) = default;

// ---------------------------------------------------------------------------

Exception::~Exception() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** Return the exception text. */
const char *Exception::what() const noexcept { return msg_.c_str(); }

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
InvalidValueTypeException::InvalidValueTypeException(const char *message)
    : Exception(message) {}

// ---------------------------------------------------------------------------

InvalidValueTypeException::InvalidValueTypeException(const std::string &message)
    : Exception(message) {}

// ---------------------------------------------------------------------------

InvalidValueTypeException::~InvalidValueTypeException() = default;

// ---------------------------------------------------------------------------

InvalidValueTypeException::InvalidValueTypeException(
    const InvalidValueTypeException &) = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
UnsupportedOperationException::UnsupportedOperationException(
    const char *message)
    : Exception(message) {}

// ---------------------------------------------------------------------------

UnsupportedOperationException::UnsupportedOperationException(
    const std::string &message)
    : Exception(message) {}

// ---------------------------------------------------------------------------

UnsupportedOperationException::~UnsupportedOperationException() = default;

// ---------------------------------------------------------------------------

UnsupportedOperationException::UnsupportedOperationException(
    const UnsupportedOperationException &) = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
IComparable::~IComparable() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Returns whether an object is equivalent to another one.
 * @param other other object to compare to
 * @param criterion comparison criterion.
 * @param dbContext Database context, or nullptr.
 * @return true if objects are equivalent.
 */
bool IComparable::isEquivalentTo(
    const IComparable *other, Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    if (this == other)
        return true;
    return _isEquivalentTo(other, criterion, dbContext);
}

// ---------------------------------------------------------------------------

} // namespace util
NS_PROJ_END
