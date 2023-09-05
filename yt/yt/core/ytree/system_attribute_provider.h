#pragma once

#include "public.h"
#include "interned_attributes.h"
#include "permission.h"

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/actions/future.h>

#include <optional>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct ISystemAttributeProvider
{
    virtual ~ISystemAttributeProvider() = default;

    //! Describes a system attribute.
    struct TAttributeDescriptor
    {
        TInternedAttributeKey InternedKey = InvalidInternedAttribute;
        bool Present = true;
        bool Opaque = false;
        bool Custom = false;
        bool Writable = false;
        bool Removable = false;
        bool Replicated = false;
        bool Mandatory = false;
        bool External = false;
        EPermissionSet ModifyPermission = EPermission::Write;

        TAttributeDescriptor& SetPresent(bool value)
        {
            Present = value;
            return *this;
        }

        TAttributeDescriptor& SetOpaque(bool value)
        {
            Opaque = value;
            return *this;
        }

        TAttributeDescriptor& SetCustom(bool value)
        {
            Custom = value;
            return *this;
        }

        TAttributeDescriptor& SetWritable(bool value)
        {
            Writable = value;
            return *this;
        }

        TAttributeDescriptor& SetRemovable(bool value)
        {
            Removable = value;
            return *this;
        }

        TAttributeDescriptor& SetReplicated(bool value)
        {
            Replicated = value;
            return *this;
        }

        TAttributeDescriptor& SetMandatory(bool value)
        {
            Mandatory = value;
            return *this;
        }

        TAttributeDescriptor& SetExternal(bool value)
        {
            External = value;
            return *this;
        }

        TAttributeDescriptor& SetWritePermission(EPermission value)
        {
            ModifyPermission = value;
            return *this;
        }

        TAttributeDescriptor(TInternedAttributeKey key)
            : InternedKey(key)
        { }
    };

    //! Populates the list of all system attributes supported by this object.
    /*!
     *  \note
     *  Must not clear #attributes since additional items may be added in inheritors.
     */
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) = 0;

    //! Returns a (typically cached) set consisting of all non-custom attributes keys.
    //! \see TAttributeDescriptor::Custom
    //! \see ListSystemAttributes
    virtual const THashSet<TInternedAttributeKey>& GetBuiltinAttributeKeys() = 0;

    //! Gets the value of a builtin attribute.
    /*!
     *  \returns |false| if there is no builtin attribute with the given key.
     */
    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) = 0;

    //! Asynchronously gets the value of a builtin attribute.
    /*!
     *  \returns A future representing attribute value or null if there is no such async builtin attribute.
     */
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) = 0;

    //! Sets the value of a builtin attribute.
    /*!
     *  \returns |false| if there is no writable builtin attribute with the given key.
     */
    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const NYson::TYsonString& value, bool force) = 0;

    //! Removes the builtin attribute.
    /*!
     *  \returns |false| if there is no removable builtin attribute with the given key.
     */
    virtual bool RemoveBuiltinAttribute(TInternedAttributeKey key) = 0;

    //! Permission to set/remove non-builtin attributes.
    /*!
     *  \returns permission to set/remove non-builtin attributes.
     */
    virtual EPermission GetCustomAttributeModifyPermission();

    // Extension methods.

    //! Similar to its interface counterpart, but reserves the vector beforehand.
    void ReserveAndListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors);

    //! Similar to its interface counterpart, but populates a map rather than a vector.
    void ListSystemAttributes(std::map<TInternedAttributeKey, TAttributeDescriptor>* descriptors);

    //! Populates the list of all builtin attributes supported by this object.
    void ListBuiltinAttributes(std::vector<TAttributeDescriptor>* descriptors);

    //! Returns an instance of TAttributeDescriptor matching a given #key or null if no such
    //! builtin attribute is known.
    std::optional<TAttributeDescriptor> FindBuiltinAttributeDescriptor(TInternedAttributeKey key);

    //! Wraps #GetBuiltinAttribute and returns the YSON string instead
    //! of writing it into a consumer.
    NYson::TYsonString FindBuiltinAttribute(TInternedAttributeKey key);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
