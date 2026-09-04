#pragma once

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/logging/tag.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Persists a tag list as its serialized payload.
struct TLoggingTagListSerializer
{
    template <class C>
    static void Save(C& context, const NLogging::TLoggingTagList& tags);

    template <class C>
    static void Load(C& context, NLogging::TLoggingTagList& tags);
};

template <class C>
struct TSerializerTraits<NLogging::TLoggingTagList, C, void>
{
    using TSerializer = TLoggingTagListSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SERIALIZABLE_TAG_INL_H_
#include "serializable_tag-inl.h"
#undef SERIALIZABLE_TAG_INL_H_
