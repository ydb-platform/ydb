#pragma once

#include "public.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt_proto/yt/core/ytree/proto/attributes.pb.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

// NB: Pretty slow.
bool operator == (const IAttributeDictionary& lhs, const IAttributeDictionary& rhs);

//! Creates attributes dictionary in memory.
IAttributeDictionaryPtr CreateEphemeralAttributes(std::optional<int> ysonNestingLevelLimit = std::nullopt);

//! Wraps an arbitrary implementation of IAttributeDictionary and turns it into a thread-safe one.
//! Internally uses a read-write spinlock to protect the underlying instance from concurrent access.
IAttributeDictionaryPtr CreateThreadSafeAttributes(IAttributeDictionary* underlying);

//! Creates empty attributes dictionary with deprecated method Set.
const IAttributeDictionary& EmptyAttributes();

//! Serialize attributes to consumer. Used in ConvertTo* functions.
void Serialize(const IAttributeDictionary& attributes, NYson::IYsonConsumer* consumer);

//! Protobuf conversion methods.
void ToProto(NProto::TAttributeDictionary* protoAttributes, const IAttributeDictionary& attributes);
IAttributeDictionaryPtr FromProto(const NProto::TAttributeDictionary& protoAttributes);

//! By-ptr binary serializer.
//! Supports TIntrusivePtr only.
struct TAttributeDictionarySerializer
{
    static void Save(TStreamSaveContext& context, const IAttributeDictionaryPtr& attributes);
    static void SaveNonNull(TStreamSaveContext& context, const IAttributeDictionaryPtr& attributes);
    static void Load(TStreamLoadContext& context, IAttributeDictionaryPtr& attributes);
    static void LoadNonNull(TStreamLoadContext& context, const IAttributeDictionaryPtr& attributes);
};

////////////////////////////////////////////////////////////////////////////////

void ValidateYTreeKey(TStringBuf key);

void ValidateYPathResolutionDepth(TYPathBuf path, int depth);

//! Helps implementing IAttributeDictionary::ListPairs by delegating to
//! IAttributeDictionary::ListKeys and IAttributeDictionary::FindYson for those not capable
//! of providing a custom efficient implementation.
std::vector<std::pair<TString, NYson::TYsonString>> ListAttributesPairs(const IAttributeDictionary& attributes);


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct TSerializerTraits<NYTree::IAttributeDictionaryPtr, C, void>
{
    using TSerializer = NYTree::TAttributeDictionarySerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
