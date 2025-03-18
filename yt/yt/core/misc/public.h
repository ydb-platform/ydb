#pragma once

#include "common.h"

#include <library/cpp/yt/error/public.h>

// Google Protobuf forward declarations.
namespace google::protobuf {

////////////////////////////////////////////////////////////////////////////////

class Descriptor;
class EnumDescriptor;
class MessageLite;
class Message;

template <class Element>
class RepeatedField;
template <class Element>
class RepeatedPtrField;

class Timestamp;

namespace io {

class ZeroCopyInputStream;
class ZeroCopyOutputStream;

} // namespace io

////////////////////////////////////////////////////////////////////////////////

} // namespace google::protobuf

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TError;
class TBloomFilter;
class TDataStatistics;
class TExtensionSet;

} // namespace NProto

struct TExponentialBackoffOptions;
struct TConstantBackoffOptions;

class TBackoffStrategy;

struct TGuid;

template <class T>
class TErrorOr;

using TError = TErrorOr<void>;

template <class T>
struct TErrorTraits;

class TStreamLoadContext;
class TStreamSaveContext;

struct TEntitySerializationContext;
class TEntityStreamSaveContext;
class TEntityStreamLoadContext;

template <class TSaveContext, class TLoadContext, class TSnapshotVersion = int>
class TCustomPersistenceContext;

using TStreamPersistenceContext = TCustomPersistenceContext<
    TStreamSaveContext,
    TStreamLoadContext
>;

struct TValueBoundComparer;
struct TValueBoundSerializer;

template <class T, class C, class = void>
struct TSerializerTraits;

DEFINE_ENUM(ESerializationDumpMode,
    (None)
    (Content)
    (Checksum)
);

template <class TKey, class TComparer>
class TSkipList;

class TBlobOutput;

class TStringBuilderBase;
class TStringBuilder;

DECLARE_REFCOUNTED_STRUCT(IDigest)
DECLARE_REFCOUNTED_STRUCT(IPersistentDigest)

DECLARE_REFCOUNTED_STRUCT(TSlruCacheDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TSlruCacheConfig)

DECLARE_REFCOUNTED_STRUCT(TAsyncExpiringCacheDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TAsyncExpiringCacheConfig)

DECLARE_REFCOUNTED_STRUCT(TLogDigestConfig)
DECLARE_REFCOUNTED_STRUCT(THistogramDigestConfig)

DECLARE_REFCOUNTED_STRUCT(TSingletonsConfig)
DECLARE_REFCOUNTED_STRUCT(TSingletonsDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(TFairShareHierarchicalSchedulerDynamicConfig)

class TSignalRegistry;

class TBloomFilterBuilder;
class TBloomFilter;

using TChecksum = ui64;

constexpr TChecksum NullChecksum = 0;

template <class T, size_t N>
class TCompactVector;

template <class TProto>
class TRefCountedProto;

DECLARE_REFCOUNTED_CLASS(TProcessBase)

const ui32 YTCoreNoteType = 0x5f59545f; // = hex("_YT_") ;)
extern const TString YTCoreNoteName;

template <class T>
class TInternRegistry;

template <class T>
using TInternRegistryPtr = TIntrusivePtr<TInternRegistry<T>>;

template <class T>
class TInternedObjectData;

template <class T>
using TInternedObjectDataPtr = TIntrusivePtr<TInternedObjectData<T>>;

template <class T>
class TInternedObject;

namespace NStatisticPath {

class TStatisticPathLiteral;
class TStatisticPath;
struct TStatisticPathSerializer;

} // namespace NStatisticPath

class TStatistics;
class TSummary;

template <class TTask>
struct IFairScheduler;

template <class TTask>
using IFairSchedulerPtr = TIntrusivePtr<IFairScheduler<TTask>>;

template <typename TTag>
class TFairShareHierarchicalSlotQueueSlot;

template <typename TTag>
using TFairShareHierarchicalSlotQueueSlotPtr = TIntrusivePtr<TFairShareHierarchicalSlotQueueSlot<TTag>>;

template <typename TTag>
class TFairShareHierarchicalSchedulerLog;

template <typename TTag>
using TFairShareHierarchicalSchedulerLogPtr = TIntrusivePtr<TFairShareHierarchicalSchedulerLog<TTag>>;

template <typename TTag>
class TFairShareHierarchicalScheduler;

template <typename TTag>
using TFairShareHierarchicalSchedulerPtr = TIntrusivePtr<TFairShareHierarchicalScheduler<TTag>>;

template <typename TTag>
class TFairShareHierarchicalSlotQueue;

template <typename TTag>
using TFairShareHierarchicalSlotQueuePtr = TIntrusivePtr<TFairShareHierarchicalSlotQueue<TTag>>;

DECLARE_REFCOUNTED_STRUCT(TAdaptiveHedgingManagerConfig)
DECLARE_REFCOUNTED_STRUCT(IHedgingManager)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProcessErrorCode,
    ((NonZeroExitCode)    (10000))
    ((Signal)             (10001))
    ((CannotResolveBinary)(10002))
    ((CannotStartProcess) (10003))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
