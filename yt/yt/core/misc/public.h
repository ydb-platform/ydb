#pragma once

#include "common.h"
#include "error_code.h"

#include <library/cpp/yt/misc/concepts.h>

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

template <class TKey, class TComparer>
class TSkipList;

class TBlobOutput;

class TStringBuilderBase;
class TStringBuilder;

DECLARE_REFCOUNTED_STRUCT(IDigest)
DECLARE_REFCOUNTED_STRUCT(IPersistentDigest)

DECLARE_REFCOUNTED_CLASS(TSlruCacheDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TSlruCacheConfig)

DECLARE_REFCOUNTED_CLASS(TAsyncExpiringCacheDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TAsyncExpiringCacheConfig)

DECLARE_REFCOUNTED_CLASS(TLogDigestConfig)
DECLARE_REFCOUNTED_CLASS(THistogramDigestConfig)

class TSignalRegistry;

class TBloomFilterBuilder;
class TBloomFilter;

using TChecksum = ui64;

constexpr TChecksum NullChecksum = 0;

template <class T, size_t N>
class TCompactVector;

class TRef;
class TMutableRef;

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

DECLARE_REFCOUNTED_CLASS(TAdaptiveHedgingManagerConfig)
DECLARE_REFCOUNTED_STRUCT(IHedgingManager)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((OK)                    (0))
    ((Generic)               (1))
    ((Canceled)              (2))
    ((Timeout)               (3))
    ((FutureCombinerFailure) (4))
    ((FutureCombinerShortcut)(5))
);

DEFINE_ENUM(EProcessErrorCode,
    ((NonZeroExitCode)    (10000))
    ((Signal)             (10001))
    ((CannotResolveBinary)(10002))
    ((CannotStartProcess) (10003))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IMemoryUsageTracker)
DECLARE_REFCOUNTED_STRUCT(IReservingMemoryUsageTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
