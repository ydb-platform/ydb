#pragma once

#include "defs.h"
#include "scheme_tablecell.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/scheme/protos/key_range.pb.h>
#include <ydb/core/scheme_types/scheme_types.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

#include <util/generic/maybe.h>
#include <util/generic/map.h>

namespace NKikimr {

using TSchemaVersion = ui64;

enum class EColumnTypeConstraint {
    Nullable,
    NotNull,
};

// ident for table, must be unique in selected scope
// for global transactions ownerid is tabletid of owning schemeshard and tableid is counter designated by schemeshard
// SysViewInfo is not empty for system views attached to corresponding table

struct TTableId {
    TPathId PathId;
    TString SysViewInfo;
    TSchemaVersion SchemaVersion = 0;

    TTableId() = default;

    // raw ctors
    TTableId(ui64 ownerId, ui64 tableId, const TString& sysViewInfo, ui64 schemaVersion)
        : PathId(ownerId, tableId)
        , SysViewInfo(sysViewInfo)
        , SchemaVersion(schemaVersion)
    {}

    TTableId(ui64 ownerId, ui64 tableId, const TString& sysViewInfo)
        : TTableId(ownerId, tableId, sysViewInfo, 0)
    {}

    TTableId(ui64 ownerId, ui64 tableId, ui64 schemaVersion)
        : TTableId(ownerId, tableId, TString(), schemaVersion)
    {}

    TTableId(ui64 ownerId, ui64 tableId)
        : TTableId(ownerId, tableId, TString(), 0)
    {}

    // ctors from TPathId
    TTableId(const TPathId& pathId, const TString& sysViewInfo, ui64 schemaVersion)
        : TTableId(pathId.OwnerId, pathId.LocalPathId, sysViewInfo, schemaVersion)
    {}

    TTableId(const TPathId& pathId, const TString& sysViewInfo)
        : TTableId(pathId, sysViewInfo, 0)
    {}

    TTableId(const TPathId& pathId, ui64 schemaVersion)
        : TTableId(pathId, TString(), schemaVersion)
    {}

    TTableId(const TPathId& pathId)
        : TTableId(pathId, TString(), 0)
    {}

    explicit operator bool() const noexcept {
        return bool(PathId);
    }

    bool HasSamePath(const TTableId &x) const noexcept {
        return PathId == x.PathId && SysViewInfo == x.SysViewInfo;
    }

    bool IsSystemView() const noexcept {
        return !SysViewInfo.empty();
    }

    bool operator==(const TTableId& x) const {
        return PathId == x.PathId && SysViewInfo == x.SysViewInfo && SchemaVersion == x.SchemaVersion;
    }

    bool operator!=(const TTableId& x) const {
        return !operator==(x);
    }

    template<typename H>
    friend H AbslHashValue(H h, const TTableId& tableId) {
        return H::combine(std::move(h), tableId.PathId, tableId.SysViewInfo, tableId.SchemaVersion);
    }

    ui64 Hash() const noexcept {
        auto hash = PathId.Hash();
        if (SysViewInfo) {
            hash = CombineHashes(hash, THash<TString>()(SysViewInfo));
        }
        if (SchemaVersion) {
            hash = CombineHashes(hash, THash<TSchemaVersion>()(SchemaVersion));
        }

        return hash;
    }

    ui64 PathHash() const noexcept {
        auto hash = PathId.Hash();
        if (SysViewInfo) {
            hash = CombineHashes(hash, THash<TString>()(SysViewInfo));
        }

        return hash;
    }
};

struct TIndexId {
    TPathId PathId;
    TSchemaVersion SchemaVersion = 0;

    TIndexId(const TPathId& pathId, TSchemaVersion schemaVersion)
        : PathId(pathId)
        , SchemaVersion(schemaVersion)
    {}

    TIndexId(const TOwnerId ownerId, const TLocalPathId localPathId, TSchemaVersion schemaVersion)
        : PathId(ownerId, localPathId)
        , SchemaVersion(schemaVersion)
    {}
};

struct TTablePathHashFn {
    ui64 operator()(const NKikimr::TTableId &x) const noexcept {
        return x.PathHash();
    }
};

struct TTablePathEqualFn {
    bool operator()(const NKikimr::TTableId &x, const NKikimr::TTableId &y) const noexcept {
        return x.HasSamePath(y);
    }
};

template <typename T>
using TTablePathHashMap = THashMap<TTableId, T, TTablePathHashFn, TTablePathEqualFn>;

using TTablePathHashSet = THashSet<TTableId, TTablePathHashFn, TTablePathEqualFn>;

// defines key range as low and high borders (possibly partial - w/o defined key values on tail)
// missing values interpreted as infinity and mean "greater then any key with set prefix"
// column order must match table key order
// [{a, null}, {b, null}) => { (a, null), (b, null), inclusive = true, inclusive = false ) }
// ({a, inf}, {b, inf}) => { (a), (b), false, false) } ('inclusive' does not matter for infinity)
// [{a, 1}, {inf}) => { (a, 1), (), inclusive = true, inclusive = false) }
// (-inf, +inf) => { (null, null), (), inclusive = true, false) } ((null, null) is smallest defined key greater then -infinity, so we must include it in range
// [{a, 1}, {b, 3}] => { (a, 1), (b, 3), inclusive = true, inclusive = true) } (probably most used case w/o any corner cases
class TTableRange {
public:
    TConstArrayRef<TCell> From;
    TConstArrayRef<TCell> To;
    bool InclusiveFrom;
    bool InclusiveTo;
    bool Point;

    explicit TTableRange(TConstArrayRef<TCell> point)
        : From(point)
        , To()
        , InclusiveFrom(true)
        , InclusiveTo(true)
        , Point(true) {}

    TTableRange(TConstArrayRef<TCell> fromValues, bool inclusiveFrom, TConstArrayRef<TCell> toValues, bool inclusiveTo,
        bool point = false)
        : From(fromValues)
        , To(toValues)
        , InclusiveFrom(inclusiveFrom || point)
        , InclusiveTo(inclusiveTo || point)
        , Point(point)
    {
        if (Point) {
            Y_DEBUG_ABORT_UNLESS(toValues.empty() || fromValues.size() == toValues.size());
        }
    }

    bool IsEmptyRange(TConstArrayRef<const NScheme::TTypeInfo> types) const;
    bool IsFullRange(ui32 columnsCount) const;

    bool IsAmbiguous(size_t keyColumnsCount) const noexcept {
        return IsAmbiguousReason(keyColumnsCount) != nullptr;
    }

    const char* IsAmbiguousReason(size_t keyColumnsCount) const noexcept;
};

class TSerializedTableRange {
public:
    TSerializedCellVec From;
    TSerializedCellVec To;
    bool FromInclusive = false;
    bool ToInclusive = false;
    bool Point = false;

    TSerializedTableRange() {}

    TSerializedTableRange(const TString& from, const TString& to, bool fromInclusive, bool toInclusive)
        : From(from)
        , To(to)
        , FromInclusive(fromInclusive)
        , ToInclusive(toInclusive) {}

    TSerializedTableRange(TConstArrayRef<TCell> fromValues, bool inclusiveFrom, TConstArrayRef<TCell> toValues,
        bool inclusiveTo)
        : From(fromValues)
        , To(toValues)
        , FromInclusive(inclusiveFrom)
        , ToInclusive(inclusiveTo) {}


    explicit TSerializedTableRange(const TTableRange& range)
        : TSerializedTableRange(range.From, range.InclusiveFrom, range.To, range.InclusiveTo)
    {
        Point = range.Point;
    }

    explicit TSerializedTableRange(const NKikimrTx::TKeyRange& range) {
        Load(range);
    }

    TSerializedTableRange(const TSerializedTableRange &other) = default;
    TSerializedTableRange(TSerializedTableRange &&other) = default;
    TSerializedTableRange &operator=(const TSerializedTableRange &other) = default;
    TSerializedTableRange &operator=(TSerializedTableRange &&other) = default;

    void Load(const NKikimrTx::TKeyRange& range) {
        From.Parse(range.GetFrom());
        To.Parse(range.GetTo());
        FromInclusive = range.GetFromInclusive();
        ToInclusive = range.GetToInclusive();
    }

    void Serialize(NKikimrTx::TKeyRange& range) const {
        range.SetFrom(From.GetBuffer());
        range.SetFromInclusive(FromInclusive);
        if (Point) {
            Y_DEBUG_ABORT_UNLESS(FromInclusive);
            range.SetTo(From.GetBuffer());
            range.SetToInclusive(true);
        } else {
            range.SetTo(To.GetBuffer());
            range.SetToInclusive(ToInclusive);
        }
    }

    bool IsEmpty(TConstArrayRef<NScheme::TTypeInfo> types) const;

    TTableRange ToTableRange() const {
        return TTableRange(From.GetCells(), FromInclusive, To.GetCells(), ToInclusive, Point);
    }
};

template <typename T>
int ComparePointAndRange(const TConstArrayRef<TCell>& point, const TTableRange& range,
                         const T& pointTypes, const T& rangeTypes)
{
    Y_ABORT_UNLESS(!range.Point);
    Y_ABORT_UNLESS(rangeTypes.size() <= pointTypes.size());
    Y_ABORT_UNLESS(range.From.size() <= rangeTypes.size());
    Y_ABORT_UNLESS(range.To.size() <= rangeTypes.size());

    int cmpFrom = CompareTypedCellVectors(point.data(), range.From.data(), pointTypes.data(), range.From.size());
    if (!range.InclusiveFrom && cmpFrom == 0) {
        cmpFrom = -1;
        for (ui32 i = range.From.size(); i < point.size(); ++i) {
            if (!point[i].IsNull()) {
                cmpFrom = 1;
                break;
            }
        }
    }

    if (cmpFrom < 0)
        return -1;

    int cmpTo = CompareTypedCellVectors(point.data(), range.To.data(), pointTypes.data(), range.To.size());
    if (!range.InclusiveTo && cmpTo == 0) {
        cmpTo = 1;
        if (range.To.size() < point.size()) {
            cmpTo = -1;
        }
    }

    if (cmpTo > 0)
        return 1;
    return 0;
}

// Method used to compare range borders.
// Template args determine where range lies regarding compared border.
// E.g. CompareBorders<true, true>(...) compares borders of ranges lying on the left
// of compared borders (or in other words upper range borders are compared).
template<bool FirstLeft, bool SecondLeft>
int CompareBorders(TConstArrayRef<TCell> first, TConstArrayRef<TCell> second, bool inclusiveFirst, bool inclusiveSecond,
    TConstArrayRef<NScheme::TTypeInfo> cellTypes)
{
    const ui32 firstSize = first.size();
    const ui32 secondSize = second.size();

    for (ui32 idx = 0, keyColumns = cellTypes.size(); idx < keyColumns; ++idx) {
        const bool firstComplete = (firstSize == idx);
        const bool secondComplete = (secondSize == idx);

        if (firstComplete) {
            if (secondComplete)
                return 0;
            return 1;
        }

        if (secondComplete)
            return -1;

        if (first[idx].IsNull()) {
            if (!second[idx].IsNull())
                return -1;
        } else if (second[idx].IsNull()) {
            return 1;
        } else if (const int compares = CompareTypedCells(first[idx], second[idx], cellTypes[idx])) {
            return compares;
        }
    }

    if (FirstLeft && SecondLeft) {
        if (inclusiveFirst == inclusiveSecond)
            return 0;
        return (inclusiveFirst ? 1 : -1);
    } else if (FirstLeft && !SecondLeft) {
        if (inclusiveFirst && inclusiveSecond)
            return 0;
        return -1;
    } else if (!FirstLeft && SecondLeft) {
        if (inclusiveFirst && inclusiveSecond)
            return 0;
        return 1;
    } else { // !FirstLeft && !SecondLeft
        if (inclusiveFirst == inclusiveSecond)
            return 0;
        return (inclusiveFirst ? -1 : 1);
    }
}

/// @note returns 0 on any overlap
inline int CompareRanges(const TTableRange& rangeX, const TTableRange& rangeY,
                         const TConstArrayRef<NScheme::TTypeInfo> types)
{
    Y_ABORT_UNLESS(!rangeX.Point);
    Y_ABORT_UNLESS(!rangeY.Point);

    int xStart_yEnd = CompareBorders<true, false>(
        rangeX.From, rangeY.To, rangeX.InclusiveFrom, rangeY.InclusiveTo, types);
    if (xStart_yEnd > 0)
        return 1;

    int xEnd_yStart = CompareBorders<false, true>(
        rangeX.To, rangeY.From, rangeX.InclusiveTo, rangeY.InclusiveFrom, types);
    if (xEnd_yStart < 0)
        return -1;

    return 0; // overlapped
}

/// @note returns true on any overlap
inline bool CheckRangesOverlap(
        const TTableRange& rangeX,
        const TTableRange& rangeY,
        const TConstArrayRef<NScheme::TTypeInfo> typesX,
        const TConstArrayRef<NScheme::TTypeInfo> typesY)
{
    if (rangeX.Point && rangeY.Point) {
        // Works like ComparePointKeys
        return 0 == CompareTypedCellVectors(
            rangeX.From.data(),
            rangeY.From.data(),
            typesX.data(),
            typesX.size());
    } else if (rangeX.Point) {
        return 0 == ComparePointAndRange(rangeX.From, rangeY, typesX, typesY);
    } else if (rangeY.Point) {
        return 0 == ComparePointAndRange(rangeY.From, rangeX, typesY, typesX);
    } else {
        return 0 == CompareRanges(rangeX, rangeY, typesX);
    }
}

// TTableRange that owns its cell data and may be safely copied/moved
class TOwnedTableRange
    : public TTableRange
{
private:
    struct TInit {
        TTableRange Range;
        TOwnedCellVec FromKey;
        TOwnedCellVec ToKey;
    };

    TOwnedTableRange(TInit init)
        : TTableRange(std::move(init.Range))
        , FromKey_(std::move(init.FromKey))
        , ToKey_(std::move(init.ToKey))
    { }

    static TInit Allocate(TOwnedCellVec fromKey, bool inclusiveFrom,
                          TOwnedCellVec toKey, bool inclusiveTo,
                          bool point)
    {
        TTableRange range(fromKey, inclusiveFrom, toKey, inclusiveTo, point);
        return TInit{ std::move(range), std::move(fromKey), std::move(toKey) };
    }

    static TInit Allocate(TConstArrayRef<TCell> fromValues, bool inclusiveFrom,
                          TConstArrayRef<TCell> toValues, bool inclusiveTo,
                          bool point)
    {
        TOwnedCellVec fromKey(fromValues);
        bool sameKey = fromValues.data() == toValues.data() && fromValues.size() == toValues.size();
        TOwnedCellVec toKey = sameKey ? fromKey : TOwnedCellVec(toValues);
        return Allocate(std::move(fromKey), inclusiveFrom, std::move(toKey), inclusiveTo, point);
    }

    TTableRange& UnsafeTableRange() {
        return static_cast<TTableRange&>(*this);
    }

    void InvalidateTableRange() {
        From = { };
        To = { };
        InclusiveFrom = false;
        InclusiveTo = false;
        Point = false;
    }

public:
    TOwnedTableRange()
        : TTableRange({ }, false, { }, false, false)
    { }

    explicit TOwnedTableRange(TOwnedCellVec point)
        : TOwnedTableRange(Allocate(std::move(point), true, TOwnedCellVec(), true, true))
    { }

    explicit TOwnedTableRange(TConstArrayRef<TCell> point)
        : TOwnedTableRange(Allocate(TOwnedCellVec(point), true, TOwnedCellVec(), true, true))
    { }

    TOwnedTableRange(TOwnedCellVec fromKey, bool inclusiveFrom, TOwnedCellVec toKey, bool inclusiveTo, bool point = false)
        : TOwnedTableRange(Allocate(std::move(fromKey), inclusiveFrom, std::move(toKey), inclusiveTo, point))
    { }

    TOwnedTableRange(TConstArrayRef<TCell> fromValues, bool inclusiveFrom, TConstArrayRef<TCell> toValues, bool inclusiveTo, bool point = false)
        : TOwnedTableRange(Allocate(fromValues, inclusiveFrom, toValues, inclusiveTo, point))
    { }

    explicit TOwnedTableRange(const TTableRange& range)
        : TOwnedTableRange(Allocate(range.From, range.InclusiveFrom, range.To, range.InclusiveTo, range.Point))
    { }

    TOwnedTableRange(const TOwnedTableRange& rhs) noexcept
        : TTableRange(rhs)
        , FromKey_(rhs.FromKey_)
        , ToKey_(rhs.ToKey_)
    { }

    TOwnedTableRange(TOwnedTableRange&& rhs) noexcept
        : TTableRange(rhs)
        , FromKey_(std::move(rhs.FromKey_))
        , ToKey_(std::move(rhs.ToKey_))
    {
        rhs.InvalidateTableRange();
    }

    TOwnedTableRange& operator=(const TOwnedTableRange& rhs) noexcept {
        if (Y_LIKELY(this != &rhs)) {
            FromKey_ = rhs.FromKey_;
            ToKey_ = rhs.ToKey_;
            UnsafeTableRange() = rhs;
        }

        return *this;
    }

    TOwnedTableRange& operator=(TOwnedTableRange&& rhs) noexcept {
        if (Y_LIKELY(this != &rhs)) {
            FromKey_ = std::move(rhs.FromKey_);
            ToKey_ = std::move(rhs.ToKey_);
            UnsafeTableRange() = rhs;
            rhs.InvalidateTableRange();
        }

        return *this;
    }

public:
    const TOwnedCellVec& GetOwnedFrom() const noexcept {
        return FromKey_;
    }

    const TOwnedCellVec& GetOwnedTo() const noexcept {
        return ToKey_;
    }

private:
    TOwnedCellVec FromKey_;
    TOwnedCellVec ToKey_;
};

struct TReadTarget {
    enum class EMode {
        Online,
        Snapshot,
        Head,
        HeadAfterSnapshot,
        Follower,
        FollowerAfterSnapshot
    };

    TReadTarget()
        : Mode(EMode::Online)
    {}

    EMode GetMode() const {
        return Mode;
    }

    bool HasSnapshotTime() const {
        return Mode == EMode::Snapshot || Mode == EMode::HeadAfterSnapshot || Mode == EMode::FollowerAfterSnapshot;
    }

    std::pair<ui64, ui64> GetSnapshotTime() const {
        return SnapshotTime;
    }

    static TReadTarget Default() {
        return TReadTarget();
    }

    static TReadTarget Online() {
        return TReadTarget(EMode::Online);
    }

    static TReadTarget Snapshot(const std::pair<ui64, ui64>& snapshotTime) {
        return TReadTarget(EMode::Snapshot, snapshotTime);
    }

    static TReadTarget Head() {
        return TReadTarget(EMode::Head);
    }

    static TReadTarget HeadAfterSnapshot(const std::pair<ui64, ui64>& snapshotTime) {
        return TReadTarget(EMode::HeadAfterSnapshot, snapshotTime);
    }

    static TReadTarget Follower() {
        return TReadTarget(EMode::Follower);
    }

    static TReadTarget FollowerAfterSnapshot(const std::pair<ui64, ui64>& snapshotTime) {
        return TReadTarget(EMode::FollowerAfterSnapshot, snapshotTime);
    }

private:
    TReadTarget(EMode mode, const std::pair<ui64, ui64>& snapshotTime = std::pair<ui64, ui64>())
        : Mode(mode)
        , SnapshotTime(snapshotTime)
    {}

private:
    EMode Mode;
    std::pair<ui64, ui64> SnapshotTime;
};

struct TSecurityObject : TAtomicRefCount<TSecurityObject>, NACLib::TSecurityObject {
    using TPtr = TIntrusivePtr<TSecurityObject>;

    static NACLib::TSecurityObject FromByteStream(const NACLibProto::TSecurityObject* parent, const TString& owner, const TString& acl, bool isContainer) {
        NACLib::TSecurityObject object(owner, isContainer);
        Y_ABORT_UNLESS(object.MutableACL()->ParseFromString(acl));
        return parent != nullptr ? object.MergeWithParent(*parent) : object;
    }

    TSecurityObject(const TString& owner, const TString& acl, bool isContainer)
        : NACLib::TSecurityObject(FromByteStream(nullptr, owner, acl, isContainer))
    {}

    TSecurityObject(const TSecurityObject* parent, const TString& owner, const TString& acl, bool isContainer)
        : NACLib::TSecurityObject(FromByteStream(parent, owner, acl, isContainer))
    {}
};

// key description of one minikql operation
class TKeyDesc : TNonCopyable {
public:

    // what we do on row?
    enum struct ERowOperation {
        Unknown = 1,
        Read = 2,
        Update = 3,
        Erase = 4,
    };

    // what we do on column?
    enum struct EColumnOperation {
        Read = 1,
        Set = 2, // == erase
        InplaceUpdate = 3,
    };

    enum ESystemColumnIds : ui32 {
        EColumnIdInvalid = Max<ui32>(),
        EColumnIdSystemStart = EColumnIdInvalid - 1000,
        EColumnIdDataShard = EColumnIdSystemStart + 1
    };

#define SCHEME_KEY_DESCRIPTION_STATUS_MAP(XX) \
    XX(Unknown, 0) \
    XX(Ok, 1) \
    XX(TypeCheckFailed, 2) \
    XX(OperationNotSupported, 3) \
    XX(NotExists, 4) \
    XX(SnapshotNotExist, 5) \
    XX(SnapshotNotReady, 6)

    enum class EStatus {
        SCHEME_KEY_DESCRIPTION_STATUS_MAP(ENUM_VALUE_GEN)
    };

    static void Out(IOutputStream& out, EStatus x);

    // one column operation (in)
    struct TColumnOp {
        ui32 Column;
        EColumnOperation Operation;
        NScheme::TTypeInfo ExpectedType;
        ui32 InplaceUpdateMode;
        ui32 ImmediateUpdateSize;
    };

    // one column info (out)
    struct TColumnInfo {
        ui32 Column;
        NScheme::TTypeInfo Type;
        ui32 AllowInplaceMode;
        EStatus Status;
    };

    struct TRangeLimits {
        ui64 ItemsLimit;
        ui64 BytesLimit;

        TRangeLimits(ui64 itemsLimit = 0, ui64 bytesLimit = 0)
            : ItemsLimit(itemsLimit)
            , BytesLimit(bytesLimit)
        {}
    };

    struct TPartitionRangeInfo {
        TSerializedCellVec EndKeyPrefix;
        bool IsInclusive = false;
        bool IsPoint = false;
    };

    struct TPartitionInfo {
        TPartitionInfo() {}

        TPartitionInfo(ui64 shardId)
            : ShardId(shardId) {}

        ui64 ShardId = 0;
        TMaybe<TPartitionRangeInfo> Range;
    };

    // in
    const TTableId TableId;
    const TOwnedTableRange Range;
    const TRangeLimits RangeLimits;
    const ERowOperation RowOperation;
    const TVector<NScheme::TTypeInfo> KeyColumnTypes; // For SelectRange there can be not full key
    const TVector<TColumnOp> Columns;
    const bool Reverse;
    TReadTarget ReadTarget; // Set for Read row operation

    // out
    EStatus Status;
    TVector<TColumnInfo> ColumnInfos;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    TIntrusivePtr<TSecurityObject> SecurityObject;

    const TVector<TKeyDesc::TPartitionInfo>& GetPartitions() const { Y_ABORT_UNLESS(Partitioning); return *Partitioning; }
    bool IsSystemView() const { return GetPartitions().empty(); }

    template<typename TKeyColumnTypes, typename TColumns>
    TKeyDesc(const TTableId& tableId, const TTableRange& range, ERowOperation rowOperation,
            const TKeyColumnTypes &keyColumnTypes, const TColumns &columns,
            ui64 itemsLimit = 0, ui64 bytesLimit = 0, bool reverse = false)
        : TableId(tableId)
        , Range(range.From, range.InclusiveFrom, range.To, range.InclusiveTo, range.Point)
        , RangeLimits(itemsLimit, bytesLimit)
        , RowOperation(rowOperation)
        , KeyColumnTypes(keyColumnTypes.begin(), keyColumnTypes.end())
        , Columns(columns.begin(), columns.end())
        , Reverse(reverse)
        , Status(EStatus::Unknown)
        , Partitioning(std::make_shared<TVector<TKeyDesc::TPartitionInfo>>())
    {}

    static THolder<TKeyDesc> CreateMiniKeyDesc(const TVector<NScheme::TTypeInfo> &keyColumnTypes) {
        return THolder<TKeyDesc>(new TKeyDesc(keyColumnTypes));
    }
private:
    TKeyDesc(const TVector<NScheme::TTypeInfo> &keyColumnTypes)
        : RowOperation(ERowOperation::Unknown)
        , KeyColumnTypes(keyColumnTypes.begin(), keyColumnTypes.end())
        , Reverse(false)
        , Status(EStatus::Unknown)
        , Partitioning(std::make_shared<TVector<TKeyDesc::TPartitionInfo>>())
    {}
};

struct TSystemColumnInfo {
    TKeyDesc::ESystemColumnIds ColumnId;
    NKikimr::NScheme::TTypeId TypeId;
};

const TMap<TString, TSystemColumnInfo>& GetSystemColumns();

bool IsSystemColumn(ui32 columnId);
bool IsSystemColumn(const TStringBuf columnName);

inline int ComparePointKeys(const TKeyDesc& point1, const TKeyDesc& point2) {
    Y_ABORT_UNLESS(point1.Range.Point);
    Y_ABORT_UNLESS(point2.Range.Point);
    return CompareTypedCellVectors(
        point1.Range.From.data(), point2.Range.From.data(), point1.KeyColumnTypes.data(), point1.KeyColumnTypes.size());
}

inline int ComparePointAndRangeKeys(const TKeyDesc& point, const TKeyDesc& range) {
    Y_ABORT_UNLESS(point.Range.Point);
    return ComparePointAndRange(point.Range.From, range.Range, point.KeyColumnTypes, range.KeyColumnTypes);
}

inline int CompareRangeKeys(const TKeyDesc& rangeX, const TKeyDesc& rangeY) {
    return CompareRanges(rangeX.Range, rangeY.Range, rangeX.KeyColumnTypes);
}

} // namespace NKikimr

template<>
struct THash<NKikimr::TTableId> {
    inline ui64 operator()(const NKikimr::TTableId& x) const noexcept {
        return x.Hash();
    }
};

template<>
inline void Out<NKikimr::TTableId>(IOutputStream& o, const NKikimr::TTableId& x) {
    o << '[' << x.PathId.OwnerId << ':' << x.PathId.LocalPathId << ':' << x.SchemaVersion;
    if (x.SysViewInfo) {
        o << ":" << x.SysViewInfo;
    }
    o << ']';
}

template<>
inline void Out<NKikimr::TIndexId>(IOutputStream& o, const NKikimr::TIndexId& x) {
    o << '[' << x.PathId.OwnerId << ':' << x.PathId.LocalPathId << ':' << x.SchemaVersion << ']';
}
