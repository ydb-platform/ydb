#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_info.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NChangeExchange {

// Compact CDC record. Little-endian, no padding.
//
//   u8  version = 1
//   u8  kind    = 1 data, 2 heartbeat, 3 schema
//   u8  op      = 0 none, 1 upsert, 2 reset, 3 erase
//   u8  flags
//   u16 keyCount
//   u16 valueCount
//   [u16 oldImageCount]  if HasOldImage
//   [u16 newImageCount]  if HasNewImage
//   [u64 step][u64 txId] if HasTs
//   key cells, then value cells, then old image, then new image
//   [u16 len][bytes] user SID  if HasUser
//   [u16 len][bytes] trace id  if HasTrace
//
// A fixed-width cell is its raw bytes. A variable cell is u32 length plus raw bytes.
// NULL on an indexed value or image cell sets the high bit of the column index and
// omits the cell body. Key cells are positional, in primary-key order. When any key
// cell is NULL the record sets NullKeys, and every key cell is preceded by one byte:
// 0 means a value follows, 1 means NULL and no value bytes follow. Without NullKeys
// every key cell is a value, so a record with no NULL key is unchanged.
// Value indexes are positions of non-key columns sorted by name at the schema version
// that produced the record. Index 0 is one byte while every index in the record is
// below 128; otherwise the record sets WideIndex and each index is u16.
//
// A schema record carries the same JSON snapshot as FORMAT JSON, after a u32 length.
// Async replication reads that snapshot with the existing schema-change parser.
// A heartbeat is the header plus step and txId.

inline constexpr ui8 BinaryCdcVersion = 1;

enum class EBinaryCdcKind : ui8 {
    Data = 1,
    Heartbeat = 2,
    Schema = 3,
};

enum class EBinaryCdcOp : ui8 {
    None = 0,
    Upsert = 1,
    Reset = 2,
    Erase = 3,
};

enum class EBinaryCdcFlag : ui8 {
    HasTs = 1 << 0,
    WideIndex = 1 << 1,
    HasOldImage = 1 << 2,
    HasNewImage = 1 << 3,
    HasUser = 1 << 4,
    HasTrace = 1 << 5,
    // Each key cell is preceded by a 0/1 null marker.
    NullKeys = 1 << 6,
};

inline constexpr ui8 BinaryCdcKnownFlags =
    ui8(EBinaryCdcFlag::HasTs)
    | ui8(EBinaryCdcFlag::WideIndex)
    | ui8(EBinaryCdcFlag::HasOldImage)
    | ui8(EBinaryCdcFlag::HasNewImage)
    | ui8(EBinaryCdcFlag::HasUser)
    | ui8(EBinaryCdcFlag::HasTrace)
    | ui8(EBinaryCdcFlag::NullKeys);

struct TBinaryCdcHeader {
    ui8 Version = 0;
    EBinaryCdcKind Kind = EBinaryCdcKind::Data;
    EBinaryCdcOp Op = EBinaryCdcOp::None;
    ui8 Flags = 0;
    ui16 KeyCount = 0;
    ui16 ValueCount = 0;
    ui16 OldImageCount = 0;
    ui16 NewImageCount = 0;
    ui64 Step = 0;
    ui64 TxId = 0;

    bool Has(EBinaryCdcFlag flag) const {
        return (Flags & ui8(flag)) != 0;
    }
};

class TBinaryCdcWriter {
public:
    void Start(const TBinaryCdcHeader& header);
    void AddKey(const TCell& cell, NScheme::TTypeInfo type);
    void AddValue(ui16 index, const TCell& cell, NScheme::TTypeInfo type);
    void AddOldImage(ui16 index, const TCell& cell, NScheme::TTypeInfo type);
    void AddNewImage(ui16 index, const TCell& cell, NScheme::TTypeInfo type);
    void SetUser(TStringBuf user);
    void SetTrace(TStringBuf trace);
    void SetSchemaJson(TStringBuf json);
    TString Finish();

private:
    void AddIndexed(ui16 index, const TCell& cell, NScheme::TTypeInfo type);
    void WriteCell(const TCell& cell, NScheme::TTypeInfo type);

    TBinaryCdcHeader Header_;
    TString Out_;
    ui16 Keys_ = 0;
    ui16 Values_ = 0;
    ui16 Old_ = 0;
    ui16 New_ = 0;
    bool Started_ = false;
    bool Finished_ = false;
};

class TBinaryCdcReader {
public:
    bool Parse(TStringBuf body, TString& error);

    const TBinaryCdcHeader& Header() const {
        return Header_;
    }

    TStringBuf SchemaJson() const {
        return SchemaJson_;
    }

    bool ReadKeys(TArrayRef<const NScheme::TTypeInfo> types, TVector<TCell>& cells, TString& error);
    bool ReadIndexed(ui16 count, TArrayRef<const NScheme::TTypeInfo> typeByIndex,
        TVector<ui16>& indexes, TVector<TCell>& cells, TString& error);

    TStringBuf Rest() const {
        return Rest_;
    }

private:
    bool ReadCell(NScheme::TTypeInfo type, bool isNull, TCell& cell, TString& error);
    bool ReadIndex(ui16& index, bool& isNull, TString& error);

    TBinaryCdcHeader Header_;
    TStringBuf Rest_;
    TStringBuf SchemaJson_;
    bool Parsed_ = false;
};

}
