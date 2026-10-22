#include "binary_cdc.h"

#include <ydb/core/scheme/scheme_types_defs.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <bit>
#include <cstring>

namespace NKikimr::NChangeExchange {

static_assert(std::endian::native == std::endian::little,
    "binary CDC stores multi-byte integers in native order and requires little-endian");

namespace {

ui32 CellFixedSize(const NScheme::TTypeInfo& typeInfo) {
    using namespace NScheme;
    switch (typeInfo.GetTypeId()) {
#define KIKIMR_TYPE_MACRO(typeEnum, typeType, ...) case NTypeIds::typeEnum: return typeType::GetFixedSize();
    KIKIMR_FOREACH_TYPE(KIKIMR_TYPE_MACRO)
#undef KIKIMR_TYPE_MACRO
    default:
        // Pg and other parameterized types are length-prefixed. Writer and reader share this function.
        return 0;
    }
}

void AppendPod(TString& out, const void* data, size_t size) {
    out.append(static_cast<const char*>(data), size);
}

template <typename T>
void Append(TString& out, T value) {
    AppendPod(out, &value, sizeof(value));
}

template <typename T>
bool Take(TStringBuf& in, T& value, TString& error) {
    if (in.size() < sizeof(T)) {
        error = "truncated binary CDC record";
        return false;
    }

    std::memcpy(&value, in.data(), sizeof(value));
    in.Skip(sizeof(T));
    return true;
}

} // namespace

void TBinaryCdcWriter::Start(const TBinaryCdcHeader& header) {
    Y_ENSURE(header.Version == BinaryCdcVersion);
    Y_ENSURE((header.Flags & ~BinaryCdcKnownFlags) == 0);
    if (header.Has(EBinaryCdcFlag::NullKeys)) {
        Y_ENSURE(header.Kind == EBinaryCdcKind::Data && header.KeyCount > 0,
            "binary CDC null key flag requires key cells");
    }
    Header_ = header;
    Out_.clear();
    Out_.reserve(64);
    Append<ui8>(Out_, header.Version);
    Append<ui8>(Out_, static_cast<ui8>(header.Kind));
    Append<ui8>(Out_, static_cast<ui8>(header.Op));
    Append<ui8>(Out_, header.Flags);
    Append<ui16>(Out_, header.KeyCount);
    Append<ui16>(Out_, header.ValueCount);
    if (header.Has(EBinaryCdcFlag::HasOldImage)) {
        Append<ui16>(Out_, header.OldImageCount);
    }
    if (header.Has(EBinaryCdcFlag::HasNewImage)) {
        Append<ui16>(Out_, header.NewImageCount);
    }
    if (header.Has(EBinaryCdcFlag::HasTs)) {
        Append<ui64>(Out_, header.Step);
        Append<ui64>(Out_, header.TxId);
    }
    Started_ = true;
}

void TBinaryCdcWriter::WriteCell(const TCell& cell, NScheme::TTypeInfo type) {
    Y_ENSURE(!cell.IsNull(), "binary CDC key and non-null value cells cannot be null");
    const ui32 fixed = CellFixedSize(type);
    if (fixed) {
        Y_ENSURE(cell.Size() == fixed, "binary CDC cell size " << cell.Size()
            << " does not match fixed type size " << fixed);
        AppendPod(Out_, cell.Data(), cell.Size());
        return;
    }

    const ui32 size = cell.Size();
    Append<ui32>(Out_, size);
    if (size) {
        AppendPod(Out_, cell.Data(), size);
    }
}

void TBinaryCdcWriter::AddKey(const TCell& cell, NScheme::TTypeInfo type) {
    Y_ENSURE(Started_ && !Finished_);
    Y_ENSURE(Header_.Kind == EBinaryCdcKind::Data);
    if (Header_.Has(EBinaryCdcFlag::NullKeys)) {
        const ui8 marker = cell.IsNull() ? 1 : 0;
        Append<ui8>(Out_, marker);
        if (!cell.IsNull()) {
            WriteCell(cell, type);
        }
    } else {
        WriteCell(cell, type);
    }
    ++Keys_;
}

void TBinaryCdcWriter::AddIndexed(ui16 index, const TCell& cell, NScheme::TTypeInfo type) {
    Y_ENSURE(Started_ && !Finished_);
    const bool isNull = cell.IsNull();
    if (Header_.Has(EBinaryCdcFlag::WideIndex)) {
        Y_ENSURE(index < 0x8000, "binary CDC column index does not fit into 15 bits");
        const ui16 wire = index | (isNull ? 0x8000 : 0);
        Append<ui16>(Out_, wire);
    } else {
        Y_ENSURE(index < 0x80, "binary CDC column index requires WideIndex");
        const ui8 wire = static_cast<ui8>(index) | (isNull ? 0x80 : 0);
        Append<ui8>(Out_, wire);
    }
    if (!isNull) {
        WriteCell(cell, type);
    }
}

void TBinaryCdcWriter::AddValue(ui16 index, const TCell& cell, NScheme::TTypeInfo type) {
    Y_ENSURE(Header_.Kind == EBinaryCdcKind::Data);
    AddIndexed(index, cell, type);
    ++Values_;
}

void TBinaryCdcWriter::AddOldImage(ui16 index, const TCell& cell, NScheme::TTypeInfo type) {
    Y_ENSURE(Header_.Has(EBinaryCdcFlag::HasOldImage));
    AddIndexed(index, cell, type);
    ++Old_;
}

void TBinaryCdcWriter::AddNewImage(ui16 index, const TCell& cell, NScheme::TTypeInfo type) {
    Y_ENSURE(Header_.Has(EBinaryCdcFlag::HasNewImage));
    AddIndexed(index, cell, type);
    ++New_;
}

void TBinaryCdcWriter::SetUser(TStringBuf user) {
    Y_ENSURE(Header_.Has(EBinaryCdcFlag::HasUser));
    Y_ENSURE(user.size() <= Max<ui16>());
    Append<ui16>(Out_, static_cast<ui16>(user.size()));
    AppendPod(Out_, user.data(), user.size());
}

void TBinaryCdcWriter::SetTrace(TStringBuf trace) {
    Y_ENSURE(Header_.Has(EBinaryCdcFlag::HasTrace));
    Y_ENSURE(trace.size() <= Max<ui16>());
    Append<ui16>(Out_, static_cast<ui16>(trace.size()));
    AppendPod(Out_, trace.data(), trace.size());
}

void TBinaryCdcWriter::SetSchemaJson(TStringBuf json) {
    Y_ENSURE(Started_ && !Finished_);
    Y_ENSURE(Header_.Kind == EBinaryCdcKind::Schema);
    Y_ENSURE(Header_.KeyCount == 0 && Header_.ValueCount == 0);
    Append<ui32>(Out_, static_cast<ui32>(json.size()));
    AppendPod(Out_, json.data(), json.size());
}

TString TBinaryCdcWriter::Finish() {
    Y_ENSURE(Started_ && !Finished_);
    Y_ENSURE(Keys_ == Header_.KeyCount);
    Y_ENSURE(Values_ == Header_.ValueCount);
    Y_ENSURE(Old_ == Header_.OldImageCount);
    Y_ENSURE(New_ == Header_.NewImageCount);
    Finished_ = true;
    return Out_;
}

bool TBinaryCdcReader::Parse(TStringBuf body, TString& error) {
    Parsed_ = false;
    Header_ = {};
    SchemaJson_ = {};
    Rest_ = {};

    ui8 version = 0;
    ui8 kind = 0;
    ui8 op = 0;
    if (!Take(body, version, error) || !Take(body, kind, error) || !Take(body, op, error)
        || !Take(body, Header_.Flags, error)
        || !Take(body, Header_.KeyCount, error) || !Take(body, Header_.ValueCount, error))
    {
        return false;
    }

    if (version != BinaryCdcVersion) {
        error = TStringBuilder() << "unsupported binary CDC version: " << static_cast<unsigned>(version);
        return false;
    }
    if ((Header_.Flags & ~BinaryCdcKnownFlags) != 0) {
        error = "unknown binary CDC flags";
        return false;
    }

    switch (kind) {
    case static_cast<ui8>(EBinaryCdcKind::Data):
    case static_cast<ui8>(EBinaryCdcKind::Heartbeat):
    case static_cast<ui8>(EBinaryCdcKind::Schema):
        Header_.Kind = static_cast<EBinaryCdcKind>(kind);
        break;
    default:
        error = TStringBuilder() << "unknown binary CDC kind: " << static_cast<unsigned>(kind);
        return false;
    }

    switch (op) {
    case static_cast<ui8>(EBinaryCdcOp::None):
    case static_cast<ui8>(EBinaryCdcOp::Upsert):
    case static_cast<ui8>(EBinaryCdcOp::Reset):
    case static_cast<ui8>(EBinaryCdcOp::Erase):
        Header_.Op = static_cast<EBinaryCdcOp>(op);
        break;
    default:
        error = TStringBuilder() << "unknown binary CDC operation: " << static_cast<unsigned>(op);
        return false;
    }

    if (Header_.Has(EBinaryCdcFlag::NullKeys)
        && (Header_.Kind != EBinaryCdcKind::Data || Header_.KeyCount == 0))
    {
        error = "binary CDC null key flag requires key cells";
        return false;
    }

    if (Header_.Has(EBinaryCdcFlag::HasOldImage) && !Take(body, Header_.OldImageCount, error)) {
        return false;
    }
    if (Header_.Has(EBinaryCdcFlag::HasNewImage) && !Take(body, Header_.NewImageCount, error)) {
        return false;
    }
    if (Header_.Has(EBinaryCdcFlag::HasTs)
        && (!Take(body, Header_.Step, error) || !Take(body, Header_.TxId, error)))
    {
        return false;
    }

    Header_.Version = version;
    if (Header_.Kind == EBinaryCdcKind::Schema) {
        ui32 size = 0;
        if (!Take(body, size, error)) {
            return false;
        }
        if (body.size() < size) {
            error = "truncated binary CDC schema record";
            return false;
        }
        SchemaJson_ = body.Head(size);
        body.Skip(size);
        if (body) {
            error = "trailing bytes after binary CDC schema record";
            return false;
        }
    }

    Rest_ = body;
    Parsed_ = true;
    return true;
}

bool TBinaryCdcReader::ReadIndex(ui16& index, bool& isNull, TString& error) {
    if (Header_.Has(EBinaryCdcFlag::WideIndex)) {
        ui16 wire = 0;
        if (!Take(Rest_, wire, error)) {
            return false;
        }
        isNull = (wire & 0x8000) != 0;
        index = wire & 0x7fff;
        return true;
    }

    ui8 wire = 0;
    if (!Take(Rest_, wire, error)) {
        return false;
    }
    isNull = (wire & 0x80) != 0;
    index = wire & 0x7f;
    return true;
}

bool TBinaryCdcReader::ReadCell(NScheme::TTypeInfo type, bool isNull, TCell& cell, TString& error) {
    if (isNull) {
        cell = TCell();
        return true;
    }

    const ui32 fixed = CellFixedSize(type);
    if (fixed) {
        if (Rest_.size() < fixed) {
            error = "truncated binary CDC cell";
            return false;
        }
        cell = TCell(Rest_.data(), fixed);
        Rest_.Skip(fixed);
        return true;
    }

    ui32 size = 0;
    if (!Take(Rest_, size, error)) {
        return false;
    }
    if (Rest_.size() < size) {
        error = "truncated binary CDC cell";
        return false;
    }
    if (size == 0) {
        static const char empty = 0;
        cell = TCell(&empty, 0);
    } else {
        cell = TCell(Rest_.data(), size);
    }
    Rest_.Skip(size);
    return true;
}

bool TBinaryCdcReader::ReadKeys(TArrayRef<const NScheme::TTypeInfo> types, TVector<TCell>& cells, TString& error) {
    if (!Parsed_) {
        error = "binary CDC record is not parsed";
        return false;
    }
    if (types.size() != Header_.KeyCount) {
        error = TStringBuilder() << "binary CDC key column count mismatch: record " << Header_.KeyCount
            << ", schema " << types.size();
        return false;
    }

    cells.clear();
    cells.reserve(types.size());
    const bool nullKeys = Header_.Has(EBinaryCdcFlag::NullKeys);
    for (const auto& type : types) {
        bool isNull = false;
        if (nullKeys) {
            ui8 marker = 0;
            if (!Take(Rest_, marker, error)) {
                return false;
            }
            if (marker > 1) {
                error = "binary CDC key null marker is not 0 or 1";
                return false;
            }
            isNull = marker == 1;
        }

        TCell cell;
        if (!ReadCell(type, isNull, cell, error)) {
            return false;
        }
        cells.push_back(cell);
    }
    return true;
}

bool TBinaryCdcReader::ReadIndexed(ui16 count, TArrayRef<const NScheme::TTypeInfo> typeByIndex,
        TVector<ui16>& indexes, TVector<TCell>& cells, TString& error)
{
    if (!Parsed_) {
        error = "binary CDC record is not parsed";
        return false;
    }

    indexes.clear();
    cells.clear();
    indexes.reserve(count);
    cells.reserve(count);
    for (ui16 i = 0; i < count; ++i) {
        ui16 index = 0;
        bool isNull = false;
        if (!ReadIndex(index, isNull, error)) {
            return false;
        }
        if (index >= typeByIndex.size()) {
            error = TStringBuilder() << "binary CDC column index " << index
                << " is outside the schema of " << typeByIndex.size() << " value columns";
            return false;
        }

        TCell cell;
        if (!ReadCell(typeByIndex[index], isNull, cell, error)) {
            return false;
        }
        indexes.push_back(index);
        cells.push_back(cell);
    }
    return true;
}

bool TBinaryCdcReader::SkipOptionalTail(TString& error) {
    if (!Parsed_) {
        error = "binary CDC record is not parsed";
        return false;
    }

    auto skip = [&](const char* what) {
        ui16 len = 0;
        if (!Take(Rest_, len, error)) {
            error = TStringBuilder() << "truncated binary CDC " << what;
            return false;
        }
        if (Rest_.size() < len) {
            error = TStringBuilder() << "truncated binary CDC " << what;
            return false;
        }
        Rest_.Skip(len);
        return true;
    };

    if (Header_.Has(EBinaryCdcFlag::HasUser) && !skip("user SID")) {
        return false;
    }
    if (Header_.Has(EBinaryCdcFlag::HasTrace) && !skip("trace id")) {
        return false;
    }
    if (Rest_) {
        error = "trailing bytes in binary CDC record";
        return false;
    }
    return true;
}

}
