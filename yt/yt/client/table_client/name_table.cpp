#include "name_table.h"

#include "column_sort_schema.h"
#include "schema.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TNameTablePtr TNameTable::FromSchema(const TTableSchema& schema)
{
    auto nameTable = New<TNameTable>();
    nameTable->NameToId_.reserve(schema.Columns().size());
    for (const auto& column : schema.Columns()) {
        nameTable->DoRegisterNameOrThrow(column.Name());
    }
    return nameTable;
}

TNameTablePtr TNameTable::FromSchemaStable(const TTableSchema& schema)
{
    auto nameTable = New<TNameTable>();
    nameTable->NameToId_.reserve(schema.Columns().size());
    for (const auto& column : schema.Columns()) {
        nameTable->DoRegisterNameOrThrow(column.StableName().Underlying());
    }
    return nameTable;
}

TNameTablePtr TNameTable::FromKeyColumns(const TKeyColumns& keyColumns)
{
    auto nameTable = New<TNameTable>();
    nameTable->NameToId_.reserve(keyColumns.size());
    for (const auto& name : keyColumns) {
        nameTable->DoRegisterNameOrThrow(name);
    }
    return nameTable;
}

TNameTablePtr TNameTable::FromSortColumns(const TSortColumns& sortColumns)
{
    return TNameTable::FromKeyColumns(GetColumnNames(sortColumns));
}

int TNameTable::GetSize() const
{
    auto guard = Guard(SpinLock_);
    return IdToName_.size();
}

i64 TNameTable::GetByteSize() const
{
    auto guard = Guard(SpinLock_);
    return ByteSize_;
}

void TNameTable::SetEnableColumnNameValidation()
{
    auto guard = Guard(SpinLock_);
    EnableColumnNameValidation_ = true;
}

std::optional<int> TNameTable::FindId(TStringBuf name) const
{
    auto guard = Guard(SpinLock_);
    auto it = NameToId_.find(name);
    if (it == NameToId_.end()) {
        return std::nullopt;
    } else {
        return std::make_optional(it->second);
    }
}

int TNameTable::GetIdOrThrow(TStringBuf name) const
{
    auto optionalId = FindId(name);
    if (!optionalId) {
        THROW_ERROR_EXCEPTION("No such column %Qv", name);
    }
    return *optionalId;
}

int TNameTable::GetId(TStringBuf name) const
{
    auto index = FindId(name);
    YT_VERIFY(index);
    return *index;
}

TStringBuf TNameTable::GetName(int id) const
{
    auto guard = Guard(SpinLock_);
    YT_VERIFY(id >= 0 && id < std::ssize(IdToName_));
    return IdToName_[id];
}

TStringBuf TNameTable::GetNameOrThrow(int id) const
{
    auto guard = Guard(SpinLock_);
    if (id < 0 || id >= std::ssize(IdToName_)) {
        THROW_ERROR_EXCEPTION("Invalid column requested from name table: expected in range [0, %v), got %v",
            IdToName_.size(),
            id);
    }
    return IdToName_[id];
}

int TNameTable::RegisterName(TStringBuf name)
{
    auto guard = Guard(SpinLock_);
    return DoRegisterName(name);
}

int TNameTable::RegisterNameOrThrow(TStringBuf name)
{
    auto guard = Guard(SpinLock_);
    return DoRegisterNameOrThrow(name);
}

int TNameTable::GetIdOrRegisterName(TStringBuf name)
{
    auto guard = Guard(SpinLock_);
    auto it = NameToId_.find(name);
    if (it == NameToId_.end()) {
        return DoRegisterName(name);
    } else {
        return it->second;
    }
}

int TNameTable::DoRegisterName(TStringBuf name)
{
    int id = IdToName_.size();

    if (id >= MaxColumnId) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::CorruptedNameTable,
            "Cannot register column %Qv: column limit exceeded",
            name)
            << TErrorAttribute("max_column_id", MaxColumnId);
    }

    if (EnableColumnNameValidation_ && name.length() > MaxColumnNameLength) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::CorruptedNameTable,
            "Cannot register column %Qv: column name is too long",
            name)
            << TErrorAttribute("max_column_name_length", MaxColumnNameLength);
    }

    const auto& savedName = IdToName_.emplace_back(name);
    YT_VERIFY(NameToId_.emplace(savedName, id).second);
    ByteSize_ += savedName.length();
    return id;
}

int TNameTable::DoRegisterNameOrThrow(TStringBuf name)
{
    auto optionalId = NameToId_.find(name);
    if (optionalId != NameToId_.end()) {
        THROW_ERROR_EXCEPTION("Cannot register column %Qv: column already exists", name);
    }
    return DoRegisterName(name);
}

std::vector<TString> TNameTable::GetNames() const
{
    auto guard = Guard(SpinLock_);
    std::vector<TString> result(IdToName_.begin(), IdToName_.end());
    return result;
}

void FormatValue(TStringBuilderBase* builder, const TNameTable& nameTable, TStringBuf /*spec*/)
{
    builder->AppendChar('{');
    bool first = true;
    for (const auto& name : nameTable.GetNames()) {
        if (first) {
            first = false;
        } else {
            builder->AppendString("; ");
        }
        builder->AppendFormat("%Qv=%v", name, nameTable.GetId(name));
    }
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

TNameTableReader::TNameTableReader(TNameTablePtr nameTable)
    : NameTable_(std::move(nameTable))
{
    Fill();
}

TStringBuf TNameTableReader::FindName(int id) const
{
    if (id < 0) {
        return {};
    }

    if (id >= std::ssize(IdToNameCache_)) {
        Fill();

        if (id >= std::ssize(IdToNameCache_)) {
            return {};
        }
    }

    return IdToNameCache_[id];
}

TStringBuf TNameTableReader::GetName(int id) const
{
    YT_ASSERT(id >= 0);
    if (id >= std::ssize(IdToNameCache_)) {
        Fill();
    }

    YT_ASSERT(id < std::ssize(IdToNameCache_));
    return IdToNameCache_[id];
}

int TNameTableReader::GetSize() const
{
    Fill();
    return static_cast<int>(IdToNameCache_.size());
}

void TNameTableReader::Fill() const
{
    int thisSize = static_cast<int>(IdToNameCache_.size());
    int underlyingSize = NameTable_->GetSize();
    for (int id = thisSize; id < underlyingSize; ++id) {
        IdToNameCache_.push_back(std::string(NameTable_->GetName(id)));
    }
}

////////////////////////////////////////////////////////////////////////////////

TNameTableWriter::TNameTableWriter(TNameTablePtr nameTable)
    : NameTable_(std::move(nameTable))
{ }

std::optional<int> TNameTableWriter::FindId(TStringBuf name) const
{
    auto it = NameToId_.find(name);
    if (it != NameToId_.end()) {
        return it->second;
    }

    auto optionalId = NameTable_->FindId(name);
    if (optionalId) {
        Names_.push_back(TString(name));
        YT_VERIFY(NameToId_.emplace(Names_.back(), *optionalId).second);
    }
    return optionalId;
}

int TNameTableWriter::GetIdOrThrow(TStringBuf name) const
{
    auto optionalId = FindId(name);
    if (!optionalId) {
        THROW_ERROR_EXCEPTION("No such column %Qv", name);
    }
    return *optionalId;
}

int TNameTableWriter::GetIdOrRegisterName(TStringBuf name)
{
    auto it = NameToId_.find(name);
    if (it != NameToId_.end()) {
        return it->second;
    }

    auto id = NameTable_->GetIdOrRegisterName(name);
    Names_.push_back(TString(name));
    YT_VERIFY(NameToId_.emplace(Names_.back(), id).second);
    return id;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNameTableExt* protoNameTable, const TNameTablePtr& nameTable)
{
    using NYT::ToProto;

    ToProto(protoNameTable->mutable_names(), nameTable->GetNames());
}

void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable)
{
    using NYT::FromProto;

    *nameTable = TNameTable::FromKeyColumns(FromProto<std::vector<TString>>(protoNameTable.names()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
