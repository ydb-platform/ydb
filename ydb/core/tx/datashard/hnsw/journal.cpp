#include "cache.h"
#include "serialization.h"

#include <util/generic/yexception.h>

namespace NKikimr::NDataShard {
namespace {

void WriteWord(std::ostream& out, ui64 value) {
    char data[8];
    for (unsigned i = 0; i < 8; ++i)
        data[i] = static_cast<char>(value >> (8 * i));
    out.write(data, sizeof(data));
}

ui64 ReadWord(std::istream& in) {
    unsigned char data[8];
    if (!in.read(reinterpret_cast<char*>(data), sizeof(data)))
        ythrow yexception() << "Truncated HNSW journal";
    ui64 value = 0;
    for (unsigned i = 0; i < 8; ++i)
        value |= ui64(data[i]) << (8 * i);
    return value;
}

void WriteBytes(std::ostream& out, TStringBuf value) {
    WriteWord(out, value.size());
    out.write(value.data(), value.size());
}

TString ReadBytes(std::istream& in, ui64& budget) {
    const ui64 size = ReadWord(in);
    if (size > budget)
        ythrow yexception() << "HNSW journal exceeds memory budget";
    budget -= size;
    TString value = TString::Uninitialized(size);
    if (!in.read(value.Detach(), size))
        ythrow yexception() << "Truncated HNSW journal value";
    return value;
}

} // namespace

void THnswJournalSnapshot::Save(std::ostream& destination) const {
    NHnswSerialization::TSnapshotStreamBuf buffer(destination.rdbuf());
    std::ostream output(&buffer);
    output.exceptions(std::ios::badbit | std::ios::failbit);
    WriteWord(output, 1);
    WriteWord(output, Changes.size());
    for (const auto& change : Changes) {
        WriteBytes(output, change.Key);
        WriteWord(output, change.Version.Step);
        WriteWord(output, change.Version.TxId);
        WriteWord(output, bool(change.Vector));
        if (change.Vector)
            WriteBytes(output, *change.Vector);
    }
    WriteWord(destination, buffer.Hash);
    if (!destination)
        ythrow yexception() << "Cannot write HNSW journal";
}

std::shared_ptr<THnswJournalSnapshot> THnswJournalSnapshot::Load(std::istream& source, ui64 budget) {
    NHnswSerialization::TSnapshotStreamBuf buffer(source.rdbuf());
    std::istream input(&buffer);
    if (ReadWord(input) != 1)
        ythrow yexception() << "Unsupported HNSW journal format";
    const auto count = ReadWord(input);
    if (count > budget / sizeof(TChange))
        ythrow yexception() << "HNSW journal exceeds memory budget";
    budget -= count * sizeof(TChange);
    auto result = std::make_shared<THnswJournalSnapshot>();
    result->Changes.reserve(count);
    for (ui64 i = 0; i < count; ++i) {
        TChange change;
        change.Key = ReadBytes(input, budget);
        change.Version.Step = ReadWord(input);
        change.Version.TxId = ReadWord(input);
        const auto present = ReadWord(input);
        if (present > 1)
            ythrow yexception() << "Invalid HNSW journal entry";
        if (present)
            change.Vector = ReadBytes(input, budget);
        result->Changes.push_back(std::move(change));
    }
    if (ReadWord(source) != buffer.Hash)
        ythrow yexception() << "HNSW journal checksum mismatch";
    if (source.peek() != std::char_traits<char>::eof())
        ythrow yexception() << "Trailing data in HNSW journal";
    return result;
}

} // namespace NKikimr::NDataShard
