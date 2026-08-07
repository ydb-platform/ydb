#include "bytecode.h"

#include <util/digest/city.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 HashSharedRef(const TSharedRef& ref)
{
    const auto buf = ref.ToStringBuf();
    return CityHash64(buf.data(), buf.size());
}

} // namespace

bool TModuleBytecode::operator==(const TModuleBytecode& other) const
{
    return Format == other.Format
        && Data.ToStringBuf() == other.Data.ToStringBuf()
        && ObjectCode.ToStringBuf() == other.ObjectCode.ToStringBuf();
}

TModuleBytecode::operator size_t() const
{
    // Prefer TModuleBytecodeKey for hot maps; this remains correct for HashSet.
    return static_cast<size_t>(TModuleBytecodeKey::From(*this));
}

TModuleBytecodeKey TModuleBytecodeKey::From(const TModuleBytecode& bytecode)
{
    const auto data = bytecode.Data.ToStringBuf();
    const auto objectCode = bytecode.ObjectCode.ToStringBuf();
    return TModuleBytecodeKey{
        .Format = bytecode.Format,
        .DataHash = HashSharedRef(bytecode.Data),
        .ObjectCodeHash = HashSharedRef(bytecode.ObjectCode),
        .DataSize = data.size(),
        .ObjectCodeSize = objectCode.size(),
    };
}

bool TModuleBytecodeKey::operator==(const TModuleBytecodeKey& other) const
{
    return Format == other.Format
        && DataHash == other.DataHash
        && ObjectCodeHash == other.ObjectCodeHash
        && DataSize == other.DataSize
        && ObjectCodeSize == other.ObjectCodeSize;
}

TModuleBytecodeKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, Format);
    HashCombine(result, DataHash);
    HashCombine(result, ObjectCodeHash);
    HashCombine(result, DataSize);
    HashCombine(result, ObjectCodeSize);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
