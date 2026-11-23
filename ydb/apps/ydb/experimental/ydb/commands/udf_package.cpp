#include "udf_package.h"

#include <library/cpp/digest/old_crc/crc.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <util/system/types.h>

#include <array>
#include <limits>
#include <optional>

namespace NYdb::NConsoleClient {
namespace {

constexpr size_t MaxBodySize = 256ULL * 1024 * 1024;
constexpr size_t MaxManifestSize = 1ULL * 1024 * 1024;
constexpr size_t MaxArchiveSize = MaxBodySize + MaxManifestSize + 16ULL * 1024 * 1024;
constexpr size_t MaxExpandedTarSize = MaxBodySize + MaxManifestSize + 4ULL * 1024 * 1024;
constexpr size_t TarBlockSize = 512;

ui16 ReadLe16(TStringBuf data, size_t offset) {
    if (offset > data.size() || data.size() - offset < sizeof(ui16)) {
        ythrow yexception() << "Truncated ZIP package";
    }
    const auto* ptr = reinterpret_cast<const ui8*>(data.data() + offset);
    return static_cast<ui16>(ptr[0]) | (static_cast<ui16>(ptr[1]) << 8);
}

ui32 ReadLe32(TStringBuf data, size_t offset) {
    if (offset > data.size() || data.size() - offset < sizeof(ui32)) {
        ythrow yexception() << "Truncated ZIP package";
    }
    const auto* ptr = reinterpret_cast<const ui8*>(data.data() + offset);
    return static_cast<ui32>(ptr[0]) |
        (static_cast<ui32>(ptr[1]) << 8) |
        (static_cast<ui32>(ptr[2]) << 16) |
        (static_cast<ui32>(ptr[3]) << 24);
}

TString ReadLimited(IInputStream& input, size_t limit, TStringBuf description) {
    TString result;
    std::array<char, 64 * 1024> buffer;
    while (const size_t read = input.Load(buffer.data(), buffer.size())) {
        if (read > limit - result.size()) {
            ythrow yexception() << description << " exceeds " << limit << " bytes";
        }
        result.append(buffer.data(), read);
    }
    return result;
}

void ValidateEntryName(TStringBuf name) {
    if (name.empty() || name == "." || name == ".." || name.Contains('/') || name.Contains('\\') || name.Contains('\0')) {
        ythrow yexception() << "Package entries must be regular files in the archive root: '" << name << "'";
    }
}

class TPackageBuilder {
public:
    void Add(TStringBuf name, std::string data) {
        ValidateEntryName(name);
        if (name == "manifest.json") {
            if (HaveManifest_) {
                ythrow yexception() << "Package contains duplicate manifest.json";
            }
            if (data.size() > MaxManifestSize) {
                ythrow yexception() << "Package manifest.json exceeds " << MaxManifestSize << " bytes";
            }
            Package_.Manifest = std::move(data);
            HaveManifest_ = true;
        } else {
            if (HaveBody_) {
                ythrow yexception() << "Package must contain exactly one binary file";
            }
            if (data.size() > MaxBodySize) {
                ythrow yexception() << "Package binary exceeds " << MaxBodySize << " bytes";
            }
            Package_.Body = std::move(data);
            HaveBody_ = true;
        }
    }

    TUdfPackage Finish() {
        if (!HaveManifest_) {
            ythrow yexception() << "Package does not contain manifest.json";
        }
        if (!HaveBody_) {
            ythrow yexception() << "Package does not contain a binary file";
        }
        return std::move(Package_);
    }

private:
    TUdfPackage Package_;
    bool HaveManifest_ = false;
    bool HaveBody_ = false;
};

bool IsZeroBlock(TStringBuf block) {
    for (char value : block) {
        if (value != '\0') {
            return false;
        }
    }
    return true;
}

ui64 ParseTarOctal(TStringBuf field, TStringBuf fieldName) {
    size_t begin = 0;
    while (begin < field.size() && (field[begin] == ' ' || field[begin] == '\0')) {
        ++begin;
    }
    ui64 result = 0;
    bool haveDigit = false;
    for (size_t index = begin; index < field.size(); ++index) {
        const char value = field[index];
        if (value == ' ' || value == '\0') {
            break;
        }
        if (value < '0' || value > '7' || result > (std::numeric_limits<ui64>::max() >> 3)) {
            ythrow yexception() << "Invalid TAR " << fieldName;
        }
        result = (result << 3) + static_cast<ui64>(value - '0');
        haveDigit = true;
    }
    if (!haveDigit) {
        ythrow yexception() << "Missing TAR " << fieldName;
    }
    return result;
}

ui64 ParseTarDecimal(TStringBuf field, TStringBuf fieldName) {
    if (field.empty()) {
        ythrow yexception() << "Missing TAR " << fieldName;
    }
    ui64 result = 0;
    for (const char digit : field) {
        if (digit < '0' || digit > '9' ||
            result > (std::numeric_limits<ui64>::max() - (digit - '0')) / 10)
        {
            ythrow yexception() << "Invalid TAR " << fieldName;
        }
        result = result * 10 + (digit - '0');
    }
    return result;
}

struct TPaxAttributes {
    std::optional<TString> Path;
    std::optional<TString> Size;
};

void ParsePaxRecords(TStringBuf data, TPaxAttributes& attributes) {
    size_t offset = 0;
    while (offset < data.size()) {
        const size_t space = data.find(' ', offset);
        if (space == TStringBuf::npos) {
            ythrow yexception() << "Invalid PAX record length";
        }
        const ui64 length = ParseTarDecimal(data.SubStr(offset, space - offset), "PAX record length");
        if (length <= space - offset + 2 || length > data.size() - offset) {
            ythrow yexception() << "Invalid PAX record length";
        }
        const TStringBuf record = data.SubStr(space + 1, length - (space - offset) - 1);
        const size_t equals = record.find('=');
        if (equals == TStringBuf::npos || equals == 0 || record.back() != '\n' ||
            record.Contains('\0'))
        {
            ythrow yexception() << "Invalid PAX record";
        }
        const TStringBuf key = record.Head(equals);
        const TStringBuf value = record.SubStr(equals + 1, record.size() - equals - 2);
        if (key.StartsWith("GNU.sparse.")) {
            ythrow yexception() << "Sparse TAR entries are not supported";
        }
        if (key == "path") {
            attributes.Path = TString(value);
        } else if (key == "size") {
            attributes.Size = TString(value);
        }
        // Other metadata (timestamps, owner, etc.) does not affect package contents.
        offset += length;
    }
}

TStringBuf PaxValue(const std::optional<TString>& global, const std::optional<TString>& local) {
    // An explicitly empty local value suppresses the global override.
    if (local) {
        return *local;
    }
    if (global) {
        return *global;
    }
    return {};
}

TStringBuf TarStringField(TStringBuf header, size_t offset, size_t length) {
    TStringBuf field = header.SubStr(offset, length);
    const size_t end = field.find('\0');
    return end == TStringBuf::npos ? field : field.Head(end);
}

TString TarEntryName(TStringBuf header, TStringBuf paxPath) {
    if (!paxPath.empty()) {
        return TString(paxPath);
    }
    const TStringBuf name = TarStringField(header, 0, 100);
    const TStringBuf prefix = TarStringField(header, 345, 155);
    if (!prefix.empty()) {
        return TString(prefix) + "/" + TString(name);
    }
    return TString(name);
}

void ValidateTarChecksum(TStringBuf header) {
    const ui64 expected = ParseTarOctal(header.SubStr(148, 8), "checksum");
    ui64 actual = 0;
    for (size_t index = 0; index < header.size(); ++index) {
        actual += index >= 148 && index < 156 ? static_cast<ui8>(' ') : static_cast<ui8>(header[index]);
    }
    if (actual != expected) {
        ythrow yexception() << "Invalid TAR header checksum";
    }
}

TUdfPackage ParseTar(TStringBuf data) {
    TPackageBuilder builder;
    TPaxAttributes globalPax;
    TPaxAttributes localPax;
    bool haveLocalPax = false;
    size_t offset = 0;
    bool endSeen = false;
    while (offset < data.size()) {
        if (data.size() - offset < TarBlockSize) {
            ythrow yexception() << "Truncated TAR package";
        }
        const TStringBuf header = data.SubStr(offset, TarBlockSize);
        offset += TarBlockSize;
        if (IsZeroBlock(header)) {
            endSeen = true;
            break;
        }
        ValidateTarChecksum(header);
        const TStringBuf headerName = TarStringField(header, 0, 100);
        const char type = header[156];
        if (type != '\0' && type != '0' && type != 'x' && type != 'g') {
            ythrow yexception() << "Package contains a non-regular TAR entry '" << headerName << "'";
        }
        const bool isPax = type == 'x' || type == 'g';
        const TStringBuf paxSize = isPax ? TStringBuf{} : PaxValue(globalPax.Size, localPax.Size);
        const ui64 size64 = paxSize.empty()
            ? ParseTarOctal(header.SubStr(124, 12), "file size")
            : ParseTarDecimal(paxSize, "PAX file size");
        if (size64 > data.size() - offset) {
            ythrow yexception() << "Truncated TAR entry '" << headerName << "'";
        }
        const size_t size = static_cast<size_t>(size64);
        if (isPax) {
            if (size > MaxManifestSize) {
                ythrow yexception() << "PAX metadata exceeds " << MaxManifestSize << " bytes";
            }
            ParsePaxRecords(data.SubStr(offset, size), type == 'g' ? globalPax : localPax);
            haveLocalPax |= type == 'x';
        } else {
            const TString name = TarEntryName(header, PaxValue(globalPax.Path, localPax.Path));
            ValidateEntryName(name);
            const size_t limit = name == "manifest.json" ? MaxManifestSize : MaxBodySize;
            if (size > limit) {
                ythrow yexception() << "Package entry '" << name << "' exceeds " << limit << " bytes";
            }
            builder.Add(name, std::string(data.data() + offset, size));
            localPax = {};
            haveLocalPax = false;
        }
        const size_t paddedSize = (size + TarBlockSize - 1) / TarBlockSize * TarBlockSize;
        if (paddedSize > data.size() - offset) {
            ythrow yexception() << "Truncated TAR padding after '" << headerName << "'";
        }
        offset += paddedSize;
    }
    if (!endSeen) {
        ythrow yexception() << "TAR package has no end marker";
    }
    if (haveLocalPax) {
        ythrow yexception() << "TAR package ends with unused PAX metadata";
    }
    while (offset < data.size()) {
        if (data[offset++] != '\0') {
            ythrow yexception() << "TAR package contains data after the end marker";
        }
    }
    return builder.Finish();
}

size_t FindZipEnd(TStringBuf data) {
    constexpr ui32 EndSignature = 0x06054b50;
    constexpr size_t EndSize = 22;
    constexpr size_t MaxCommentSize = 65535;
    if (data.size() < EndSize) {
        ythrow yexception() << "Truncated ZIP package";
    }
    const size_t begin = data.size() > EndSize + MaxCommentSize ? data.size() - EndSize - MaxCommentSize : 0;
    for (size_t offset = data.size() - EndSize + 1; offset-- > begin;) {
        if (ReadLe32(data, offset) == EndSignature &&
            offset + EndSize + ReadLe16(data, offset + 20) == data.size())
        {
            return offset;
        }
    }
    ythrow yexception() << "ZIP package has no end-of-central-directory record";
}

std::string ExtractZipEntry(TStringBuf data, size_t localOffset, TStringBuf expectedName,
        ui16 expectedFlags, ui16 expectedMethod, ui32 compressedSize, ui32 uncompressedSize, ui32 expectedCrc)
{
    constexpr ui32 LocalSignature = 0x04034b50;
    constexpr size_t LocalHeaderSize = 30;
    if (ReadLe32(data, localOffset) != LocalSignature || data.size() - localOffset < LocalHeaderSize) {
        ythrow yexception() << "Invalid ZIP local header for '" << expectedName << "'";
    }
    const ui16 flags = ReadLe16(data, localOffset + 6);
    const ui16 method = ReadLe16(data, localOffset + 8);
    const size_t nameLength = ReadLe16(data, localOffset + 26);
    const size_t extraLength = ReadLe16(data, localOffset + 28);
    const size_t nameOffset = localOffset + LocalHeaderSize;
    if (nameOffset > data.size() || nameLength > data.size() - nameOffset) {
        ythrow yexception() << "Truncated ZIP local file name";
    }
    const TStringBuf localName = data.SubStr(nameOffset, nameLength);
    if (localName != expectedName || flags != expectedFlags || method != expectedMethod) {
        ythrow yexception() << "ZIP central and local headers disagree for '" << expectedName << "'";
    }
    const size_t bodyOffset = nameOffset + nameLength + extraLength;
    if (bodyOffset > data.size() || compressedSize > data.size() - bodyOffset) {
        ythrow yexception() << "Truncated ZIP entry '" << expectedName << "'";
    }
    const TStringBuf compressed = data.SubStr(bodyOffset, compressedSize);
    TString unpacked;
    if (method == 0) {
        unpacked = TString(compressed);
    } else if (method == 8) {
        const TString compressedData(compressed);
        TStringInput input(compressedData);
        TZLibDecompress inflater(&input, ZLib::Raw);
        unpacked = ReadLimited(inflater, uncompressedSize, "ZIP entry");
    } else {
        ythrow yexception() << "Unsupported ZIP compression method " << method << " for '" << expectedName << "'";
    }
    if (unpacked.size() != uncompressedSize) {
        ythrow yexception() << "ZIP entry size mismatch for '" << expectedName << "'";
    }
    if (crc32(unpacked.data(), unpacked.size()) != expectedCrc) {
        ythrow yexception() << "ZIP entry CRC mismatch for '" << expectedName << "'";
    }
    return std::string(unpacked.data(), unpacked.size());
}

TUdfPackage ParseZip(TStringBuf data) {
    constexpr ui32 CentralSignature = 0x02014b50;
    constexpr size_t CentralHeaderSize = 46;
    const size_t endOffset = FindZipEnd(data);
    if (ReadLe16(data, endOffset + 4) != 0 || ReadLe16(data, endOffset + 6) != 0 ||
        ReadLe16(data, endOffset + 8) != ReadLe16(data, endOffset + 10))
    {
        ythrow yexception() << "Multi-disk ZIP packages are not supported";
    }
    const size_t entryCount = ReadLe16(data, endOffset + 10);
    const size_t centralSize = ReadLe32(data, endOffset + 12);
    const size_t centralOffset = ReadLe32(data, endOffset + 16);
    if (entryCount != 2) {
        ythrow yexception() << "Package must contain exactly two files";
    }
    if (centralOffset > endOffset || centralSize > endOffset - centralOffset || centralOffset + centralSize != endOffset) {
        ythrow yexception() << "Invalid ZIP central directory";
    }

    TPackageBuilder builder;
    size_t offset = centralOffset;
    for (size_t index = 0; index < entryCount; ++index) {
        if (offset > endOffset || endOffset - offset < CentralHeaderSize || ReadLe32(data, offset) != CentralSignature) {
            ythrow yexception() << "Invalid ZIP central directory entry";
        }
        const ui16 versionMadeBy = ReadLe16(data, offset + 4);
        const ui16 flags = ReadLe16(data, offset + 8);
        const ui16 method = ReadLe16(data, offset + 10);
        const ui32 crc = ReadLe32(data, offset + 16);
        const ui32 compressedSize = ReadLe32(data, offset + 20);
        const ui32 uncompressedSize = ReadLe32(data, offset + 24);
        const size_t nameLength = ReadLe16(data, offset + 28);
        const size_t extraLength = ReadLe16(data, offset + 30);
        const size_t commentLength = ReadLe16(data, offset + 32);
        const ui16 disk = ReadLe16(data, offset + 34);
        const ui32 externalAttributes = ReadLe32(data, offset + 38);
        const ui32 localOffset = ReadLe32(data, offset + 42);
        const size_t recordSize = CentralHeaderSize + nameLength + extraLength + commentLength;
        if (recordSize > endOffset - offset) {
            ythrow yexception() << "Truncated ZIP central directory entry";
        }
        const TStringBuf name = data.SubStr(offset + CentralHeaderSize, nameLength);
        ValidateEntryName(name);
        if ((flags & 1) != 0) {
            ythrow yexception() << "Encrypted ZIP entries are not supported";
        }
        if (disk != 0 || compressedSize == std::numeric_limits<ui32>::max() ||
            uncompressedSize == std::numeric_limits<ui32>::max() || localOffset == std::numeric_limits<ui32>::max())
        {
            ythrow yexception() << "Multi-disk and ZIP64 packages are not supported";
        }
        const ui8 creator = versionMadeBy >> 8;
        const ui32 unixMode = externalAttributes >> 16;
        const ui32 fileType = unixMode & 0170000;
        if ((externalAttributes & 0x10) != 0 || (creator == 3 && fileType != 0 && fileType != 0100000)) {
            ythrow yexception() << "Package contains a non-regular ZIP entry '" << name << "'";
        }
        const size_t limit = name == "manifest.json" ? MaxManifestSize : MaxBodySize;
        if (uncompressedSize > limit) {
            ythrow yexception() << "ZIP entry '" << name << "' exceeds " << limit << " bytes";
        }
        builder.Add(name, ExtractZipEntry(
            data, localOffset, name, flags, method, compressedSize, uncompressedSize, crc));
        offset += recordSize;
    }
    if (offset != endOffset) {
        ythrow yexception() << "Invalid ZIP central directory size";
    }
    return builder.Finish();
}

} // namespace

TUdfPackage ParseUdfPackage(TStringBuf data) {
    constexpr TStringBuf ZipSignature = "PK\x03\x04";
    constexpr TStringBuf GzipSignature = "\x1f\x8b";
    if (data.StartsWith(ZipSignature)) {
        return ParseZip(data);
    }
    if (data.StartsWith(GzipSignature)) {
        const TString compressedData(data);
        TStringInput input(compressedData);
        TBufferedZLibDecompress gzip(&input, ZLib::GZip);
        const TString tar = ReadLimited(gzip, MaxExpandedTarSize, "Expanded TAR package");
        return ParseTar(tar);
    }
    return ParseTar(data);
}

TUdfPackage ReadUdfPackage(const TString& path) {
    TFileInput input(path);
    const TString data = ReadLimited(input, MaxArchiveSize, "Package archive");
    return ParseUdfPackage(data);
}

} // namespace NYdb::NConsoleClient
