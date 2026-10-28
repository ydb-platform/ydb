#include <ydb/apps/ydb/experimental/ydb/commands/udf_package.h>

#include <library/cpp/digest/old_crc/crc.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <util/string/cast.h>

#include <cstring>

namespace NYdb::NConsoleClient {
namespace {

struct TEntry {
    TString Name;
    TString Data;
    char Type = '0';
};

TString PaxRecord(TStringBuf key, TStringBuf value) {
    const TString payload = TString(key) + "=" + TString(value) + "\n";
    size_t length = payload.size() + 2;
    while (true) {
        const TString record = ToString(length) + " " + payload;
        if (record.size() == length) {
            return record;
        }
        length = record.size();
    }
}

void WriteOctal(TString& header, size_t offset, size_t length, ui64 value) {
    std::memset(header.Detach() + offset, '0', length);
    header[offset + length - 1] = '\0';
    for (size_t index = offset + length - 1; index-- > offset && value;) {
        header[index] = '0' + value % 8;
        value /= 8;
    }
}

TString MakeTar(const TVector<TEntry>& entries) {
    TString result;
    for (const auto& entry : entries) {
        TString header(512, '\0');
        UNIT_ASSERT(entry.Name.size() < 100);
        std::memcpy(header.Detach(), entry.Name.data(), entry.Name.size());
        WriteOctal(header, 100, 8, 0644);
        WriteOctal(header, 108, 8, 0);
        WriteOctal(header, 116, 8, 0);
        WriteOctal(header, 124, 12, entry.Data.size());
        WriteOctal(header, 136, 12, 0);
        std::memset(header.Detach() + 148, ' ', 8);
        header[156] = entry.Type;
        std::memcpy(header.Detach() + 257, "ustar", 5);
        ui64 checksum = 0;
        for (char value : header) {
            checksum += static_cast<ui8>(value);
        }
        WriteOctal(header, 148, 7, checksum);
        header[155] = ' ';
        result += header;
        result += entry.Data;
        result.append((512 - entry.Data.size() % 512) % 512, '\0');
    }
    result.append(1024, '\0');
    return result;
}

TString Compress(TStringBuf data, ZLib::StreamType type) {
    TString result;
    TStringOutput output(result);
    {
        TZLibCompress compressor(&output, type);
        compressor.Write(data.data(), data.size());
        compressor.Finish();
    }
    return result;
}

void AppendLe16(TString& output, ui16 value) {
    output.push_back(static_cast<char>(value));
    output.push_back(static_cast<char>(value >> 8));
}

void AppendLe32(TString& output, ui32 value) {
    output.push_back(static_cast<char>(value));
    output.push_back(static_cast<char>(value >> 8));
    output.push_back(static_cast<char>(value >> 16));
    output.push_back(static_cast<char>(value >> 24));
}

TString MakeZip(const TVector<TEntry>& entries, bool deflate) {
    struct TCentralEntry {
        TEntry Entry;
        TString Compressed;
        ui32 Offset;
        ui32 Crc;
        ui16 Method;
    };

    TString result;
    TVector<TCentralEntry> central;
    for (const auto& entry : entries) {
        TCentralEntry item{entry, deflate ? Compress(entry.Data, ZLib::Raw) : entry.Data,
            static_cast<ui32>(result.size()), crc32(entry.Data.data(), entry.Data.size()),
            static_cast<ui16>(deflate ? 8 : 0)};
        AppendLe32(result, 0x04034b50);
        AppendLe16(result, 20);
        AppendLe16(result, 0);
        AppendLe16(result, item.Method);
        AppendLe16(result, 0);
        AppendLe16(result, 0);
        AppendLe32(result, item.Crc);
        AppendLe32(result, item.Compressed.size());
        AppendLe32(result, entry.Data.size());
        AppendLe16(result, entry.Name.size());
        AppendLe16(result, 0);
        result += entry.Name;
        result += item.Compressed;
        central.push_back(std::move(item));
    }

    const ui32 centralOffset = result.size();
    for (const auto& item : central) {
        AppendLe32(result, 0x02014b50);
        AppendLe16(result, (3 << 8) | 20);
        AppendLe16(result, 20);
        AppendLe16(result, 0);
        AppendLe16(result, item.Method);
        AppendLe16(result, 0);
        AppendLe16(result, 0);
        AppendLe32(result, item.Crc);
        AppendLe32(result, item.Compressed.size());
        AppendLe32(result, item.Entry.Data.size());
        AppendLe16(result, item.Entry.Name.size());
        AppendLe16(result, 0);
        AppendLe16(result, 0);
        AppendLe16(result, 0);
        AppendLe16(result, 0);
        AppendLe32(result, 0100644 << 16);
        AppendLe32(result, item.Offset);
        result += item.Entry.Name;
    }
    const ui32 centralSize = result.size() - centralOffset;
    AppendLe32(result, 0x06054b50);
    AppendLe16(result, 0);
    AppendLe16(result, 0);
    AppendLe16(result, entries.size());
    AppendLe16(result, entries.size());
    AppendLe32(result, centralSize);
    AppendLe32(result, centralOffset);
    AppendLe16(result, 0);
    return result;
}

const TVector<TEntry> ValidEntries = {
    {"manifest.json", R"({"module_name":"m","module_type":"library","module_kind":"wasm"})"},
    {"module.wasm", "wasm body"},
};

} // namespace

Y_UNIT_TEST_SUITE(TUdfPackageTest) {
    Y_UNIT_TEST(ParseTar) {
        const auto package = ParseUdfPackage(MakeTar(ValidEntries));
        UNIT_ASSERT_VALUES_EQUAL(package.Manifest, ValidEntries[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(package.Body, ValidEntries[1].Data);
    }

    Y_UNIT_TEST(ParseGzipTar) {
        const auto package = ParseUdfPackage(Compress(MakeTar(ValidEntries), ZLib::GZip));
        UNIT_ASSERT_VALUES_EQUAL(package.Manifest, ValidEntries[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(package.Body, ValidEntries[1].Data);
    }

    Y_UNIT_TEST(ParsePythonPaxGzipTar) {
        // Python tarfile emits PAX extended headers for fractional mtimes.
        const TString path = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "data/python_pax_package.tar.gz");
        const auto package = ReadUdfPackage(path);
        UNIT_ASSERT_VALUES_EQUAL(package.Manifest, ValidEntries[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(package.Body, ValidEntries[1].Data);
    }

    Y_UNIT_TEST(ValidateEffectivePaxPath) {
        const auto valid = ParseUdfPackage(MakeTar({
            {"PaxHeader", PaxRecord("path", "module.wasm"), 'x'},
            {"placeholder", ValidEntries[1].Data},
            ValidEntries[0],
        }));
        UNIT_ASSERT_VALUES_EQUAL(valid.Body, ValidEntries[1].Data);
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseUdfPackage(MakeTar({
            ValidEntries[0],
            {"PaxHeader", PaxRecord("path", "dir/module.wasm"), 'x'},
            ValidEntries[1],
        })), yexception, "archive root");
    }

    Y_UNIT_TEST(RejectSparsePax) {
        for (const char type : {'x', 'g'}) {
            for (const TString& records : {
                // These records describe four zero bytes followed by the stored body.
                PaxRecord("GNU.sparse.map", "4,9") + PaxRecord("GNU.sparse.size", "13"),
                PaxRecord("GNU.sparse.major", "1"),
                PaxRecord("GNU.sparse.name", "module.wasm"),
            }) {
                UNIT_ASSERT_EXCEPTION_CONTAINS(ParseUdfPackage(MakeTar({
                    ValidEntries[0],
                    {"PaxHeader", records, type},
                    ValidEntries[1],
                })), yexception, "Sparse TAR entries are not supported");
            }
        }
    }

    Y_UNIT_TEST(PaxOverridesAndScope) {
        const TEntry global{"PaxHeader", PaxRecord("path", "dir/file"), 'g'};
        const TEntry local{"PaxHeader", PaxRecord("path", "manifest.json"), 'x'};
        const auto package = ParseUdfPackage(MakeTar({
            global, local, {"placeholder", ValidEntries[0].Data},
            {"PaxHeader", PaxRecord("path", "") + PaxRecord("mtime", "1.25"), 'x'},
            ValidEntries[1],
        }));
        UNIT_ASSERT_VALUES_EQUAL(package.Manifest, ValidEntries[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(package.Body, ValidEntries[1].Data);
        // The local path applies only to the manifest, so the next entry inherits
        // the unsafe global path unless it explicitly suppresses that override.
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseUdfPackage(MakeTar({
            global, local, ValidEntries[0], ValidEntries[1],
        })), yexception, "archive root");

        const auto sized = ParseUdfPackage(MakeTar({
            ValidEntries[0],
            {"PaxHeader", PaxRecord("size", "1"), 'g'},
            {"PaxHeader", PaxRecord("size", "9"), 'x'},
            ValidEntries[1],
        }));
        UNIT_ASSERT_VALUES_EQUAL(sized.Body, ValidEntries[1].Data);
    }

    Y_UNIT_TEST(RejectMalformedPax) {
        for (const TString& records : {TString("0 path=a\n"), TString("999 path=a\n"), TString("13 path=abc\n")}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(ParseUdfPackage(MakeTar({
                {"PaxHeader", records, 'x'}, ValidEntries[0], ValidEntries[1],
            })), yexception, "PAX");
        }
        for (const TString& records : {TString{}, PaxRecord("mtime", "1.25")}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(ParseUdfPackage(MakeTar({
                ValidEntries[0], ValidEntries[1], {"PaxHeader", records, 'x'},
            })), yexception, "unused PAX metadata");
        }
    }

    Y_UNIT_TEST(ParseStoredZip) {
        const auto package = ParseUdfPackage(MakeZip(ValidEntries, false));
        UNIT_ASSERT_VALUES_EQUAL(package.Manifest, ValidEntries[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(package.Body, ValidEntries[1].Data);
    }

    Y_UNIT_TEST(ParseDeflatedZip) {
        const auto package = ParseUdfPackage(MakeZip(ValidEntries, true));
        UNIT_ASSERT_VALUES_EQUAL(package.Manifest, ValidEntries[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(package.Body, ValidEntries[1].Data);
    }

    Y_UNIT_TEST(RejectUnsafeAndAmbiguousLayouts) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseUdfPackage(MakeTar({{"dir/manifest.json", "{}"}, {"module.wasm", "body"}})),
            yexception, "archive root");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseUdfPackage(MakeTar({{"manifest.json", "{}"}, {"first.wasm", "one"}, {"second.wasm", "two"}})),
            yexception, "exactly one binary");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseUdfPackage(MakeZip({{"manifest.json", "{}"}, {"manifest.json", "{}"}}, false)),
            yexception, "duplicate manifest");
    }

    Y_UNIT_TEST(RejectCorruptZipBody) {
        TString package = MakeZip(ValidEntries, false);
        const size_t offset = 30 + ValidEntries[0].Name.size();
        package[offset] = static_cast<char>(package[offset]) ^ 1;
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseUdfPackage(package), yexception, "CRC mismatch");
    }
}

} // namespace NYdb::NConsoleClient
