#include "format.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <array>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TFormatTest) {
    Y_UNIT_TEST(ContentTypeHeader) {
        UNIT_ASSERT_EQUAL(FormatFromContentType(""), EFormat::UNKNOWN);
        UNIT_ASSERT_EQUAL(FormatFromContentType("application/json;some=stuff"), EFormat::JSON);
        UNIT_ASSERT_EQUAL(FormatFromContentType("application/x-solomon-spack"), EFormat::SPACK);
        UNIT_ASSERT_EQUAL(FormatFromContentType("application/xml"), EFormat::UNKNOWN);
        UNIT_ASSERT_EQUAL(FormatFromContentType(";application/xml"), EFormat::UNKNOWN);
    }

    Y_UNIT_TEST(AcceptHeader) {
        UNIT_ASSERT_EQUAL(FormatFromAcceptHeader(""), EFormat::UNKNOWN);
        UNIT_ASSERT_EQUAL(FormatFromAcceptHeader("*/*"), EFormat::UNKNOWN);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("application/xml"),
            EFormat::UNKNOWN);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("application/json"),
            EFormat::JSON);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("application/x-solomon-spack"),
            EFormat::SPACK);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("application/x-solomon-pb"),
            EFormat::PROTOBUF);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("application/x-solomon-txt"),
            EFormat::TEXT);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("application/json, text/plain"),
            EFormat::JSON);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("application/x-solomon-spack, application/json, text/plain"),
            EFormat::SPACK);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader(" , application/x-solomon-spack ,, application/json , text/plain"),
            EFormat::SPACK);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("application/xml, application/x-solomon-spack, text/plain"),
            EFormat::SPACK);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("text/plain"),
            EFormat::PROMETHEUS);

        UNIT_ASSERT_EQUAL(
            FormatFromAcceptHeader("application/openmetrics-text;version=0.0.1;q=0.75,text/plain;version=0.0.4"),
            EFormat::PROMETHEUS);
    }

    Y_UNIT_TEST(FormatToStrFromStr) {
        const std::array<EFormat, 6> formats = {{
            EFormat::UNKNOWN,
            EFormat::SPACK,
            EFormat::JSON,
            EFormat::PROTOBUF,
            EFormat::TEXT,
            EFormat::PROMETHEUS,
        }};

        for (EFormat f : formats) {
            TString str = (TStringBuilder() << f);
            EFormat g = FromString<EFormat>(str);
            UNIT_ASSERT_EQUAL(f, g);
        }
    }

    Y_UNIT_TEST(AcceptEncodingHeader) {
        UNIT_ASSERT_EQUAL(
            CompressionFromAcceptEncodingHeader(""),
            ECompression::UNKNOWN);

        UNIT_ASSERT_EQUAL(
            CompressionFromAcceptEncodingHeader("br"),
            ECompression::UNKNOWN);

        UNIT_ASSERT_EQUAL(
            CompressionFromAcceptEncodingHeader("identity"),
            ECompression::IDENTITY);

        UNIT_ASSERT_EQUAL(
            CompressionFromAcceptEncodingHeader("zlib"),
            ECompression::ZLIB);

        UNIT_ASSERT_EQUAL(
            CompressionFromAcceptEncodingHeader("lz4"),
            ECompression::LZ4);

        UNIT_ASSERT_EQUAL(
            CompressionFromAcceptEncodingHeader("zstd"),
            ECompression::ZSTD);

        UNIT_ASSERT_EQUAL(
            CompressionFromAcceptEncodingHeader("zstd, zlib"),
            ECompression::ZSTD);

        UNIT_ASSERT_EQUAL(
            CompressionFromAcceptEncodingHeader(" ,, , zstd , zlib"),
            ECompression::ZSTD);

        UNIT_ASSERT_EQUAL(
            CompressionFromAcceptEncodingHeader("br, deflate,lz4, zlib"),
            ECompression::LZ4);
    }

    Y_UNIT_TEST(CompressionToStrFromStr) {
        const std::array<ECompression, 5> algs = {{
            ECompression::UNKNOWN,
            ECompression::IDENTITY,
            ECompression::ZLIB,
            ECompression::LZ4,
            ECompression::ZSTD,
        }};

        for (ECompression a : algs) {
            TString str = (TStringBuilder() << a);
            ECompression b = FromString<ECompression>(str);
            UNIT_ASSERT_EQUAL(a, b);
        }
    }
}
