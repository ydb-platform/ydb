#include <yql/essentials/public/sql_format/abi/interface/sql_format.h>

#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>

struct TYqlSqlFormatHandle {
    TString Output;
    TString Error;
    int Status = YQL_SQL_FORMAT_EXCEPTION;
};

namespace {

TYqlSqlFormatHandle AllocationFailure;
constexpr TStringBuf AllocationFailureMessage = "Failed to allocate SQL formatter result";

bool Format(const TString& query, TString& formattedQuery, TString& error) {
    NSQLTranslationV1::TLexers lexers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
    };

    NSQLTranslationV1::TParsers parsers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false),
    };

    return NSQLFormat::SqlFormatSimple(lexers, parsers, query, formattedQuery, error);
}

TYqlSqlFormatHandle* MakeResult(const char* query, size_t size) {
    auto result = MakeHolder<TYqlSqlFormatHandle>();
    try {
        Y_ENSURE(query || size == 0, "SQL query is null with nonzero size");
        result->Status = Format(TString(query ? query : "", size), result->Output, result->Error)
                             ? YQL_SQL_FORMAT_OK
                             : YQL_SQL_FORMAT_ERROR;
    } catch (...) {
        result->Error = CurrentExceptionMessage();
        result->Status = YQL_SQL_FORMAT_EXCEPTION;
    }
    return result.Release();
}

} // namespace

extern "C" TYqlSqlFormatHandle* YqlSqlFormatV1(const char* query, size_t size) noexcept {
    try {
        return MakeResult(query, size);
    } catch (...) {
        // Reporting an exception can itself exhaust memory. This immutable
        // emergency result requires no allocation and still obeys the ABI.
        return &AllocationFailure;
    }
}

extern "C" void YqlSqlFormatFreeV1(TYqlSqlFormatHandle* handle) noexcept {
    if (handle != &AllocationFailure) {
        delete handle;
    }
}

extern "C" int YqlSqlFormatStatusV1(const TYqlSqlFormatHandle* handle) noexcept {
    return handle->Status;
}

extern "C" const char* YqlSqlFormatOutputV1(const TYqlSqlFormatHandle* handle, size_t* size) noexcept {
    *size = handle->Output.size();
    return handle->Output.data();
}

extern "C" const char* YqlSqlFormatErrorV1(const TYqlSqlFormatHandle* handle, size_t* size) noexcept {
    const TStringBuf error = handle == &AllocationFailure ? AllocationFailureMessage : TStringBuf(handle->Error);
    *size = error.size();
    return error.data();
}
