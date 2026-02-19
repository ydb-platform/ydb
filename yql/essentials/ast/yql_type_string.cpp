#include "yql_type_string.h"
#include "yql_expr.h"
#include "yql_ast_escaping.h"

#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/string/cast.h>
#include <util/generic/map.h>
#include <util/generic/utility.h>
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

#define EXPECT_AND_SKIP_TOKEN_IMPL(token, message, result) \
    do {                                                   \
        if (Y_LIKELY(Token_ == token)) {                   \
            GetNextToken();                                \
        } else {                                           \
            AddError(message);                             \
            return result;                                 \
        }                                                  \
    } while (0);

#define EXPECT_AND_SKIP_TOKEN(token, result) \
    EXPECT_AND_SKIP_TOKEN_IMPL(token, "Expected " #token, result)

namespace NYql {
namespace {

enum EToken {
    TOKEN_EOF = -1,

    // type keywords
    TOKEN_TYPE_MIN = -2,
    TOKEN_STRING = -3,
    TOKEN_BOOL = -4,
    TOKEN_INT32 = -6,
    TOKEN_UINT32 = -7,
    TOKEN_INT64 = -8,
    TOKEN_UINT64 = -9,
    TOKEN_FLOAT = -10,
    TOKEN_DOUBLE = -11,
    TOKEN_LIST = -12,
    TOKEN_OPTIONAL = -13,
    TOKEN_DICT = -14,
    TOKEN_TUPLE = -15,
    TOKEN_STRUCT = -16,
    TOKEN_RESOURCE = -17,
    TOKEN_VOID = -18,
    TOKEN_CALLABLE = -19,
    TOKEN_TAGGED = -20,
    TOKEN_YSON = -21,
    TOKEN_UTF8 = -22,
    TOKEN_VARIANT = -23,
    TOKEN_UNIT = -24,
    TOKEN_STREAM = -25,
    TOKEN_GENERIC = -26,
    TOKEN_JSON = -27,
    TOKEN_NULL = -28,
    TOKEN_DATE = -29,
    TOKEN_DATETIME = -30,
    TOKEN_TIMESTAMP = -31,
    TOKEN_INTERVAL = -32,
    TOKEN_DECIMAL = -33,
    TOKEN_INT8 = -34,
    TOKEN_UINT8 = -35,
    TOKEN_INT16 = -36,
    TOKEN_UINT16 = -37,
    TOKEN_TZDATE = -38,
    TOKEN_TZDATETIME = -39,
    TOKEN_TZTIMESTAMP = -40,
    TOKEN_UUID = -41,
    TOKEN_FLOW = -42,
    TOKEN_SET = -43,
    TOKEN_ENUM = -44,
    TOKEN_EMPTYLIST = -45,
    TOKEN_EMPTYDICT = -46,
    TOKEN_TYPE_MAX = -47,
    TOKEN_JSON_DOCUMENT = -48,
    TOKEN_DYNUMBER = -49,
    TOKEN_SCALAR = -50,
    TOKEN_BLOCK = -51,
    TOKEN_DATE32 = -52,
    TOKEN_DATETIME64 = -53,
    TOKEN_TIMESTAMP64 = -54,
    TOKEN_INTERVAL64 = -55,
    TOKEN_TZDATE32 = -56,
    TOKEN_TZDATETIME64 = -57,
    TOKEN_TZTIMESTAMP64 = -58,
    TOKEN_MULTI = -59,
    TOKEN_ERROR = -60,
    TOKEN_LINEAR = -61,
    TOKEN_DYNAMICLINEAR = -62,

    // identifiers
    TOKEN_IDENTIFIER = -100,
    TOKEN_ESCAPED_IDENTIFIER = -101,

    // special
    TOKEN_ARROW = -200,
};

bool IsTypeKeyword(int token)
{
    return token < TOKEN_TYPE_MIN && token > TOKEN_TYPE_MAX;
}

EToken TokenTypeFromStr(TStringBuf str)
{
    static const THashMap<TStringBuf, EToken> Map = {
        {TStringBuf("String"), TOKEN_STRING},
        {TStringBuf("Bool"), TOKEN_BOOL},
        {TStringBuf("Int32"), TOKEN_INT32},
        {TStringBuf("Uint32"), TOKEN_UINT32},
        {TStringBuf("Int64"), TOKEN_INT64},
        {TStringBuf("Uint64"), TOKEN_UINT64},
        {TStringBuf("Float"), TOKEN_FLOAT},
        {TStringBuf("Double"), TOKEN_DOUBLE},
        {TStringBuf("List"), TOKEN_LIST},
        {TStringBuf("Optional"), TOKEN_OPTIONAL},
        {TStringBuf("Dict"), TOKEN_DICT},
        {TStringBuf("Tuple"), TOKEN_TUPLE},
        {TStringBuf("Struct"), TOKEN_STRUCT},
        {TStringBuf("Multi"), TOKEN_MULTI},
        {TStringBuf("Resource"), TOKEN_RESOURCE},
        {TStringBuf("Void"), TOKEN_VOID},
        {TStringBuf("Callable"), TOKEN_CALLABLE},
        {TStringBuf("Tagged"), TOKEN_TAGGED},
        {TStringBuf("Yson"), TOKEN_YSON},
        {TStringBuf("Utf8"), TOKEN_UTF8},
        {TStringBuf("Variant"), TOKEN_VARIANT},
        {TStringBuf("Unit"), TOKEN_UNIT},
        {TStringBuf("Stream"), TOKEN_STREAM},
        {TStringBuf("Generic"), TOKEN_GENERIC},
        {TStringBuf("Json"), TOKEN_JSON},
        {TStringBuf("Date"), TOKEN_DATE},
        {TStringBuf("Datetime"), TOKEN_DATETIME},
        {TStringBuf("Timestamp"), TOKEN_TIMESTAMP},
        {TStringBuf("Interval"), TOKEN_INTERVAL},
        {TStringBuf("Null"), TOKEN_NULL},
        {TStringBuf("Decimal"), TOKEN_DECIMAL},
        {TStringBuf("Int8"), TOKEN_INT8},
        {TStringBuf("Uint8"), TOKEN_UINT8},
        {TStringBuf("Int16"), TOKEN_INT16},
        {TStringBuf("Uint16"), TOKEN_UINT16},
        {TStringBuf("TzDate"), TOKEN_TZDATE},
        {TStringBuf("TzDatetime"), TOKEN_TZDATETIME},
        {TStringBuf("TzTimestamp"), TOKEN_TZTIMESTAMP},
        {TStringBuf("Uuid"), TOKEN_UUID},
        {TStringBuf("Flow"), TOKEN_FLOW},
        {TStringBuf("Set"), TOKEN_SET},
        {TStringBuf("Enum"), TOKEN_ENUM},
        {TStringBuf("EmptyList"), TOKEN_EMPTYLIST},
        {TStringBuf("EmptyDict"), TOKEN_EMPTYDICT},
        {TStringBuf("JsonDocument"), TOKEN_JSON_DOCUMENT},
        {TStringBuf("DyNumber"), TOKEN_DYNUMBER},
        {TStringBuf("Block"), TOKEN_BLOCK},
        {TStringBuf("Scalar"), TOKEN_SCALAR},
        {TStringBuf("Date32"), TOKEN_DATE32},
        {TStringBuf("Datetime64"), TOKEN_DATETIME64},
        {TStringBuf("Timestamp64"), TOKEN_TIMESTAMP64},
        {TStringBuf("Interval64"), TOKEN_INTERVAL64},
        {TStringBuf("TzDate32"), TOKEN_TZDATE32},
        {TStringBuf("TzDatetime64"), TOKEN_TZDATETIME64},
        {TStringBuf("TzTimestamp64"), TOKEN_TZTIMESTAMP64},
        {TStringBuf("Error"), TOKEN_ERROR},
        {TStringBuf("Linear"), TOKEN_LINEAR},
        {TStringBuf("DynamicLinear"), TOKEN_DYNAMICLINEAR},
    };

    auto it = Map.find(str);
    if (it != Map.end()) {
        return it->second;
    }

    return TOKEN_IDENTIFIER;
}

//////////////////////////////////////////////////////////////////////////////
// TTypeParser
//////////////////////////////////////////////////////////////////////////////
class TTypeParser {
public:
    TTypeParser(
        TStringBuf str, TIssues& issues,
        TPosition position, TMemoryPool& pool)
        : Str_(str)
        , Issues_(issues)
        , Position_(position)
        , Index_(0)
        , Pool_(pool)
    {
        GetNextToken();
    }

    TAstNode* ParseTopLevelType() {
        TAstNode* type = ParseType();
        if (type) {
            EXPECT_AND_SKIP_TOKEN_IMPL(
                TOKEN_EOF, "Expected end of string", nullptr);
        }
        return type;
    }

private:
    TAstNode* ParseType() {
        TAstNode* type = nullptr;

        switch (Token_) {
            case '(':
                return ParseCallableType();

            case TOKEN_STRING:
            case TOKEN_BOOL:
            case TOKEN_INT8:
            case TOKEN_UINT8:
            case TOKEN_INT16:
            case TOKEN_UINT16:
            case TOKEN_INT32:
            case TOKEN_UINT32:
            case TOKEN_INT64:
            case TOKEN_UINT64:
            case TOKEN_FLOAT:
            case TOKEN_DOUBLE:
            case TOKEN_YSON:
            case TOKEN_UTF8:
            case TOKEN_JSON:
            case TOKEN_DATE:
            case TOKEN_DATETIME:
            case TOKEN_TIMESTAMP:
            case TOKEN_INTERVAL:
            case TOKEN_TZDATE:
            case TOKEN_TZDATETIME:
            case TOKEN_TZTIMESTAMP:
            case TOKEN_UUID:
            case TOKEN_JSON_DOCUMENT:
            case TOKEN_DYNUMBER:
            case TOKEN_DATE32:
            case TOKEN_DATETIME64:
            case TOKEN_TIMESTAMP64:
            case TOKEN_INTERVAL64:
            case TOKEN_TZDATE32:
            case TOKEN_TZDATETIME64:
            case TOKEN_TZTIMESTAMP64:
                type = MakeDataType(Identifier_);
                GetNextToken();
                break;

            case TOKEN_DECIMAL:
                type = ParseDecimalType();
                break;

            case TOKEN_LIST:
                type = ParseListType();
                break;

            case TOKEN_OPTIONAL:
                type = ParseOptionalType();
                break;

            case TOKEN_DICT:
                type = ParseDictType();
                break;

            case TOKEN_TUPLE:
                type = ParseTupleType();
                break;

            case TOKEN_STRUCT:
                type = ParseStructType();
                break;

            case TOKEN_MULTI:
                type = ParseMultiType();
                break;

            case TOKEN_RESOURCE:
                type = ParseResourceType();
                break;

            case TOKEN_VOID:
                type = MakeVoidType();
                GetNextToken();
                break;

            case TOKEN_NULL:
                type = MakeNullType();
                GetNextToken();
                break;

            case TOKEN_EMPTYLIST:
                type = MakeEmptyListType();
                GetNextToken();
                break;

            case TOKEN_EMPTYDICT:
                type = MakeEmptyDictType();
                GetNextToken();
                break;

            case TOKEN_CALLABLE:
                type = ParseCallableTypeWithKeyword();
                break;

            case TOKEN_TAGGED:
                type = ParseTaggedType();
                break;

            case TOKEN_VARIANT:
                type = ParseVariantType();
                break;

            case TOKEN_UNIT:
                type = MakeUnitType();
                GetNextToken();
                break;

            case TOKEN_STREAM:
                type = ParseStreamType();
                break;

            case TOKEN_FLOW:
                type = ParseFlowType();
                break;

            case TOKEN_GENERIC:
                type = MakeGenericType();
                GetNextToken();
                break;

            case TOKEN_SET:
                type = ParseSetType();
                break;

            case TOKEN_ENUM:
                type = ParseEnumType();
                break;

            case TOKEN_BLOCK:
                type = ParseBlockType();
                break;

            case TOKEN_SCALAR:
                type = ParseScalarType();
                break;

            case TOKEN_ERROR:
                type = ParseErrorType();
                break;

            case TOKEN_LINEAR:
                type = ParseLinearType(false);
                break;

            case TOKEN_DYNAMICLINEAR:
                type = ParseLinearType(true);
                break;

            default:
                if (Identifier_.empty()) {
                    return AddError("Expected type");
                }

                auto id = Identifier_;
                if (id.SkipPrefix("pg")) {
                    if (NPg::HasType(TString(id))) {
                        type = MakePgType(id);
                        GetNextToken();
                    }
                } else if (id.SkipPrefix("_pg")) {
                    if (NPg::HasType(TString(id)) && !id.StartsWith('_')) {
                        type = MakePgType(TString("_") + id);
                        GetNextToken();
                    }
                }

                if (!type) {
                    return AddError(TString("Unknown type: '") + Identifier_ + "\'");
                }
        }

        if (type) {
            while (Token_ == '?') {
                type = MakeOptionalType(type);
                GetNextToken();
            }
        }
        return type;
    }

    char LookaheadNonSpaceChar() {
        size_t i = Index_;
        while (i < Str_.size() && isspace(Str_[i])) {
            i++;
        }
        return (i < Str_.size()) ? Str_[i] : -1;
    }

    int GetNextToken() {
        return Token_ = ReadNextToken();
    }

    int ReadNextToken() {
        // skip spaces
        while (!AtEnd() && isspace(Get())) {
            Move();
        }

        TokenBegin_ = Position_;
        if (AtEnd()) {
            return TOKEN_EOF;
        }

        // clear last readed indentifier
        Identifier_ = {};

        char lastChar = Get();
        if (lastChar == '_' || isalnum(lastChar)) { // identifier
            size_t start = Index_;
            while (!AtEnd()) {
                lastChar = Get();
                if (lastChar == '_' || isalnum(lastChar)) {
                    Move();
                } else {
                    break;
                }
            }

            Identifier_ = Str_.SubString(start, Index_ - start);
            return TokenTypeFromStr(Identifier_);
        } else if (lastChar == '\'') { // escaped identifier
            Move();                    // skip '\''
            if (AtEnd()) {
                return TOKEN_EOF;
            }

            UnescapedIdentifier_.clear();
            TStringOutput sout(UnescapedIdentifier_);
            TStringBuf atom = Str_.SubStr(Index_);
            size_t readBytes = 0;
            EUnescapeResult unescapeResunt =
                UnescapeArbitraryAtom(atom, '\'', &sout, &readBytes);

            if (unescapeResunt != EUnescapeResult::OK) {
                return TOKEN_EOF;
            }

            // skip already readed chars
            while (readBytes-- != 0) {
                Move();
            }

            if (AtEnd()) {
                return TOKEN_EOF;
            }

            Identifier_ = UnescapedIdentifier_;
            return TOKEN_ESCAPED_IDENTIFIER;
        } else {
            Move(); // skip last char
            if (lastChar == '-' && !AtEnd() && Get() == '>') {
                Move(); // skip '>'
                return TOKEN_ARROW;
            }
            // otherwise, just return the last character as its ascii value
            return lastChar;
        }
    }

    TAstNode* ParseCallableType() {
        EXPECT_AND_SKIP_TOKEN('(', nullptr);

        TSmallVec<TAstNode*> args;
        args.push_back(nullptr); // CallableType Atom + settings + return type
        args.push_back(nullptr);
        args.push_back(nullptr);
        bool optArgsStarted = false;
        bool namedArgsStarted = false;
        ui32 optArgsCount = 0;
        bool lastWasTypeStatement = false;

        // (1) parse argements
        for (;;) {
            if (Token_ == TOKEN_EOF) {
                if (optArgsStarted) {
                    return AddError("Expected ']'");
                }
                return AddError("Expected ')'");
            }

            if (Token_ == ']' || Token_ == ')') {
                break;
            }

            if (lastWasTypeStatement) {
                EXPECT_AND_SKIP_TOKEN(',', nullptr);
                lastWasTypeStatement = false;
            }

            if (Token_ == '[') {
                optArgsStarted = true;
                GetNextToken(); // eat '['
            } else if (Token_ == ':') {
                return AddError("Expected non empty argument name");
            } else if (IsTypeKeyword(Token_) || Token_ == '(' || // '(' - begin of callable type
                       Token_ == TOKEN_IDENTIFIER ||
                       Token_ == TOKEN_ESCAPED_IDENTIFIER)
            {
                TStringBuf argName;
                ui32 argNameFlags = TNodeFlags::Default;

                if (LookaheadNonSpaceChar() == ':') {
                    namedArgsStarted = true;
                    argName = Identifier_;

                    if (Token_ == TOKEN_ESCAPED_IDENTIFIER) {
                        argNameFlags = TNodeFlags::ArbitraryContent;
                    }

                    GetNextToken(); // eat name
                    EXPECT_AND_SKIP_TOKEN(':', nullptr);

                    if (Token_ == TOKEN_EOF) {
                        return AddError("Expected type of named argument");
                    }
                } else {
                    if (namedArgsStarted) {
                        return AddError("Expected named argument, because of "
                                        "previous argument(s) was named");
                    }
                }

                auto argType = ParseType();
                if (!argType) {
                    return nullptr;
                }
                lastWasTypeStatement = true;

                if (optArgsStarted) {
                    if (!argType->IsList() || argType->GetChildrenCount() == 0 ||
                        !argType->GetChild(0)->IsAtom() ||
                        argType->GetChild(0)->GetContent() != TStringBuf("OptionalType"))
                    {
                        return AddError("Optionals are only allowed in the optional arguments");
                    }
                    optArgsCount++;
                }

                ui32 argFlags = 0;
                if (Token_ == '{') {
                    if (!ParseCallableArgFlags(argFlags)) {
                        return nullptr;
                    }
                }

                TSmallVec<TAstNode*> argSettings;
                argSettings.push_back(argType);
                if (!argName.empty()) {
                    argSettings.push_back(MakeQuotedAtom(argName, argNameFlags));
                }
                if (argFlags) {
                    if (argName.empty()) {
                        auto atom = MakeQuotedLiteralAtom(TStringBuf(""), TNodeFlags::ArbitraryContent);
                        argSettings.push_back(atom);
                    }
                    argSettings.push_back(MakeQuotedAtom(ToString(argFlags)));
                }
                args.push_back(MakeQuote(
                    MakeList(argSettings.data(), argSettings.size())));
            } else {
                return AddError("Expected type or argument name");
            }
        }

        if (optArgsStarted) {
            EXPECT_AND_SKIP_TOKEN(']', nullptr);
        }

        EXPECT_AND_SKIP_TOKEN(')', nullptr);

        // (2) expect '->' after arguments
        EXPECT_AND_SKIP_TOKEN_IMPL(
            TOKEN_ARROW, "Expected '->' after arguments", nullptr);

        // (3) parse return type
        TAstNode* returnType = ParseType();
        if (!returnType) {
            return nullptr;
        }

        // (4) parse payload
        TStringBuf payload;
        if (Token_ == '{') {
            if (!ParseCallablePayload(payload)) {
                return nullptr;
            }
        }

        return MakeCallableType(args, optArgsCount, returnType, payload);
    }

    // { Flags: f1 | f2 | f3 }
    bool ParseCallableArgFlags(ui32& argFlags) {
        GetNextToken(); // eat '{'

        if (Token_ != TOKEN_IDENTIFIER || Identifier_ != TStringBuf("Flags")) {
            AddError("Expected Flags field");
            return false;
        }

        GetNextToken(); // eat 'Flags'
        EXPECT_AND_SKIP_TOKEN(':', false);

        for (;;) {
            if (Token_ == TOKEN_IDENTIFIER) {
                if (Identifier_ == TStringBuf("AutoMap")) {
                    argFlags |= NUdf::ICallablePayload::TArgumentFlags::AutoMap;
                } else if (Identifier_ == TStringBuf("NoYield")) {
                    argFlags |= NUdf::ICallablePayload::TArgumentFlags::NoYield;
                } else {
                    AddError(TString("Unknown flag name: ") + Identifier_);
                    return false;
                }
                GetNextToken(); // eat flag name
            } else {
                AddError("Expected flag name");
                return false;
            }

            if (Token_ == '}') {
                break;
            } else if (Token_ == '|') {
                GetNextToken(); // eat '|'
            } else {
                AddError("Expected '}' or '|'");
            }
        }

        GetNextToken(); // eat '}'
        return true;
    }

    bool ParseCallablePayload(TStringBuf& payload) {
        GetNextToken(); // eat '{'

        if (Token_ != TOKEN_IDENTIFIER && Identifier_ != TStringBuf("Payload")) {
            AddError("Expected Payload field");
            return false;
        }

        GetNextToken(); // eat 'Payload'
        EXPECT_AND_SKIP_TOKEN(':', false);

        if (Token_ == TOKEN_IDENTIFIER || Token_ == TOKEN_ESCAPED_IDENTIFIER) {
            payload = Identifier_;
            GetNextToken(); // eat payload data
        } else {
            AddError("Expected payload data");
            return false;
        }

        EXPECT_AND_SKIP_TOKEN('}', false);
        return true;
    }

    TAstNode* ParseCallableTypeWithKeyword() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto type = ParseCallableType();
        if (!type) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return type;
    }

    TAstNode* ParseListType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto itemType = ParseType();
        if (!itemType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeListType(itemType);
    }

    TAstNode* ParseStreamType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto itemType = ParseType();
        if (!itemType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeStreamType(itemType);
    }

    TAstNode* ParseFlowType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto itemType = ParseType();
        if (!itemType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeFlowType(itemType);
    }

    TAstNode* ParseBlockType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto itemType = ParseType();
        if (!itemType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeBlockType(itemType);
    }

    TAstNode* ParseScalarType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto itemType = ParseType();
        if (!itemType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeScalarType(itemType);
    }

    TAstNode* ParseErrorType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        TString file;
        if (Token_ == TOKEN_IDENTIFIER ||
            Token_ == TOKEN_ESCAPED_IDENTIFIER)
        {
            file = Identifier_;
        } else {
            return AddError("Expected file name");
        }

        GetNextToken(); // eat file name
        EXPECT_AND_SKIP_TOKEN(':', nullptr);
        ui32 line;
        if (!(Token_ == TOKEN_IDENTIFIER ||
              Token_ == TOKEN_ESCAPED_IDENTIFIER) ||
            !TryFromString(Identifier_, line)) {
            return AddError("Expected line");
        }

        GetNextToken();
        EXPECT_AND_SKIP_TOKEN(':', nullptr);
        ui32 column;
        if (!(Token_ == TOKEN_IDENTIFIER ||
              Token_ == TOKEN_ESCAPED_IDENTIFIER) ||
            !TryFromString(Identifier_, column)) {
            return AddError("Expected column");
        }

        GetNextToken();
        EXPECT_AND_SKIP_TOKEN(':', nullptr);
        TString message;
        if (Token_ == TOKEN_IDENTIFIER ||
            Token_ == TOKEN_ESCAPED_IDENTIFIER)
        {
            message = Identifier_;
        } else {
            return AddError("Expected message");
        }

        GetNextToken();
        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeErrorType(file, line, column, message);
    }

    TAstNode* ParseLinearType(bool isDynamic) {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto itemType = ParseType();
        if (!itemType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeLinearType(itemType, isDynamic);
    }

    TAstNode* ParseDecimalType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('(', nullptr);

        const auto precision = Identifier_;
        GetNextToken(); // eat keyword

        EXPECT_AND_SKIP_TOKEN(',', nullptr);

        const auto scale = Identifier_;
        GetNextToken(); // eat keyword

        EXPECT_AND_SKIP_TOKEN(')', nullptr);

        return MakeDecimalType(precision, scale);
    }

    TAstNode* ParseOptionalType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto itemType = ParseType();
        if (!itemType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeOptionalType(itemType);
    }

    TAstNode* ParseDictType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto keyType = ParseType();
        if (!keyType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN(',', nullptr);

        auto valueType = ParseType();
        if (!valueType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeDictType(keyType, valueType);
    }

    TAstNode* ParseSetType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto keyType = ParseType();
        if (!keyType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeDictType(keyType, MakeVoidType());
    }

    TAstNode* ParseTupleTypeImpl(TAstNode* (TTypeParser::*typeCreator)(TSmallVec<TAstNode*>&)) {
        TSmallVec<TAstNode*> items;
        items.push_back(nullptr); // reserve for type callable

        if (Token_ != '>') {
            for (;;) {
                auto itemType = ParseType();
                if (!itemType) {
                    return nullptr;
                }

                items.push_back(itemType);

                if (Token_ == '>') {
                    break;
                } else if (Token_ == ',') {
                    GetNextToken();
                } else {
                    return AddError("Expected '>' or ','");
                }
            }
        }

        return (this->*typeCreator)(items);
    }

    TAstNode* ParseTupleType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);
        TAstNode* tupleType = ParseTupleTypeImpl(&TTypeParser::MakeTupleType);
        if (tupleType) {
            EXPECT_AND_SKIP_TOKEN('>', nullptr);
        }
        return tupleType;
    }

    TAstNode* ParseStructTypeImpl() {
        TMap<TString, TAstNode*> members;
        if (Token_ != '>') {
            for (;;) {
                TString name;
                if (Token_ == TOKEN_IDENTIFIER ||
                    Token_ == TOKEN_ESCAPED_IDENTIFIER)
                {
                    name = Identifier_;
                } else {
                    return AddError("Expected struct member name");
                }

                if (name.empty()) {
                    return AddError("Empty name is not allowed");
                } else if (members.contains(name)) {
                    return AddError("Member name duplication");
                }

                GetNextToken(); // eat member name
                EXPECT_AND_SKIP_TOKEN(':', nullptr);

                auto type = ParseType();
                if (!type) {
                    return nullptr;
                }

                members.emplace(std::move(name), type);

                if (Token_ == '>') {
                    break;
                } else if (Token_ == ',') {
                    GetNextToken();
                } else {
                    return AddError("Expected '>' or ','");
                }
            }
        }

        return MakeStructType(members);
    }

    TAstNode* ParseStructType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);
        TAstNode* structType = ParseStructTypeImpl();
        if (structType) {
            EXPECT_AND_SKIP_TOKEN('>', nullptr);
        }
        return structType;
    }

    TAstNode* ParseMultiType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);
        TAstNode* tupleType = ParseTupleTypeImpl(&TTypeParser::MakeMultiType);
        if (tupleType) {
            EXPECT_AND_SKIP_TOKEN('>', nullptr);
        }
        return tupleType;
    }

    TAstNode* ParseVariantType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        TAstNode* underlyingType = nullptr;
        if (Token_ == TOKEN_IDENTIFIER || Token_ == TOKEN_ESCAPED_IDENTIFIER) {
            underlyingType = ParseStructTypeImpl();
        } else if (IsTypeKeyword(Token_) || Token_ == '(') {
            underlyingType = ParseTupleTypeImpl(&TTypeParser::MakeTupleType);
        } else {
            return AddError("Expected type");
        }

        if (!underlyingType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeVariantType(underlyingType);
    }

    TAstNode* ParseEnumType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        TMap<TString, TAstNode*> members;
        for (;;) {
            TString name;
            if (Token_ == TOKEN_IDENTIFIER ||
                Token_ == TOKEN_ESCAPED_IDENTIFIER)
            {
                name = Identifier_;
            } else {
                return AddError("Expected name");
            }

            if (name.empty()) {
                return AddError("Empty name is not allowed");
            } else if (members.contains(name)) {
                return AddError("Member name duplication");
            }

            GetNextToken(); // eat member name
            members.emplace(std::move(name), MakeVoidType());

            if (Token_ == '>') {
                break;
            } else if (Token_ == ',') {
                GetNextToken();
            } else {
                return AddError("Expected '>' or ','");
            }
        }

        auto underlyingType = MakeStructType(members);
        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeVariantType(underlyingType);
    }

    TAstNode* MakeCallableType(
        TSmallVec<TAstNode*>& args, size_t optionalArgsCount,
        TAstNode* returnType, TStringBuf payload)
    {
        args[0] = MakeLiteralAtom(TStringBuf("CallableType"));
        TSmallVec<TAstNode*> mainSettings;
        if (optionalArgsCount || !payload.empty()) {
            mainSettings.push_back(optionalArgsCount
                                       ? MakeQuotedAtom(ToString(optionalArgsCount))
                                       : MakeQuotedLiteralAtom(TStringBuf("0")));
        }

        if (!payload.empty()) {
            mainSettings.push_back(MakeQuotedAtom(payload, TNodeFlags::ArbitraryContent));
        }

        args[1] = MakeQuote(MakeList(mainSettings.data(), mainSettings.size()));

        TSmallVec<TAstNode*> returnSettings;
        returnSettings.push_back(returnType);
        args[2] = MakeQuote(MakeList(returnSettings.data(), returnSettings.size()));

        return MakeList(args.data(), args.size());
    }

    TAstNode* MakeListType(TAstNode* itemType) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("ListType")),
            itemType,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeStreamType(TAstNode* itemType) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("StreamType")),
            itemType,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeFlowType(TAstNode* itemType) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("FlowType")),
            itemType,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeBlockType(TAstNode* itemType) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("BlockType")),
            itemType,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeScalarType(TAstNode* itemType) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("ScalarType")),
            itemType,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeErrorType(TStringBuf file, ui32 row, ui32 column, TStringBuf message) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("ErrorType")),
            MakeQuotedAtom(ToString(row)),
            MakeQuotedAtom(ToString(column)),
            MakeQuotedAtom(file, TNodeFlags::ArbitraryContent),
            MakeQuotedAtom(message, TNodeFlags::ArbitraryContent),
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeVariantType(TAstNode* underlyingType) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("VariantType")),
            underlyingType,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeDictType(TAstNode* keyType, TAstNode* valueType) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("DictType")),
            keyType,
            valueType,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeTupleType(TSmallVec<TAstNode*>& items) {
        items[0] = MakeLiteralAtom(TStringBuf("TupleType"));
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeStructType(const TMap<TString, TAstNode*>& members) {
        TSmallVec<TAstNode*> items;
        items.push_back(MakeLiteralAtom(TStringBuf("StructType")));

        for (const auto& member : members) {
            auto memberType = std::to_array<TAstNode*>({
                MakeQuotedAtom(member.first, TNodeFlags::ArbitraryContent), // name
                member.second,                                              // type
            });
            items.push_back(MakeQuote(MakeList(memberType.data(), memberType.size())));
        }

        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeMultiType(TSmallVec<TAstNode*>& items) {
        items[0] = MakeLiteralAtom(TStringBuf("MultiType"));
        return MakeList(items.data(), items.size());
    }

    TAstNode* ParseResourceType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        if (Token_ != TOKEN_IDENTIFIER && Token_ != TOKEN_ESCAPED_IDENTIFIER) {
            return AddError("Expected resource tag");
        }

        TStringBuf tag = Identifier_;
        if (tag.empty()) {
            return AddError("Expected non empty resource tag");
        }

        GetNextToken(); // eat tag
        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeResourceType(tag);
    }

    TAstNode* ParseTaggedType() {
        GetNextToken(); // eat keyword
        EXPECT_AND_SKIP_TOKEN('<', nullptr);

        auto baseType = ParseType();
        if (!baseType) {
            return nullptr;
        }

        EXPECT_AND_SKIP_TOKEN(',', nullptr);

        if (Token_ != TOKEN_IDENTIFIER && Token_ != TOKEN_ESCAPED_IDENTIFIER) {
            return AddError("Expected tag of type");
        }

        TStringBuf tag = Identifier_;
        if (tag.empty()) {
            return AddError("Expected non empty tag of type");
        }

        GetNextToken(); // eat tag
        EXPECT_AND_SKIP_TOKEN('>', nullptr);
        return MakeTaggedType(baseType, tag);
    }

    TAstNode* MakeResourceType(TStringBuf tag) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("ResourceType")),
            MakeQuotedAtom(tag),
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeVoidType() {
        auto items = std::to_array<TAstNode*>({MakeLiteralAtom(TStringBuf("VoidType"))});
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeNullType() {
        auto items = std::to_array<TAstNode*>({MakeLiteralAtom(TStringBuf("NullType"))});
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeEmptyListType() {
        auto items = std::to_array<TAstNode*>({MakeLiteralAtom(TStringBuf("EmptyListType"))});
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeEmptyDictType() {
        auto items = std::to_array<TAstNode*>({MakeLiteralAtom(TStringBuf("EmptyDictType"))});
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeUnitType() {
        auto items = std::to_array<TAstNode*>({MakeLiteralAtom(TStringBuf("UnitType"))});
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeGenericType() {
        auto items = std::to_array<TAstNode*>({MakeLiteralAtom(TStringBuf("GenericType"))});
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeTaggedType(TAstNode* baseType, TStringBuf tag) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("TaggedType")),
            baseType,
            MakeQuotedAtom(tag),
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeDataType(TStringBuf type) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("DataType")),
            MakeQuotedAtom(type),
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakePgType(TStringBuf type) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("PgType")),
            MakeQuotedAtom(type),
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeDecimalType(TStringBuf precision, TStringBuf scale) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("DataType")),
            MakeQuotedAtom(TStringBuf("Decimal")),
            MakeQuotedAtom(precision),
            MakeQuotedAtom(scale),
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeOptionalType(TAstNode* type) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(TStringBuf("OptionalType")),
            type,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeLinearType(TAstNode* type, bool isDynamic) {
        auto items = std::to_array<TAstNode*>({
            MakeLiteralAtom(isDynamic ? TStringBuf("DynamicLinearType") : TStringBuf("LinearType")),
            type,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeAtom(TStringBuf content, ui32 flags = TNodeFlags::Default) {
        return TAstNode::NewAtom(Position_, content, Pool_, flags);
    }

    TAstNode* MakeLiteralAtom(TStringBuf content, ui32 flags = TNodeFlags::Default) {
        return TAstNode::NewLiteralAtom(Position_, content, Pool_, flags);
    }

    TAstNode* MakeQuote(TAstNode* node) {
        auto items = std::to_array<TAstNode*>({
            &TAstNode::QuoteAtom,
            node,
        });
        return MakeList(items.data(), items.size());
    }

    TAstNode* MakeQuotedAtom(TStringBuf content, ui32 flags = TNodeFlags::Default) {
        return MakeQuote(MakeAtom(content, flags));
    }

    TAstNode* MakeQuotedLiteralAtom(TStringBuf content, ui32 flags = TNodeFlags::Default) {
        return MakeQuote(MakeLiteralAtom(content, flags));
    }

    TAstNode* MakeList(TAstNode** children, ui32 count) {
        return TAstNode::NewList(Position_, children, count, Pool_);
    }

    char Get() const {
        return Str_[Index_];
    }

    bool AtEnd() const {
        return Index_ >= Str_.size();
    }

    void Move() {
        if (AtEnd()) {
            return;
        }

        ++Index_;
        ++Position_.Column;

        if (!AtEnd() && Str_[Index_] == '\n') {
            Position_.Row++;
            Position_.Column = 1;
        }
    }

    TAstNode* AddError(const TString& message) {
        Issues_.AddIssue(TIssue(TokenBegin_, message));
        return nullptr;
    }

private:
    TStringBuf Str_;
    TIssues& Issues_;
    TPosition TokenBegin_, Position_;
    size_t Index_;
    int Token_;
    TString UnescapedIdentifier_;
    TStringBuf Identifier_;
    TMemoryPool& Pool_;
};

//////////////////////////////////////////////////////////////////////////////
// TTypePrinter
//////////////////////////////////////////////////////////////////////////////
class TTypePrinter: public TTypeAnnotationVisitor {
public:
    explicit TTypePrinter(IOutputStream& out)
        : Out_(out)
    {
    }

private:
    void Visit(const TUnitExprType& type) final {
        TopLevel_ = false;
        Y_UNUSED(type);
        Out_ << TStringBuf("Unit");
    }

    void Visit(const TUniversalExprType& type) final {
        TopLevel_ = false;
        Y_UNUSED(type);
        Out_ << TStringBuf("Universal");
    }

    void Visit(const TUniversalStructExprType& type) final {
        TopLevel_ = false;
        Y_UNUSED(type);
        Out_ << TStringBuf("UniversalStruct");
    }

    void Visit(const TMultiExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Multi<");
        const auto& items = type.GetItems();
        for (ui32 i = 0; i < items.size(); ++i) {
            if (i) {
                Out_ << ',';
            }
            items[i]->Accept(*this);
        }
        Out_ << '>';
    }

    void Visit(const TTupleExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Tuple<");
        const auto& items = type.GetItems();
        for (ui32 i = 0; i < items.size(); ++i) {
            if (i) {
                Out_ << ',';
            }
            items[i]->Accept(*this);
        }
        Out_ << '>';
    }

    void Visit(const TStructExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Struct<");
        const auto& items = type.GetItems();
        for (ui32 i = 0; i < items.size(); ++i) {
            if (i) {
                Out_ << ',';
            }
            items[i]->Accept(*this);
        }
        Out_ << '>';
    }

    void Visit(const TItemExprType& type) final {
        TopLevel_ = false;
        EscapeArbitraryAtom(type.GetName(), '\'', &Out_);
        Out_ << ':';
        type.GetItemType()->Accept(*this);
    }

    void Visit(const TListExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("List<");
        type.GetItemType()->Accept(*this);
        Out_ << '>';
    }

    void Visit(const TStreamExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Stream<");
        type.GetItemType()->Accept(*this);
        Out_ << '>';
    }

    void Visit(const TFlowExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Flow<");
        type.GetItemType()->Accept(*this);
        Out_ << '>';
    }

    void Visit(const TBlockExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Block<");
        type.GetItemType()->Accept(*this);
        Out_ << '>';
    }

    void Visit(const TScalarExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Scalar<");
        type.GetItemType()->Accept(*this);
        Out_ << '>';
    }

    void Visit(const TLinearExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Linear<");
        type.GetItemType()->Accept(*this);
        Out_ << '>';
    }

    void Visit(const TDynamicLinearExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("DynamicLinear<");
        type.GetItemType()->Accept(*this);
        Out_ << '>';
    }

    void Visit(const TDataExprType& type) final {
        TopLevel_ = false;
        Out_ << type.GetName();
        if (const auto dataExprParamsType = dynamic_cast<const TDataExprParamsType*>(&type)) {
            Out_ << '(' << dataExprParamsType->GetParamOne() << ',' << dataExprParamsType->GetParamTwo() << ')';
        }
    }

    void Visit(const TPgExprType& type) final {
        TopLevel_ = false;
        TStringBuf name = type.GetName();
        if (!name.SkipPrefix("_")) {
            Out_ << "pg" << name;
        } else {
            Out_ << "_pg" << name;
        }
    }

    void Visit(const TWorldExprType& type) final {
        Y_UNUSED(type);
        TopLevel_ = false;
        Out_ << TStringBuf("World");
    }

    void Visit(const TOptionalExprType& type) final {
        const TTypeAnnotationNode* itemType = type.GetItemType();
        if (TopLevel_ || itemType->GetKind() == ETypeAnnotationKind::Callable) {
            TopLevel_ = false;
            Out_ << TStringBuf("Optional<");
            itemType->Accept(*this);
            Out_ << '>';
        } else {
            TopLevel_ = false;
            itemType->Accept(*this);
            Out_ << '?';
        }
    }

    void Visit(const TCallableExprType& type) final {
        TopLevel_ = false;
        const auto& args = type.GetArguments();
        ui32 argsCount = type.GetArgumentsSize();
        ui32 optArgsCount =
            Min<ui32>(type.GetOptionalArgumentsCount(), argsCount);

        Out_ << TStringBuf("Callable<(");
        for (ui32 i = 0; i < argsCount; ++i) {
            if (i) {
                Out_ << ',';
            }
            if (i == argsCount - optArgsCount) {
                Out_ << '[';
            }
            const TCallableExprType::TArgumentInfo& argInfo = args[i];
            if (!argInfo.Name.empty()) {
                EscapeArbitraryAtom(argInfo.Name, '\'', &Out_);
                Out_ << ':';
            }
            argInfo.Type->Accept(*this);
            if (argInfo.Flags) {
                Out_ << TStringBuf("{Flags:");
                bool start = true;
                if (argInfo.Flags & NUdf::ICallablePayload::TArgumentFlags::AutoMap) {
                    if (!start) {
                        Out_ << '|';
                    }

                    Out_ << TStringBuf("AutoMap");
                    start = false;
                }
                if (argInfo.Flags & NUdf::ICallablePayload::TArgumentFlags::NoYield) {
                    if (!start) {
                        Out_ << '|';
                    }

                    Out_ << TStringBuf("NoYield");
                    start = false;
                }
                Out_ << '}';
            }
        }

        if (optArgsCount > 0) {
            Out_ << ']';
        }

        Out_ << TStringBuf(")->");
        type.GetReturnType()->Accept(*this);
        if (!type.GetPayload().empty()) {
            Out_ << TStringBuf("{Payload:") << type.GetPayload() << '}';
        }
        Out_ << '>';
    }

    void Visit(const TResourceExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Resource<");
        EscapeArbitraryAtom(type.GetTag(), '\'', &Out_);
        Out_ << '>';
    }

    void Visit(const TTypeExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Type<");
        type.GetType()->Accept(*this);
        Out_ << '>';
    }

    void Visit(const TDictExprType& type) final {
        TopLevel_ = false;
        if (type.GetPayloadType()->GetKind() == ETypeAnnotationKind::Void) {
            Out_ << TStringBuf("Set<");
            type.GetKeyType()->Accept(*this);
            Out_ << '>';
        } else {
            Out_ << TStringBuf("Dict<");
            type.GetKeyType()->Accept(*this);
            Out_ << ',';
            type.GetPayloadType()->Accept(*this);
            Out_ << '>';
        }
    }

    void Visit(const TVoidExprType& type) final {
        Y_UNUSED(type);
        TopLevel_ = false;
        Out_ << TStringBuf("Void");
    }

    void Visit(const TNullExprType& type) final {
        Y_UNUSED(type);
        TopLevel_ = false;
        Out_ << TStringBuf("Null");
    }

    void Visit(const TEmptyListExprType& type) final {
        Y_UNUSED(type);
        TopLevel_ = false;
        Out_ << TStringBuf("EmptyList");
    }

    void Visit(const TEmptyDictExprType& type) final {
        Y_UNUSED(type);
        TopLevel_ = false;
        Out_ << TStringBuf("EmptyDict");
    }

    void Visit(const TGenericExprType& type) final {
        Y_UNUSED(type);
        TopLevel_ = false;
        Out_ << TStringBuf("Generic");
    }

    void Visit(const TTaggedExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Tagged<");
        type.GetBaseType()->Accept(*this);
        Out_ << ',';
        EscapeArbitraryAtom(type.GetTag(), '\'', &Out_);
        Out_ << '>';
    }

    void Visit(const TErrorExprType& type) final {
        TopLevel_ = false;
        Out_ << TStringBuf("Error<");
        auto pos = type.GetError().Position;
        EscapeArbitraryAtom(pos.File.empty() ? "<main>" : pos.File, '\'', &Out_);
        Out_ << ':';
        if (pos) {
            Out_ << pos.Row << ':' << pos.Column << ':';
        }

        EscapeArbitraryAtom(type.GetError().GetMessage(), '\'', &Out_);
        Out_ << '>';
    }

    void Visit(const TVariantExprType& type) final {
        TopLevel_ = false;
        auto underlyingType = type.GetUnderlyingType();
        if (underlyingType->GetKind() == ETypeAnnotationKind::Tuple) {
            Out_ << TStringBuf("Variant<");
            auto tupleType = underlyingType->Cast<TTupleExprType>();
            const auto& items = tupleType->GetItems();
            for (ui32 i = 0; i < items.size(); ++i) {
                if (i) {
                    Out_ << ',';
                }
                items[i]->Accept(*this);
            }
        } else {
            auto srtuctType = underlyingType->Cast<TStructExprType>();
            const auto& items = srtuctType->GetItems();
            bool allVoid = true;
            for (ui32 i = 0; i < items.size(); ++i) {
                allVoid = allVoid && (items[i]->GetItemType()->GetKind() == ETypeAnnotationKind::Void);
            }

            Out_ << (allVoid ? TStringBuf("Enum<") : TStringBuf("Variant<"));
            for (ui32 i = 0; i < items.size(); ++i) {
                if (i) {
                    Out_ << ',';
                }

                if (allVoid) {
                    EscapeArbitraryAtom(items[i]->GetName(), '\'', &Out_);
                } else {
                    items[i]->Accept(*this);
                }
            }
        }

        Out_ << '>';
    }

private:
    IOutputStream& Out_;
    bool TopLevel_ = true;
};

} // namespace

TAstNode* ParseType(TStringBuf str, TMemoryPool& pool, TIssues& issues,
                    TPosition position /* = TPosition(1, 1) */)
{
    TTypeParser parser(str, issues, position, pool);
    return parser.ParseTopLevelType();
}

TString FormatType(const TTypeAnnotationNode* typeNode)
{
    TStringStream ss;
    TTypePrinter printer(ss);
    typeNode->Accept(printer);
    return ss.Str();
}

} // namespace NYql
