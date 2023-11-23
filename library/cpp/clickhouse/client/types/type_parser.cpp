#include "type_parser.h"

#include <util/string/cast.h>

namespace NClickHouse {
    static TTypeAst::EMeta GetTypeMeta(const TStringBuf& name) {
        if (name == "Array") {
            return TTypeAst::Array;
        }

        if (name == "Null") {
            return TTypeAst::Null;
        }

        if (name == "Nullable") {
            return TTypeAst::Nullable;
        }

        if (name == "Tuple") {
            return TTypeAst::Tuple;
        }

        if (name == "Enum8" || name == "Enum16") {
            return TTypeAst::Enum;
        }

        return TTypeAst::Terminal;
    }

    TTypeParser::TTypeParser(const TStringBuf& name)
        : Cur_(name.data())
        , End_(name.data() + name.size())
        , Type_(nullptr)
    {
    }

    TTypeParser::~TTypeParser() = default;

    bool TTypeParser::Parse(TTypeAst* type) {
        Type_ = type;
        OpenElements_.push(Type_);

        do {
            const TToken& TToken = NextToken();

            switch (TToken.Type) {
                case TToken::QuotedString:
                {
                    Type_->Meta = TTypeAst::Terminal;
                    if (TToken.Value.length() < 1)
                        Type_->Name = {};
                    else
                        Type_->Name = TToken.Value.substr(1, TToken.Value.length() - 2);
                    //Type_->code = Type::String;
                    break;
                }
                case TToken::Name:
                    Type_->Meta = GetTypeMeta(TToken.Value);
                    Type_->Name = TToken.Value;
                    break;
                case TToken::Number:
                    Type_->Meta = TTypeAst::Number;
                    Type_->Value = FromString<i64>(TToken.Value);
                    break;
                case TToken::LPar:
                    Type_->Elements.emplace_back(TTypeAst());
                    OpenElements_.push(Type_);
                    Type_ = &Type_->Elements.back();
                    break;
                case TToken::RPar:
                    Type_ = OpenElements_.top();
                    OpenElements_.pop();
                    break;
                case TToken::Comma:
                    Type_ = OpenElements_.top();
                    OpenElements_.pop();
                    Type_->Elements.emplace_back(TTypeAst());
                    OpenElements_.push(Type_);
                    Type_ = &Type_->Elements.back();
                    break;
                case TToken::EOS:
                    return true;
                case TToken::Invalid:
                    return false;
            }
        } while (true);
    }

    TTypeParser::TToken TTypeParser::NextToken() {
        for (; Cur_ < End_; ++Cur_) {
            switch (*Cur_) {
                case ' ':
                case '\n':
                case '\t':
                case '\0':
                case '=':
                    continue;

                case '(':
                    return TToken{TToken::LPar, TStringBuf(Cur_++, 1)};
                case ')':
                    return TToken{TToken::RPar, TStringBuf(Cur_++, 1)};
                case ',':
                    return TToken{TToken::Comma, TStringBuf(Cur_++, 1)};
                case '\'':
                {
                    const size_t end_quote_length = 1;
                    const TStringBuf end_quote{Cur_, end_quote_length};
                    // Fast forward to the closing quote.
                    const auto start = Cur_++;
                    for (; Cur_ < End_ - end_quote_length; ++Cur_) {
                        // TODO (nemkov): handle escaping ?
                        if (end_quote == TStringBuf{Cur_, end_quote_length}) {
                            Cur_ += end_quote_length;

                            return TToken{TToken::QuotedString, TStringBuf{start, Cur_}};
                        }
                    }
                    return TToken{TToken::QuotedString, TStringBuf(Cur_++, 1)};
                }

                default: {
                    const char* st = Cur_;

                    if (isalpha(*Cur_) || *Cur_ == '_') {
                        for (; Cur_ < End_; ++Cur_) {
                            if (!isalpha(*Cur_) && !isdigit(*Cur_) && *Cur_ != '_') {
                                break;
                            }
                        }

                        return TToken{TToken::Name, TStringBuf(st, Cur_)};
                    }

                    if (isdigit(*Cur_) || *Cur_ == '-') {
                        ++Cur_;
                        for (; Cur_ < End_; ++Cur_) {
                            if (!isdigit(*Cur_)) {
                                break;
                            }
                        }

                        return TToken{TToken::Number, TStringBuf(st, Cur_)};
                    }

                    return TToken{TToken::Invalid, TStringBuf()};
                }
            }
        }

        return TToken{TToken::EOS, TStringBuf()};
    }

    static TTypeRef CreateTypeFromAst(const TTypeAst& ast) {
        if (ast.Meta == TTypeAst::Terminal) {
            if (ast.Name == "UInt8")
                return TType::CreateSimple<ui8>();
            if (ast.Name == "UInt16")
                return TType::CreateSimple<ui16>();
            if (ast.Name == "UInt32")
                return TType::CreateSimple<ui32>();
            if (ast.Name == "UInt64")
                return TType::CreateSimple<ui64>();

            if (ast.Name == "Int8")
                return TType::CreateSimple<i8>();
            if (ast.Name == "Int16")
                return TType::CreateSimple<i16>();
            if (ast.Name == "Int32")
                return TType::CreateSimple<i32>();
            if (ast.Name == "Int64")
                return TType::CreateSimple<i64>();

            if (ast.Name == "Float32")
                return TType::CreateSimple<float>();
            if (ast.Name == "Float64")
                return TType::CreateSimple<double>();

            if (ast.Name == "String")
                return TType::CreateString();
            if (ast.Name == "FixedString")
                return TType::CreateString(ast.Elements.front().Value);

            if (ast.Name == "DateTime")
                return TType::CreateDateTime();
            if (ast.Name == "Date")
                return TType::CreateDate();
        } else if (ast.Meta == TTypeAst::Tuple) {
            TVector<TTypeRef> columns;

            for (const auto& elem : ast.Elements) {
                if (auto col = CreateTypeFromAst(elem)) {
                    columns.push_back(col);
                } else {
                    return nullptr;
                }
            }

            return TType::CreateTuple(columns);
        } else if (ast.Meta == TTypeAst::Array) {
            return TType::CreateArray(CreateTypeFromAst(ast.Elements.front()));
        } else if (ast.Meta == TTypeAst::Enum) {
            TVector<TEnumItem> enum_items;

            for (const auto& elem : ast.Elements) {
                TString name(elem.Name);
                i16 value = elem.Value;
                enum_items.push_back({name, value});
            }

            if (ast.Name == "Enum8") {
                return TType::CreateEnum8(enum_items);
            } else {
                return TType::CreateEnum16(enum_items);
            }
        }

        return nullptr;
    }

    TTypeRef ParseTypeFromString(const TStringBuf& type_name) {
        TTypeAst ast;

        if (TTypeParser(type_name).Parse(&ast)) {
            return CreateTypeFromAst(ast);
        }

        return TTypeRef();
    }

}
