#include "obfuscate.h"

#include <yql/essentials/core/sql_types/simple_types.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/util/simple_reflection.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/builder.h>
#include <util/string/escape.h>

#include <functional>

namespace NKikimr::NKqp {

namespace {

using namespace NSQLv1Generated;

// Scopes for AST identifier classification (mirrors sql_format.cpp logic)
enum class EScope {
    Default,
    TypeName,
    Identifier,
    DoubleQuestion
};

class TMappingObfuscatingVisitor;
using TMappingObfuscatingFunctor = std::function<void(TMappingObfuscatingVisitor&, const NProtoBuf::Message& msg)>;

template <typename T, void (T::*Func)(const NProtoBuf::Message&)>
void VisitAllFieldsImpl(T* obj, const NProtoBuf::Descriptor* descr, const NProtoBuf::Message& msg) {
    for (int i = 0; i < descr->field_count(); ++i) {
        const NProtoBuf::FieldDescriptor* fd = descr->field(i);
        NProtoBuf::TConstField field(msg, fd);
        if (field.IsMessage()) {
            for (size_t j = 0; j < field.Size(); ++j) {
                (obj->*Func)(*field.template Get<NProtoBuf::Message>(j));
            }
        }
    }
}

struct TMappingStaticData {
    TMappingStaticData();
    static const TMappingStaticData& GetInstance() {
        return *Singleton<TMappingStaticData>();
    }

    THashMap<const NProtoBuf::Descriptor*, EScope> ScopeDispatch;
    THashMap<const NProtoBuf::Descriptor*, TMappingObfuscatingFunctor> VisitDispatch;
};

class TMappingObfuscatingVisitor {
    friend struct TMappingStaticData;

public:
    explicit TMappingObfuscatingVisitor(const THashMap<TString, TString>& mapping)
        : StaticData_(TMappingStaticData::GetInstance())
        , Mapping_(mapping)
    {
    }

    TString Process(const NProtoBuf::Message& msg) {
        Scopes_.push_back(EScope::Default);
        Visit(msg);
        return Sb_;
    }

private:
    void VisitToken(const TToken& token) {
        auto str = token.GetValue();
        if (str == "<EOF>") {
            return;
        }

        if (!First_) {
            Sb_ << ' ';
        } else {
            First_ = false;
        }

        if (str == "$" && FuncCall_) {
            FuncCall_ = false;
        }

        if (Scopes_.back() == EScope::Identifier && !FuncCall_) {
            if (str == "$") {
                Sb_ << str;
                AfterDollar_ = true;
            } else if (NYql::LookupSimpleTypeBySqlAlias(str, true)) {
                Sb_ << str;
                AfterDollar_ = false;
            } else if (AfterDollar_) {
                // This is a bind parameter name (token after '$').
                // Assign a unique obfuscated name so different parameters stay distinct.
                AfterDollar_ = false;
                auto it = ParamMapping_.find(str);
                if (it != ParamMapping_.end()) {
                    Sb_ << it->second;
                } else {
                    TString paramName = TStringBuilder() << "param_" << ParamCounter_++;
                    ParamMapping_[str] = paramName;
                    Sb_ << paramName;
                }
            } else {
                // Regular identifier: look up in the metadata mapping.
                // Handle backtick-quoted identifiers: strip backticks for lookup.
                AfterDollar_ = false;
                TString lookupStr = str;
                bool wasBacktickQuoted = false;
                if (lookupStr.size() >= 2 && lookupStr.front() == '`' && lookupStr.back() == '`') {
                    lookupStr = lookupStr.substr(1, lookupStr.size() - 2);
                    wasBacktickQuoted = true;
                }

                auto it = Mapping_.find(lookupStr);
                if (it != Mapping_.end()) {
                    if (wasBacktickQuoted) {
                        Sb_ << '`' << it->second << '`';
                    } else {
                        Sb_ << it->second;
                    }
                } else {
                    Sb_ << "id";
                }
            }
        } else if (NextToken_) {
            Sb_ << *NextToken_;
            NextToken_ = Nothing();
        } else {
            Sb_ << str;
        }
    }

    void VisitPragmaValue(const TRule_pragma_value& msg) {
        switch (msg.Alt_case()) {
            case TRule_pragma_value::kAltPragmaValue1:
                NextToken_ = "0";
                break;
            case TRule_pragma_value::kAltPragmaValue3:
                NextToken_ = "'str'";
                break;
            case TRule_pragma_value::kAltPragmaValue4:
                NextToken_ = "false";
                break;
            default:
                break;
        }
        VisitAllFields(TRule_pragma_value::GetDescriptor(), msg);
    }

    void VisitLiteralValue(const TRule_literal_value& msg) {
        switch (msg.Alt_case()) {
            case TRule_literal_value::kAltLiteralValue1:
                NextToken_ = "0";
                break;
            case TRule_literal_value::kAltLiteralValue2:
                NextToken_ = "0.0";
                break;
            case TRule_literal_value::kAltLiteralValue3:
                NextToken_ = "'str'";
                break;
            case TRule_literal_value::kAltLiteralValue9:
                NextToken_ = "false";
                break;
            default:
                break;
        }
        VisitAllFields(TRule_literal_value::GetDescriptor(), msg);
    }

    void VisitAtomExpr(const TRule_atom_expr& msg) {
        if (msg.Alt_case() == TRule_atom_expr::kAltAtomExpr7) {
            FuncCall_ = true;
        }
        VisitAllFields(TRule_atom_expr::GetDescriptor(), msg);
        FuncCall_ = false;
    }

    void VisitInAtomExpr(const TRule_in_atom_expr& msg) {
        if (msg.Alt_case() == TRule_in_atom_expr::kAltInAtomExpr6) {
            FuncCall_ = true;
        }
        VisitAllFields(TRule_in_atom_expr::GetDescriptor(), msg);
        FuncCall_ = false;
    }

    void VisitUnaryCasualSubexpr(const TRule_unary_casual_subexpr& msg) {
        bool invoke = false;
        for (auto& b : msg.GetRule_unary_subexpr_suffix2().GetBlock1()) {
            if (b.GetBlock1().Alt_case() == TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt2) {
                invoke = true;
            }
            break;
        }

        if (invoke) {
            FuncCall_ = true;
        }
        Visit(msg.GetBlock1());
        if (invoke) {
            FuncCall_ = false;
        }
        Visit(msg.GetRule_unary_subexpr_suffix2());
    }

    void VisitInUnaryCasualSubexpr(const TRule_in_unary_casual_subexpr& msg) {
        bool invoke = false;
        for (auto& b : msg.GetRule_unary_subexpr_suffix2().GetBlock1()) {
            if (b.GetBlock1().Alt_case() == TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt2) {
                invoke = true;
            }
            break;
        }

        if (invoke) {
            FuncCall_ = true;
        }
        Visit(msg.GetBlock1());
        if (invoke) {
            FuncCall_ = false;
        }
        Visit(msg.GetRule_unary_subexpr_suffix2());
    }

    void Visit(const NProtoBuf::Message& msg) {
        const NProtoBuf::Descriptor* descr = msg.GetDescriptor();
        auto scopePtr = StaticData_.ScopeDispatch.FindPtr(descr);
        if (scopePtr) {
            Scopes_.push_back(*scopePtr);
        }

        auto funcPtr = StaticData_.VisitDispatch.FindPtr(descr);
        if (funcPtr) {
            (*funcPtr)(*this, msg);
        } else {
            VisitAllFields(descr, msg);
        }

        if (scopePtr) {
            Scopes_.pop_back();
        }
    }

    void VisitAllFields(const NProtoBuf::Descriptor* descr, const NProtoBuf::Message& msg) {
        VisitAllFieldsImpl<TMappingObfuscatingVisitor, &TMappingObfuscatingVisitor::Visit>(this, descr, msg);
    }

    const TMappingStaticData& StaticData_;
    const THashMap<TString, TString>& Mapping_;
    TStringBuilder Sb_;
    bool First_ = true;
    TMaybe<TString> NextToken_;
    TVector<EScope> Scopes_;
    bool FuncCall_ = false;
    bool AfterDollar_ = false;
    THashMap<TString, TString> ParamMapping_;
    ui32 ParamCounter_ = 0;
};

template <typename T>
TMappingObfuscatingFunctor MakeMappingFunctor(void (TMappingObfuscatingVisitor::*memberPtr)(const T& msg)) {
    return [memberPtr](TMappingObfuscatingVisitor& visitor, const NProtoBuf::Message& rawMsg) {
        (visitor.*memberPtr)(dynamic_cast<const T&>(rawMsg));
    };
}

TMappingStaticData::TMappingStaticData()
    : ScopeDispatch({
          {TRule_type_name::GetDescriptor(), EScope::TypeName},
          {TRule_type_name_composite::GetDescriptor(), EScope::TypeName},
          {TRule_double_question::GetDescriptor(), EScope::DoubleQuestion},
          {TRule_id::GetDescriptor(), EScope::Identifier},
          {TRule_id_or_type::GetDescriptor(), EScope::Identifier},
          {TRule_id_schema::GetDescriptor(), EScope::Identifier},
          {TRule_id_expr::GetDescriptor(), EScope::Identifier},
          {TRule_id_expr_in::GetDescriptor(), EScope::Identifier},
          {TRule_id_window::GetDescriptor(), EScope::Identifier},
          {TRule_id_table::GetDescriptor(), EScope::Identifier},
          {TRule_id_without::GetDescriptor(), EScope::Identifier},
          {TRule_id_hint::GetDescriptor(), EScope::Identifier},
          {TRule_identifier::GetDescriptor(), EScope::Identifier},
          {TRule_id_table_or_type::GetDescriptor(), EScope::Identifier},
          {TRule_bind_parameter::GetDescriptor(), EScope::Identifier},
          {TRule_an_id_as_compat::GetDescriptor(), EScope::Identifier},
      })
    , VisitDispatch({
          {TToken::GetDescriptor(), MakeMappingFunctor(&TMappingObfuscatingVisitor::VisitToken)},
          {TRule_literal_value::GetDescriptor(), MakeMappingFunctor(&TMappingObfuscatingVisitor::VisitLiteralValue)},
          {TRule_pragma_value::GetDescriptor(), MakeMappingFunctor(&TMappingObfuscatingVisitor::VisitPragmaValue)},
          {TRule_atom_expr::GetDescriptor(), MakeMappingFunctor(&TMappingObfuscatingVisitor::VisitAtomExpr)},
          {TRule_in_atom_expr::GetDescriptor(), MakeMappingFunctor(&TMappingObfuscatingVisitor::VisitInAtomExpr)},
          {TRule_unary_casual_subexpr::GetDescriptor(), MakeMappingFunctor(&TMappingObfuscatingVisitor::VisitUnaryCasualSubexpr)},
          {TRule_in_unary_casual_subexpr::GetDescriptor(), MakeMappingFunctor(&TMappingObfuscatingVisitor::VisitInUnaryCasualSubexpr)},
      })
{
}

// Extract the short name (last path component) from a full table path like "/Root/mydb/orders"
TString ExtractShortName(const TString& fullPath) {
    auto pos = fullPath.rfind('/');
    if (pos == TString::npos) {
        return fullPath;
    }
    return fullPath.substr(pos + 1);
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// TReplayMessageObfuscator
// ---------------------------------------------------------------------------

TReplayMessageObfuscator::TReplayMessageObfuscator() = default;

TString TReplayMessageObfuscator::GetOrCreateTableName(const TString& original) {
    if (original.empty()) {
        return original;
    }
    auto it = Mapping_.find(original);
    if (it != Mapping_.end()) {
        return it->second;
    }
    TString obfuscated = TStringBuilder() << "table_" << TableCounter_++;
    Mapping_[original] = obfuscated;
    return obfuscated;
}

TString TReplayMessageObfuscator::GetOrCreateColumnName(const TString& original) {
    if (original.empty()) {
        return original;
    }
    auto it = Mapping_.find(original);
    if (it != Mapping_.end()) {
        return it->second;
    }
    TString obfuscated = TStringBuilder() << "column_" << ColumnCounter_++;
    Mapping_[original] = obfuscated;
    return obfuscated;
}

TString TReplayMessageObfuscator::GetOrCreateIndexName(const TString& original) {
    if (original.empty()) {
        return original;
    }
    auto it = Mapping_.find(original);
    if (it != Mapping_.end()) {
        return it->second;
    }
    TString obfuscated = TStringBuilder() << "index_" << IndexCounter_++;
    Mapping_[original] = obfuscated;
    return obfuscated;
}

TString TReplayMessageObfuscator::MapIdentifier(const TString& original) const {
    auto it = Mapping_.find(original);
    if (it != Mapping_.end()) {
        return it->second;
    }
    return {};
}

void TReplayMessageObfuscator::AddMapping(const TString& original, const TString& obfuscated) {
    Mapping_[original] = obfuscated;
}

const THashMap<TString, TString>& TReplayMessageObfuscator::GetMapping() const {
    return Mapping_;
}

void TReplayMessageObfuscator::AddTableMetadataImpl(const NKikimrKqp::TKqpTableMetadataProto& meta) {
    // Map the full table name
    const auto& fullName = meta.GetName();
    TString obfuscatedTableName = GetOrCreateTableName(fullName);

    // Also map the short name (last path component) to the same obfuscated name
    TString shortName = ExtractShortName(fullName);
    if (shortName != fullName && !shortName.empty()) {
        if (!Mapping_.contains(shortName)) {
            Mapping_[shortName] = obfuscatedTableName;
        }
    }

    // Map column names
    for (const auto& col : meta.GetColumns()) {
        GetOrCreateColumnName(col.GetName());
    }

    // Map key column names (should already be mapped from Columns, but ensure)
    for (const auto& keyCol : meta.GetKeyColunmNames()) {
        GetOrCreateColumnName(keyCol);
    }

    // Map indexes
    for (const auto& idx : meta.GetIndexes()) {
        GetOrCreateIndexName(idx.GetName());

        for (const auto& keyCol : idx.GetKeyColumns()) {
            GetOrCreateColumnName(keyCol);
        }
        for (const auto& dataCol : idx.GetDataColumns()) {
            GetOrCreateColumnName(dataCol);
        }
    }

    // Recurse into secondary global index metadata
    for (const auto& secondaryMeta : meta.GetSecondaryGlobalIndexMetadata()) {
        AddTableMetadataImpl(secondaryMeta);
    }
}

void TReplayMessageObfuscator::AddTableMetadata(const NKikimrKqp::TKqpTableMetadataProto& meta) {
    AddTableMetadataImpl(meta);
}

NKikimrKqp::TKqpTableMetadataProto TReplayMessageObfuscator::ObfuscateMetadata(
    const NKikimrKqp::TKqpTableMetadataProto& meta) const
{
    NKikimrKqp::TKqpTableMetadataProto result(meta);

    // Obfuscate table name
    TString mapped = MapIdentifier(meta.GetName());
    if (!mapped.empty()) {
        result.SetName(mapped);
    }

    // Obfuscate cluster
    mapped = MapIdentifier(meta.GetCluster());
    if (!mapped.empty()) {
        result.SetCluster(mapped);
    }

    // Obfuscate columns
    for (int i = 0; i < result.MutableColumns()->size(); ++i) {
        auto* col = result.MutableColumns(i);
        mapped = MapIdentifier(col->GetName());
        if (!mapped.empty()) {
            col->SetName(mapped);
        }
    }

    // Obfuscate key column names
    for (int i = 0; i < result.MutableKeyColunmNames()->size(); ++i) {
        mapped = MapIdentifier(result.GetKeyColunmNames(i));
        if (!mapped.empty()) {
            *result.MutableKeyColunmNames(i) = mapped;
        }
    }

    // Obfuscate indexes
    for (int i = 0; i < result.MutableIndexes()->size(); ++i) {
        auto* idx = result.MutableIndexes(i);
        mapped = MapIdentifier(idx->GetName());
        if (!mapped.empty()) {
            idx->SetName(mapped);
        }
        for (int j = 0; j < idx->MutableKeyColumns()->size(); ++j) {
            mapped = MapIdentifier(idx->GetKeyColumns(j));
            if (!mapped.empty()) {
                *idx->MutableKeyColumns(j) = mapped;
            }
        }
        for (int j = 0; j < idx->MutableDataColumns()->size(); ++j) {
            mapped = MapIdentifier(idx->GetDataColumns(j));
            if (!mapped.empty()) {
                *idx->MutableDataColumns(j) = mapped;
            }
        }
    }

    // Recurse into secondary global index metadata
    for (int i = 0; i < result.MutableSecondaryGlobalIndexMetadata()->size(); ++i) {
        auto obfuscated = ObfuscateMetadata(result.GetSecondaryGlobalIndexMetadata(i));
        *result.MutableSecondaryGlobalIndexMetadata(i) = std::move(obfuscated);
    }

    return result;
}

bool TReplayMessageObfuscator::ObfuscateQueryText(
    const TString& queryText, TString& result, NYql::TIssues& issues) const
{
    NSQLTranslation::TTranslationSettings settings;
    if (!NSQLTranslation::ParseTranslationSettings(queryText, settings, issues)) {
        return false;
    }

    if (settings.PgParser) {
        issues.AddIssue(NYql::TIssue({}, "PG dialect is not supported for obfuscation"));
        return false;
    }

    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

    google::protobuf::Arena arena;
    auto* message = NSQLTranslationV1::SqlAST(
        parsers, queryText, "query", issues,
        NSQLTranslation::SQL_MAX_PARSER_ERRORS, settings.AnsiLexer, &arena);

    if (!message) {
        return false;
    }

    TMappingObfuscatingVisitor visitor(Mapping_);
    result = visitor.Process(*message);
    return true;
}

bool TReplayMessageObfuscator::ObfuscateReplayMessage(
    const TString& replayJson, TString& result, NYql::TIssues& issues)
{
    NJson::TJsonValue root;
    {
        NJson::TJsonReaderConfig readerConfig;
        TStringInput in(replayJson);
        if (!NJson::ReadJsonTree(&in, &readerConfig, &root, false)) {
            issues.AddIssue(NYql::TIssue({}, "Failed to parse replay JSON"));
            return false;
        }
    }

    // Extract and deserialize table metadata
    TVector<NKikimrKqp::TKqpTableMetadataProto> metaProtos;

    ui64 metaType = 1; // EncodedProto by default
    if (root.Has("table_meta_serialization_type")) {
        metaType = root["table_meta_serialization_type"].GetUIntegerSafe();
    }

    if (root.Has("table_metadata")) {
        if (metaType == 1) { // EncodedProto
            NJson::TJsonValue tablemetajson;
            NJson::TJsonReaderConfig readerConfig;
            TStringInput in(root["table_metadata"].GetStringSafe());
            NJson::ReadJsonTree(&in, &readerConfig, &tablemetajson, false);

            if (tablemetajson.IsArray()) {
                for (auto& node : tablemetajson.GetArray()) {
                    NKikimrKqp::TKqpTableMetadataProto proto;
                    TString decoded = Base64Decode(node.GetStringRobust());
                    if (proto.ParseFromString(decoded)) {
                        metaProtos.push_back(std::move(proto));
                    }
                }
            }
        }
    }

    // Phase 1: Build mapping from metadata
    for (const auto& meta : metaProtos) {
        AddTableMetadata(meta);
    }

    // Also map database and cluster
    if (root.Has("query_database")) {
        const auto& db = root["query_database"].GetStringSafe();
        if (!db.empty() && !Mapping_.contains(db)) {
            AddMapping(db, "database_0");
        }
    }
    if (root.Has("query_cluster")) {
        const auto& cluster = root["query_cluster"].GetStringSafe();
        if (!cluster.empty() && !Mapping_.contains(cluster)) {
            AddMapping(cluster, "cluster_0");
        }
    }

    // Phase 2a: Obfuscate metadata
    NJson::TJsonValue obfuscatedTablesMeta(NJson::JSON_ARRAY);
    for (const auto& meta : metaProtos) {
        auto obfuscated = ObfuscateMetadata(meta);
        obfuscatedTablesMeta.AppendValue(Base64Encode(obfuscated.SerializeAsString()));
    }

    // Phase 2b: Obfuscate query text
    TString obfuscatedQueryText;
    if (root.Has("query_text")) {
        TString queryText = UnescapeC(root["query_text"].GetStringSafe());
        if (!ObfuscateQueryText(queryText, obfuscatedQueryText, issues)) {
            return false;
        }
    }

    // Phase 3: Reassemble JSON
    NJson::TJsonValue resultJson(root);

    resultJson["table_metadata"] = TString(NJson::WriteJson(obfuscatedTablesMeta, false));
    resultJson["table_meta_serialization_type"] = static_cast<ui64>(1); // EncodedProto

    if (!obfuscatedQueryText.empty()) {
        resultJson["query_text"] = EscapeC(obfuscatedQueryText);
    }

    if (root.Has("query_database")) {
        TString mapped = MapIdentifier(root["query_database"].GetStringSafe());
        if (!mapped.empty()) {
            resultJson["query_database"] = mapped;
        }
    }
    if (root.Has("query_cluster")) {
        TString mapped = MapIdentifier(root["query_cluster"].GetStringSafe());
        if (!mapped.empty()) {
            resultJson["query_cluster"] = mapped;
        }
    }

    result = NJson::WriteJson(resultJson, false);
    return true;
}

} // namespace NKikimr::NKqp
