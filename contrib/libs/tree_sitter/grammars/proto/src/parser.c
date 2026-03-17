#include <tree_sitter/parser.h>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#ifdef _MSC_VER
#pragma optimize("", off)
#elif defined(__clang__)
#pragma clang optimize off
#elif defined(__GNUC__)
#pragma GCC optimize ("O0")
#endif

#define LANGUAGE_VERSION 14
#define STATE_COUNT 317
#define LARGE_STATE_COUNT 2
#define SYMBOL_COUNT 127
#define ALIAS_COUNT 0
#define TOKEN_COUNT 68
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 1
#define MAX_ALIAS_SEQUENCE_LENGTH 14
#define PRODUCTION_ID_COUNT 3

enum {
  anon_sym_SEMI = 1,
  anon_sym_syntax = 2,
  anon_sym_EQ = 3,
  anon_sym_DQUOTEproto3_DQUOTE = 4,
  anon_sym_DQUOTEproto2_DQUOTE = 5,
  anon_sym_import = 6,
  anon_sym_weak = 7,
  anon_sym_public = 8,
  anon_sym_package = 9,
  anon_sym_option = 10,
  anon_sym_LPAREN = 11,
  anon_sym_RPAREN = 12,
  anon_sym_DOT = 13,
  anon_sym_enum = 14,
  anon_sym_LBRACE = 15,
  anon_sym_RBRACE = 16,
  anon_sym_DASH = 17,
  anon_sym_LBRACK = 18,
  anon_sym_COMMA = 19,
  anon_sym_RBRACK = 20,
  anon_sym_message = 21,
  anon_sym_extend = 22,
  anon_sym_optional = 23,
  anon_sym_required = 24,
  anon_sym_repeated = 25,
  anon_sym_oneof = 26,
  anon_sym_map = 27,
  anon_sym_LT = 28,
  anon_sym_GT = 29,
  anon_sym_int32 = 30,
  anon_sym_int64 = 31,
  anon_sym_uint32 = 32,
  anon_sym_uint64 = 33,
  anon_sym_sint32 = 34,
  anon_sym_sint64 = 35,
  anon_sym_fixed32 = 36,
  anon_sym_fixed64 = 37,
  anon_sym_sfixed32 = 38,
  anon_sym_sfixed64 = 39,
  anon_sym_bool = 40,
  anon_sym_string = 41,
  anon_sym_double = 42,
  anon_sym_float = 43,
  anon_sym_bytes = 44,
  anon_sym_reserved = 45,
  anon_sym_extensions = 46,
  anon_sym_to = 47,
  anon_sym_max = 48,
  anon_sym_service = 49,
  anon_sym_rpc = 50,
  anon_sym_stream = 51,
  anon_sym_returns = 52,
  anon_sym_PLUS = 53,
  anon_sym_COLON = 54,
  sym_identifier = 55,
  sym_true = 56,
  sym_false = 57,
  sym_decimal_lit = 58,
  sym_octal_lit = 59,
  sym_hex_lit = 60,
  sym_float_lit = 61,
  anon_sym_DQUOTE = 62,
  aux_sym_string_token1 = 63,
  anon_sym_SQUOTE = 64,
  aux_sym_string_token2 = 65,
  sym_escape_sequence = 66,
  sym_comment = 67,
  sym_source_file = 68,
  sym_empty_statement = 69,
  sym_syntax = 70,
  sym_import = 71,
  sym_package = 72,
  sym_option = 73,
  sym__option_name = 74,
  sym_enum = 75,
  sym_enum_name = 76,
  sym_enum_body = 77,
  sym_enum_field = 78,
  sym_enum_value_option = 79,
  sym_message = 80,
  sym_message_body = 81,
  sym_message_name = 82,
  sym_extend = 83,
  sym_field = 84,
  sym_field_options = 85,
  sym_field_option = 86,
  sym_oneof = 87,
  sym_oneof_field = 88,
  sym_map_field = 89,
  sym_key_type = 90,
  sym_type = 91,
  sym_reserved = 92,
  sym_extensions = 93,
  sym_ranges = 94,
  sym_range = 95,
  sym_field_names = 96,
  sym_message_or_enum_type = 97,
  sym_field_number = 98,
  sym_service = 99,
  sym_service_name = 100,
  sym_rpc = 101,
  sym_rpc_name = 102,
  sym_constant = 103,
  sym_block_lit = 104,
  sym__identifier_or_string = 105,
  sym_full_ident = 106,
  sym_bool = 107,
  sym_int_lit = 108,
  sym_string = 109,
  aux_sym_source_file_repeat1 = 110,
  aux_sym__option_name_repeat1 = 111,
  aux_sym_enum_body_repeat1 = 112,
  aux_sym_enum_field_repeat1 = 113,
  aux_sym_message_body_repeat1 = 114,
  aux_sym_field_options_repeat1 = 115,
  aux_sym_oneof_repeat1 = 116,
  aux_sym_ranges_repeat1 = 117,
  aux_sym_field_names_repeat1 = 118,
  aux_sym_message_or_enum_type_repeat1 = 119,
  aux_sym_service_repeat1 = 120,
  aux_sym_rpc_repeat1 = 121,
  aux_sym_block_lit_repeat1 = 122,
  aux_sym_block_lit_repeat2 = 123,
  aux_sym_string_repeat1 = 124,
  aux_sym_string_repeat2 = 125,
  aux_sym_string_repeat3 = 126,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [anon_sym_SEMI] = ";",
  [anon_sym_syntax] = "syntax",
  [anon_sym_EQ] = "=",
  [anon_sym_DQUOTEproto3_DQUOTE] = "\"proto3\"",
  [anon_sym_DQUOTEproto2_DQUOTE] = "\"proto2\"",
  [anon_sym_import] = "import",
  [anon_sym_weak] = "weak",
  [anon_sym_public] = "public",
  [anon_sym_package] = "package",
  [anon_sym_option] = "option",
  [anon_sym_LPAREN] = "(",
  [anon_sym_RPAREN] = ")",
  [anon_sym_DOT] = ".",
  [anon_sym_enum] = "enum",
  [anon_sym_LBRACE] = "{",
  [anon_sym_RBRACE] = "}",
  [anon_sym_DASH] = "-",
  [anon_sym_LBRACK] = "[",
  [anon_sym_COMMA] = ",",
  [anon_sym_RBRACK] = "]",
  [anon_sym_message] = "message",
  [anon_sym_extend] = "extend",
  [anon_sym_optional] = "optional",
  [anon_sym_required] = "required",
  [anon_sym_repeated] = "repeated",
  [anon_sym_oneof] = "oneof",
  [anon_sym_map] = "map",
  [anon_sym_LT] = "<",
  [anon_sym_GT] = ">",
  [anon_sym_int32] = "int32",
  [anon_sym_int64] = "int64",
  [anon_sym_uint32] = "uint32",
  [anon_sym_uint64] = "uint64",
  [anon_sym_sint32] = "sint32",
  [anon_sym_sint64] = "sint64",
  [anon_sym_fixed32] = "fixed32",
  [anon_sym_fixed64] = "fixed64",
  [anon_sym_sfixed32] = "sfixed32",
  [anon_sym_sfixed64] = "sfixed64",
  [anon_sym_bool] = "bool",
  [anon_sym_string] = "string",
  [anon_sym_double] = "double",
  [anon_sym_float] = "float",
  [anon_sym_bytes] = "bytes",
  [anon_sym_reserved] = "reserved",
  [anon_sym_extensions] = "extensions",
  [anon_sym_to] = "to",
  [anon_sym_max] = "max",
  [anon_sym_service] = "service",
  [anon_sym_rpc] = "rpc",
  [anon_sym_stream] = "stream",
  [anon_sym_returns] = "returns",
  [anon_sym_PLUS] = "+",
  [anon_sym_COLON] = ":",
  [sym_identifier] = "identifier",
  [sym_true] = "true",
  [sym_false] = "false",
  [sym_decimal_lit] = "decimal_lit",
  [sym_octal_lit] = "octal_lit",
  [sym_hex_lit] = "hex_lit",
  [sym_float_lit] = "float_lit",
  [anon_sym_DQUOTE] = "\"",
  [aux_sym_string_token1] = "string_token1",
  [anon_sym_SQUOTE] = "'",
  [aux_sym_string_token2] = "string_token2",
  [sym_escape_sequence] = "escape_sequence",
  [sym_comment] = "comment",
  [sym_source_file] = "source_file",
  [sym_empty_statement] = "empty_statement",
  [sym_syntax] = "syntax",
  [sym_import] = "import",
  [sym_package] = "package",
  [sym_option] = "option",
  [sym__option_name] = "_option_name",
  [sym_enum] = "enum",
  [sym_enum_name] = "enum_name",
  [sym_enum_body] = "enum_body",
  [sym_enum_field] = "enum_field",
  [sym_enum_value_option] = "enum_value_option",
  [sym_message] = "message",
  [sym_message_body] = "message_body",
  [sym_message_name] = "message_name",
  [sym_extend] = "extend",
  [sym_field] = "field",
  [sym_field_options] = "field_options",
  [sym_field_option] = "field_option",
  [sym_oneof] = "oneof",
  [sym_oneof_field] = "oneof_field",
  [sym_map_field] = "map_field",
  [sym_key_type] = "key_type",
  [sym_type] = "type",
  [sym_reserved] = "reserved",
  [sym_extensions] = "extensions",
  [sym_ranges] = "ranges",
  [sym_range] = "range",
  [sym_field_names] = "field_names",
  [sym_message_or_enum_type] = "message_or_enum_type",
  [sym_field_number] = "field_number",
  [sym_service] = "service",
  [sym_service_name] = "service_name",
  [sym_rpc] = "rpc",
  [sym_rpc_name] = "rpc_name",
  [sym_constant] = "constant",
  [sym_block_lit] = "block_lit",
  [sym__identifier_or_string] = "_identifier_or_string",
  [sym_full_ident] = "full_ident",
  [sym_bool] = "bool",
  [sym_int_lit] = "int_lit",
  [sym_string] = "string",
  [aux_sym_source_file_repeat1] = "source_file_repeat1",
  [aux_sym__option_name_repeat1] = "_option_name_repeat1",
  [aux_sym_enum_body_repeat1] = "enum_body_repeat1",
  [aux_sym_enum_field_repeat1] = "enum_field_repeat1",
  [aux_sym_message_body_repeat1] = "message_body_repeat1",
  [aux_sym_field_options_repeat1] = "field_options_repeat1",
  [aux_sym_oneof_repeat1] = "oneof_repeat1",
  [aux_sym_ranges_repeat1] = "ranges_repeat1",
  [aux_sym_field_names_repeat1] = "field_names_repeat1",
  [aux_sym_message_or_enum_type_repeat1] = "message_or_enum_type_repeat1",
  [aux_sym_service_repeat1] = "service_repeat1",
  [aux_sym_rpc_repeat1] = "rpc_repeat1",
  [aux_sym_block_lit_repeat1] = "block_lit_repeat1",
  [aux_sym_block_lit_repeat2] = "block_lit_repeat2",
  [aux_sym_string_repeat1] = "string_repeat1",
  [aux_sym_string_repeat2] = "string_repeat2",
  [aux_sym_string_repeat3] = "string_repeat3",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [anon_sym_SEMI] = anon_sym_SEMI,
  [anon_sym_syntax] = anon_sym_syntax,
  [anon_sym_EQ] = anon_sym_EQ,
  [anon_sym_DQUOTEproto3_DQUOTE] = anon_sym_DQUOTEproto3_DQUOTE,
  [anon_sym_DQUOTEproto2_DQUOTE] = anon_sym_DQUOTEproto2_DQUOTE,
  [anon_sym_import] = anon_sym_import,
  [anon_sym_weak] = anon_sym_weak,
  [anon_sym_public] = anon_sym_public,
  [anon_sym_package] = anon_sym_package,
  [anon_sym_option] = anon_sym_option,
  [anon_sym_LPAREN] = anon_sym_LPAREN,
  [anon_sym_RPAREN] = anon_sym_RPAREN,
  [anon_sym_DOT] = anon_sym_DOT,
  [anon_sym_enum] = anon_sym_enum,
  [anon_sym_LBRACE] = anon_sym_LBRACE,
  [anon_sym_RBRACE] = anon_sym_RBRACE,
  [anon_sym_DASH] = anon_sym_DASH,
  [anon_sym_LBRACK] = anon_sym_LBRACK,
  [anon_sym_COMMA] = anon_sym_COMMA,
  [anon_sym_RBRACK] = anon_sym_RBRACK,
  [anon_sym_message] = anon_sym_message,
  [anon_sym_extend] = anon_sym_extend,
  [anon_sym_optional] = anon_sym_optional,
  [anon_sym_required] = anon_sym_required,
  [anon_sym_repeated] = anon_sym_repeated,
  [anon_sym_oneof] = anon_sym_oneof,
  [anon_sym_map] = anon_sym_map,
  [anon_sym_LT] = anon_sym_LT,
  [anon_sym_GT] = anon_sym_GT,
  [anon_sym_int32] = anon_sym_int32,
  [anon_sym_int64] = anon_sym_int64,
  [anon_sym_uint32] = anon_sym_uint32,
  [anon_sym_uint64] = anon_sym_uint64,
  [anon_sym_sint32] = anon_sym_sint32,
  [anon_sym_sint64] = anon_sym_sint64,
  [anon_sym_fixed32] = anon_sym_fixed32,
  [anon_sym_fixed64] = anon_sym_fixed64,
  [anon_sym_sfixed32] = anon_sym_sfixed32,
  [anon_sym_sfixed64] = anon_sym_sfixed64,
  [anon_sym_bool] = anon_sym_bool,
  [anon_sym_string] = anon_sym_string,
  [anon_sym_double] = anon_sym_double,
  [anon_sym_float] = anon_sym_float,
  [anon_sym_bytes] = anon_sym_bytes,
  [anon_sym_reserved] = anon_sym_reserved,
  [anon_sym_extensions] = anon_sym_extensions,
  [anon_sym_to] = anon_sym_to,
  [anon_sym_max] = anon_sym_max,
  [anon_sym_service] = anon_sym_service,
  [anon_sym_rpc] = anon_sym_rpc,
  [anon_sym_stream] = anon_sym_stream,
  [anon_sym_returns] = anon_sym_returns,
  [anon_sym_PLUS] = anon_sym_PLUS,
  [anon_sym_COLON] = anon_sym_COLON,
  [sym_identifier] = sym_identifier,
  [sym_true] = sym_true,
  [sym_false] = sym_false,
  [sym_decimal_lit] = sym_decimal_lit,
  [sym_octal_lit] = sym_octal_lit,
  [sym_hex_lit] = sym_hex_lit,
  [sym_float_lit] = sym_float_lit,
  [anon_sym_DQUOTE] = anon_sym_DQUOTE,
  [aux_sym_string_token1] = aux_sym_string_token1,
  [anon_sym_SQUOTE] = anon_sym_SQUOTE,
  [aux_sym_string_token2] = aux_sym_string_token2,
  [sym_escape_sequence] = sym_escape_sequence,
  [sym_comment] = sym_comment,
  [sym_source_file] = sym_source_file,
  [sym_empty_statement] = sym_empty_statement,
  [sym_syntax] = sym_syntax,
  [sym_import] = sym_import,
  [sym_package] = sym_package,
  [sym_option] = sym_option,
  [sym__option_name] = sym__option_name,
  [sym_enum] = sym_enum,
  [sym_enum_name] = sym_enum_name,
  [sym_enum_body] = sym_enum_body,
  [sym_enum_field] = sym_enum_field,
  [sym_enum_value_option] = sym_enum_value_option,
  [sym_message] = sym_message,
  [sym_message_body] = sym_message_body,
  [sym_message_name] = sym_message_name,
  [sym_extend] = sym_extend,
  [sym_field] = sym_field,
  [sym_field_options] = sym_field_options,
  [sym_field_option] = sym_field_option,
  [sym_oneof] = sym_oneof,
  [sym_oneof_field] = sym_oneof_field,
  [sym_map_field] = sym_map_field,
  [sym_key_type] = sym_key_type,
  [sym_type] = sym_type,
  [sym_reserved] = sym_reserved,
  [sym_extensions] = sym_extensions,
  [sym_ranges] = sym_ranges,
  [sym_range] = sym_range,
  [sym_field_names] = sym_field_names,
  [sym_message_or_enum_type] = sym_message_or_enum_type,
  [sym_field_number] = sym_field_number,
  [sym_service] = sym_service,
  [sym_service_name] = sym_service_name,
  [sym_rpc] = sym_rpc,
  [sym_rpc_name] = sym_rpc_name,
  [sym_constant] = sym_constant,
  [sym_block_lit] = sym_block_lit,
  [sym__identifier_or_string] = sym__identifier_or_string,
  [sym_full_ident] = sym_full_ident,
  [sym_bool] = sym_bool,
  [sym_int_lit] = sym_int_lit,
  [sym_string] = sym_string,
  [aux_sym_source_file_repeat1] = aux_sym_source_file_repeat1,
  [aux_sym__option_name_repeat1] = aux_sym__option_name_repeat1,
  [aux_sym_enum_body_repeat1] = aux_sym_enum_body_repeat1,
  [aux_sym_enum_field_repeat1] = aux_sym_enum_field_repeat1,
  [aux_sym_message_body_repeat1] = aux_sym_message_body_repeat1,
  [aux_sym_field_options_repeat1] = aux_sym_field_options_repeat1,
  [aux_sym_oneof_repeat1] = aux_sym_oneof_repeat1,
  [aux_sym_ranges_repeat1] = aux_sym_ranges_repeat1,
  [aux_sym_field_names_repeat1] = aux_sym_field_names_repeat1,
  [aux_sym_message_or_enum_type_repeat1] = aux_sym_message_or_enum_type_repeat1,
  [aux_sym_service_repeat1] = aux_sym_service_repeat1,
  [aux_sym_rpc_repeat1] = aux_sym_rpc_repeat1,
  [aux_sym_block_lit_repeat1] = aux_sym_block_lit_repeat1,
  [aux_sym_block_lit_repeat2] = aux_sym_block_lit_repeat2,
  [aux_sym_string_repeat1] = aux_sym_string_repeat1,
  [aux_sym_string_repeat2] = aux_sym_string_repeat2,
  [aux_sym_string_repeat3] = aux_sym_string_repeat3,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_SEMI] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_syntax] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_EQ] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DQUOTEproto3_DQUOTE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DQUOTEproto2_DQUOTE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_import] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_weak] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_public] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_package] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_option] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DOT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_enum] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_message] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_extend] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_optional] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_required] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_repeated] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_oneof] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_map] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_GT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_int32] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_int64] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_uint32] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_uint64] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_sint32] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_sint64] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_fixed32] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_fixed64] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_sfixed32] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_sfixed64] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_bool] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_string] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_double] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_float] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_bytes] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_reserved] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_extensions] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_to] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_max] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_service] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_rpc] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_stream] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_returns] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PLUS] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COLON] = {
    .visible = true,
    .named = false,
  },
  [sym_identifier] = {
    .visible = true,
    .named = true,
  },
  [sym_true] = {
    .visible = true,
    .named = true,
  },
  [sym_false] = {
    .visible = true,
    .named = true,
  },
  [sym_decimal_lit] = {
    .visible = true,
    .named = true,
  },
  [sym_octal_lit] = {
    .visible = true,
    .named = true,
  },
  [sym_hex_lit] = {
    .visible = true,
    .named = true,
  },
  [sym_float_lit] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_DQUOTE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_string_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_SQUOTE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_string_token2] = {
    .visible = false,
    .named = false,
  },
  [sym_escape_sequence] = {
    .visible = true,
    .named = true,
  },
  [sym_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_source_file] = {
    .visible = true,
    .named = true,
  },
  [sym_empty_statement] = {
    .visible = true,
    .named = true,
  },
  [sym_syntax] = {
    .visible = true,
    .named = true,
  },
  [sym_import] = {
    .visible = true,
    .named = true,
  },
  [sym_package] = {
    .visible = true,
    .named = true,
  },
  [sym_option] = {
    .visible = true,
    .named = true,
  },
  [sym__option_name] = {
    .visible = false,
    .named = true,
  },
  [sym_enum] = {
    .visible = true,
    .named = true,
  },
  [sym_enum_name] = {
    .visible = true,
    .named = true,
  },
  [sym_enum_body] = {
    .visible = true,
    .named = true,
  },
  [sym_enum_field] = {
    .visible = true,
    .named = true,
  },
  [sym_enum_value_option] = {
    .visible = true,
    .named = true,
  },
  [sym_message] = {
    .visible = true,
    .named = true,
  },
  [sym_message_body] = {
    .visible = true,
    .named = true,
  },
  [sym_message_name] = {
    .visible = true,
    .named = true,
  },
  [sym_extend] = {
    .visible = true,
    .named = true,
  },
  [sym_field] = {
    .visible = true,
    .named = true,
  },
  [sym_field_options] = {
    .visible = true,
    .named = true,
  },
  [sym_field_option] = {
    .visible = true,
    .named = true,
  },
  [sym_oneof] = {
    .visible = true,
    .named = true,
  },
  [sym_oneof_field] = {
    .visible = true,
    .named = true,
  },
  [sym_map_field] = {
    .visible = true,
    .named = true,
  },
  [sym_key_type] = {
    .visible = true,
    .named = true,
  },
  [sym_type] = {
    .visible = true,
    .named = true,
  },
  [sym_reserved] = {
    .visible = true,
    .named = true,
  },
  [sym_extensions] = {
    .visible = true,
    .named = true,
  },
  [sym_ranges] = {
    .visible = true,
    .named = true,
  },
  [sym_range] = {
    .visible = true,
    .named = true,
  },
  [sym_field_names] = {
    .visible = true,
    .named = true,
  },
  [sym_message_or_enum_type] = {
    .visible = true,
    .named = true,
  },
  [sym_field_number] = {
    .visible = true,
    .named = true,
  },
  [sym_service] = {
    .visible = true,
    .named = true,
  },
  [sym_service_name] = {
    .visible = true,
    .named = true,
  },
  [sym_rpc] = {
    .visible = true,
    .named = true,
  },
  [sym_rpc_name] = {
    .visible = true,
    .named = true,
  },
  [sym_constant] = {
    .visible = true,
    .named = true,
  },
  [sym_block_lit] = {
    .visible = true,
    .named = true,
  },
  [sym__identifier_or_string] = {
    .visible = false,
    .named = true,
  },
  [sym_full_ident] = {
    .visible = true,
    .named = true,
  },
  [sym_bool] = {
    .visible = true,
    .named = true,
  },
  [sym_int_lit] = {
    .visible = true,
    .named = true,
  },
  [sym_string] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_source_file_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym__option_name_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_enum_body_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_enum_field_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_message_body_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_field_options_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_oneof_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_ranges_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_field_names_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_message_or_enum_type_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_service_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_rpc_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_block_lit_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_block_lit_repeat2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_string_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_string_repeat2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_string_repeat3] = {
    .visible = false,
    .named = false,
  },
};

enum {
  field_path = 1,
};

static const char * const ts_field_names[] = {
  [0] = NULL,
  [field_path] = "path",
};

static const TSFieldMapSlice ts_field_map_slices[PRODUCTION_ID_COUNT] = {
  [1] = {.index = 0, .length = 1},
  [2] = {.index = 1, .length = 1},
};

static const TSFieldMapEntry ts_field_map_entries[] = {
  [0] =
    {field_path, 1},
  [1] =
    {field_path, 2},
};

static const TSSymbol ts_alias_sequences[PRODUCTION_ID_COUNT][MAX_ALIAS_SEQUENCE_LENGTH] = {
  [0] = {0},
};

static const uint16_t ts_non_terminal_alias_map[] = {
  0,
};

static const TSStateId ts_primary_state_ids[STATE_COUNT] = {
  [0] = 0,
  [1] = 1,
  [2] = 2,
  [3] = 3,
  [4] = 2,
  [5] = 5,
  [6] = 3,
  [7] = 7,
  [8] = 8,
  [9] = 9,
  [10] = 10,
  [11] = 11,
  [12] = 12,
  [13] = 13,
  [14] = 14,
  [15] = 15,
  [16] = 16,
  [17] = 17,
  [18] = 18,
  [19] = 19,
  [20] = 20,
  [21] = 21,
  [22] = 22,
  [23] = 23,
  [24] = 24,
  [25] = 25,
  [26] = 26,
  [27] = 27,
  [28] = 28,
  [29] = 29,
  [30] = 30,
  [31] = 31,
  [32] = 32,
  [33] = 33,
  [34] = 34,
  [35] = 35,
  [36] = 36,
  [37] = 37,
  [38] = 38,
  [39] = 39,
  [40] = 40,
  [41] = 41,
  [42] = 21,
  [43] = 43,
  [44] = 44,
  [45] = 17,
  [46] = 46,
  [47] = 47,
  [48] = 48,
  [49] = 49,
  [50] = 50,
  [51] = 50,
  [52] = 52,
  [53] = 50,
  [54] = 54,
  [55] = 50,
  [56] = 56,
  [57] = 57,
  [58] = 58,
  [59] = 59,
  [60] = 60,
  [61] = 61,
  [62] = 61,
  [63] = 63,
  [64] = 17,
  [65] = 21,
  [66] = 66,
  [67] = 67,
  [68] = 68,
  [69] = 69,
  [70] = 70,
  [71] = 71,
  [72] = 69,
  [73] = 66,
  [74] = 26,
  [75] = 18,
  [76] = 76,
  [77] = 77,
  [78] = 78,
  [79] = 27,
  [80] = 80,
  [81] = 81,
  [82] = 23,
  [83] = 83,
  [84] = 84,
  [85] = 85,
  [86] = 24,
  [87] = 16,
  [88] = 11,
  [89] = 89,
  [90] = 90,
  [91] = 91,
  [92] = 92,
  [93] = 93,
  [94] = 94,
  [95] = 95,
  [96] = 96,
  [97] = 97,
  [98] = 98,
  [99] = 99,
  [100] = 100,
  [101] = 101,
  [102] = 102,
  [103] = 103,
  [104] = 104,
  [105] = 105,
  [106] = 106,
  [107] = 107,
  [108] = 108,
  [109] = 21,
  [110] = 110,
  [111] = 111,
  [112] = 112,
  [113] = 113,
  [114] = 114,
  [115] = 35,
  [116] = 116,
  [117] = 17,
  [118] = 118,
  [119] = 119,
  [120] = 120,
  [121] = 121,
  [122] = 22,
  [123] = 123,
  [124] = 124,
  [125] = 125,
  [126] = 126,
  [127] = 127,
  [128] = 128,
  [129] = 129,
  [130] = 130,
  [131] = 131,
  [132] = 132,
  [133] = 133,
  [134] = 134,
  [135] = 35,
  [136] = 136,
  [137] = 137,
  [138] = 138,
  [139] = 139,
  [140] = 132,
  [141] = 141,
  [142] = 142,
  [143] = 143,
  [144] = 144,
  [145] = 145,
  [146] = 146,
  [147] = 147,
  [148] = 148,
  [149] = 149,
  [150] = 150,
  [151] = 151,
  [152] = 152,
  [153] = 153,
  [154] = 154,
  [155] = 155,
  [156] = 156,
  [157] = 157,
  [158] = 158,
  [159] = 159,
  [160] = 160,
  [161] = 161,
  [162] = 162,
  [163] = 163,
  [164] = 164,
  [165] = 165,
  [166] = 166,
  [167] = 167,
  [168] = 168,
  [169] = 169,
  [170] = 170,
  [171] = 171,
  [172] = 172,
  [173] = 173,
  [174] = 174,
  [175] = 175,
  [176] = 176,
  [177] = 177,
  [178] = 178,
  [179] = 175,
  [180] = 180,
  [181] = 175,
  [182] = 182,
  [183] = 175,
  [184] = 184,
  [185] = 185,
  [186] = 186,
  [187] = 187,
  [188] = 188,
  [189] = 189,
  [190] = 190,
  [191] = 191,
  [192] = 192,
  [193] = 193,
  [194] = 194,
  [195] = 195,
  [196] = 196,
  [197] = 197,
  [198] = 198,
  [199] = 199,
  [200] = 200,
  [201] = 201,
  [202] = 202,
  [203] = 203,
  [204] = 204,
  [205] = 205,
  [206] = 206,
  [207] = 207,
  [208] = 208,
  [209] = 209,
  [210] = 210,
  [211] = 211,
  [212] = 212,
  [213] = 213,
  [214] = 214,
  [215] = 215,
  [216] = 216,
  [217] = 217,
  [218] = 218,
  [219] = 219,
  [220] = 220,
  [221] = 221,
  [222] = 222,
  [223] = 222,
  [224] = 224,
  [225] = 225,
  [226] = 212,
  [227] = 227,
  [228] = 225,
  [229] = 229,
  [230] = 230,
  [231] = 231,
  [232] = 232,
  [233] = 233,
  [234] = 220,
  [235] = 235,
  [236] = 236,
  [237] = 237,
  [238] = 238,
  [239] = 224,
  [240] = 229,
  [241] = 241,
  [242] = 242,
  [243] = 243,
  [244] = 244,
  [245] = 245,
  [246] = 246,
  [247] = 247,
  [248] = 248,
  [249] = 249,
  [250] = 250,
  [251] = 251,
  [252] = 252,
  [253] = 253,
  [254] = 254,
  [255] = 255,
  [256] = 256,
  [257] = 257,
  [258] = 258,
  [259] = 259,
  [260] = 260,
  [261] = 261,
  [262] = 262,
  [263] = 263,
  [264] = 264,
  [265] = 265,
  [266] = 266,
  [267] = 267,
  [268] = 268,
  [269] = 269,
  [270] = 270,
  [271] = 271,
  [272] = 272,
  [273] = 273,
  [274] = 274,
  [275] = 275,
  [276] = 276,
  [277] = 277,
  [278] = 278,
  [279] = 279,
  [280] = 280,
  [281] = 281,
  [282] = 282,
  [283] = 283,
  [284] = 284,
  [285] = 285,
  [286] = 286,
  [287] = 287,
  [288] = 288,
  [289] = 289,
  [290] = 290,
  [291] = 291,
  [292] = 292,
  [293] = 293,
  [294] = 294,
  [295] = 295,
  [296] = 296,
  [297] = 297,
  [298] = 256,
  [299] = 299,
  [300] = 300,
  [301] = 281,
  [302] = 256,
  [303] = 256,
  [304] = 304,
  [305] = 305,
  [306] = 306,
  [307] = 307,
  [308] = 308,
  [309] = 309,
  [310] = 310,
  [311] = 285,
  [312] = 285,
  [313] = 285,
  [314] = 314,
  [315] = 315,
  [316] = 316,
};

static inline bool sym_identifier_character_set_1(int32_t c) {
  return (c < 'g'
    ? (c < 'a'
      ? (c < '_'
        ? (c >= 'A' && c <= 'Z')
        : c <= '_')
      : (c <= 'a' || (c < 'e'
        ? c == 'c'
        : c <= 'e')))
    : (c <= 'h' || (c < 't'
      ? (c < 'p'
        ? (c >= 'j' && c <= 'n')
        : c <= 'r')
      : (c <= 't' || (c >= 'v' && c <= 'z')))));
}

static inline bool sym_identifier_character_set_2(int32_t c) {
  return (c < 'j'
    ? (c < 'a'
      ? (c < '_'
        ? (c >= 'A' && c <= 'Z')
        : c <= '_')
      : (c <= 'a' || (c < 'g'
        ? c == 'c'
        : c <= 'h')))
    : (c <= 'l' || (c < 't'
      ? (c < 'p'
        ? c == 'n'
        : c <= 'q')
      : (c <= 't' || (c >= 'v' && c <= 'z')))));
}

static inline bool sym_identifier_character_set_3(int32_t c) {
  return (c < 'e'
    ? (c < 'a'
      ? (c < '_'
        ? (c >= 'A' && c <= 'Z')
        : c <= '_')
      : (c <= 'a' || c == 'c'))
    : (c <= 'e' || (c < 't'
      ? (c < 'j'
        ? (c >= 'g' && c <= 'h')
        : c <= 'q')
      : (c <= 't' || (c >= 'v' && c <= 'z')))));
}

static inline bool sym_identifier_character_set_4(int32_t c) {
  return (c < 'e'
    ? (c < 'a'
      ? (c < '_'
        ? (c >= 'A' && c <= 'Z')
        : c <= '_')
      : (c <= 'a' || c == 'c'))
    : (c <= 'e' || (c < 't'
      ? (c < 'j'
        ? (c >= 'g' && c <= 'h')
        : c <= 'r')
      : (c <= 't' || (c >= 'v' && c <= 'z')))));
}

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(191);
      if (lookahead == '"') ADVANCE(409);
      if (lookahead == '\'') ADVANCE(416);
      if (lookahead == '(') ADVANCE(205);
      if (lookahead == ')') ADVANCE(206);
      if (lookahead == '+') ADVANCE(274);
      if (lookahead == ',') ADVANCE(215);
      if (lookahead == '-') ADVANCE(213);
      if (lookahead == '.') ADVANCE(208);
      if (lookahead == '/') ADVANCE(7);
      if (lookahead == '0') ADVANCE(401);
      if (lookahead == ':') ADVANCE(275);
      if (lookahead == ';') ADVANCE(192);
      if (lookahead == '<') ADVANCE(231);
      if (lookahead == '=') ADVANCE(194);
      if (lookahead == '>') ADVANCE(232);
      if (lookahead == '[') ADVANCE(214);
      if (lookahead == '\\') ADVANCE(34);
      if (lookahead == ']') ADVANCE(216);
      if (lookahead == 'b') ADVANCE(127);
      if (lookahead == 'd') ADVANCE(123);
      if (lookahead == 'e') ADVANCE(113);
      if (lookahead == 'f') ADVANCE(35);
      if (lookahead == 'i') ADVANCE(105);
      if (lookahead == 'm') ADVANCE(36);
      if (lookahead == 'n') ADVANCE(37);
      if (lookahead == 'o') ADVANCE(112);
      if (lookahead == 'p') ADVANCE(39);
      if (lookahead == 'r') ADVANCE(61);
      if (lookahead == 's') ADVANCE(62);
      if (lookahead == 't') ADVANCE(124);
      if (lookahead == 'u') ADVANCE(96);
      if (lookahead == 'w') ADVANCE(71);
      if (lookahead == '{') ADVANCE(211);
      if (lookahead == '}') ADVANCE(212);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(189)
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(399);
      END_STATE();
    case 1:
      if (lookahead == '"') ADVANCE(409);
      if (lookahead == '\'') ADVANCE(416);
      if (lookahead == '(') ADVANCE(205);
      if (lookahead == ')') ADVANCE(206);
      if (lookahead == ',') ADVANCE(215);
      if (lookahead == '.') ADVANCE(207);
      if (lookahead == '/') ADVANCE(7);
      if (lookahead == '0') ADVANCE(403);
      if (lookahead == ';') ADVANCE(192);
      if (lookahead == '=') ADVANCE(194);
      if (lookahead == '>') ADVANCE(232);
      if (lookahead == '[') ADVANCE(214);
      if (lookahead == ']') ADVANCE(216);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == '{') ADVANCE(211);
      if (lookahead == '}') ADVANCE(212);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(1)
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(400);
      END_STATE();
    case 2:
      if (lookahead == '"') ADVANCE(409);
      if (lookahead == '\'') ADVANCE(416);
      if (lookahead == '+') ADVANCE(274);
      if (lookahead == '-') ADVANCE(213);
      if (lookahead == '.') ADVANCE(178);
      if (lookahead == '/') ADVANCE(7);
      if (lookahead == '0') ADVANCE(401);
      if (lookahead == ':') ADVANCE(275);
      if (lookahead == '[') ADVANCE(214);
      if (lookahead == ']') ADVANCE(216);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'e') ||
          lookahead == 'g' ||
          lookahead == 'h' ||
          ('j' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'f') ADVANCE(391);
      if (lookahead == 'i') ADVANCE(345);
      if (lookahead == 'n') ADVANCE(392);
      if (lookahead == 't') ADVANCE(362);
      if (lookahead == '{') ADVANCE(211);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(2)
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(399);
      END_STATE();
    case 3:
      if (lookahead == '"') ADVANCE(409);
      if (lookahead == '/') ADVANCE(411);
      if (lookahead == '\\') ADVANCE(34);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(414);
      if (lookahead != 0) ADVANCE(415);
      END_STATE();
    case 4:
      if (lookahead == '"') ADVANCE(196);
      END_STATE();
    case 5:
      if (lookahead == '"') ADVANCE(195);
      END_STATE();
    case 6:
      if (lookahead == '\'') ADVANCE(416);
      if (lookahead == '/') ADVANCE(418);
      if (lookahead == '\\') ADVANCE(34);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(421);
      if (lookahead != 0) ADVANCE(422);
      END_STATE();
    case 7:
      if (lookahead == '*') ADVANCE(9);
      if (lookahead == '/') ADVANCE(427);
      END_STATE();
    case 8:
      if (lookahead == '*') ADVANCE(8);
      if (lookahead == '/') ADVANCE(426);
      if (lookahead != 0) ADVANCE(9);
      END_STATE();
    case 9:
      if (lookahead == '*') ADVANCE(8);
      if (lookahead != 0) ADVANCE(9);
      END_STATE();
    case 10:
      if (lookahead == '.') ADVANCE(407);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(177);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(10);
      END_STATE();
    case 11:
      if (lookahead == '.') ADVANCE(207);
      if (lookahead == '/') ADVANCE(7);
      if (lookahead == ';') ADVANCE(192);
      if (lookahead == '[') ADVANCE(214);
      if (sym_identifier_character_set_1(lookahead)) ADVANCE(394);
      if (lookahead == 'b') ADVANCE(298);
      if (lookahead == 'd') ADVANCE(350);
      if (lookahead == 'f') ADVANCE(295);
      if (lookahead == 'i') ADVANCE(344);
      if (lookahead == 'o') ADVANCE(359);
      if (lookahead == 's') ADVANCE(291);
      if (lookahead == 'u') ADVANCE(332);
      if (lookahead == '}') ADVANCE(212);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(11)
      END_STATE();
    case 12:
      if (lookahead == '.') ADVANCE(207);
      if (lookahead == '/') ADVANCE(7);
      if (lookahead == ';') ADVANCE(192);
      if (sym_identifier_character_set_2(lookahead)) ADVANCE(394);
      if (lookahead == 'b') ADVANCE(298);
      if (lookahead == 'd') ADVANCE(350);
      if (lookahead == 'e') ADVANCE(297);
      if (lookahead == 'f') ADVANCE(295);
      if (lookahead == 'i') ADVANCE(344);
      if (lookahead == 'm') ADVANCE(294);
      if (lookahead == 'o') ADVANCE(296);
      if (lookahead == 'r') ADVANCE(305);
      if (lookahead == 's') ADVANCE(291);
      if (lookahead == 'u') ADVANCE(332);
      if (lookahead == '}') ADVANCE(212);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(12)
      END_STATE();
    case 13:
      if (lookahead == '.') ADVANCE(207);
      if (lookahead == '/') ADVANCE(7);
      if (sym_identifier_character_set_3(lookahead)) ADVANCE(394);
      if (lookahead == 'b') ADVANCE(298);
      if (lookahead == 'd') ADVANCE(350);
      if (lookahead == 'f') ADVANCE(295);
      if (lookahead == 'i') ADVANCE(344);
      if (lookahead == 'r') ADVANCE(314);
      if (lookahead == 's') ADVANCE(291);
      if (lookahead == 'u') ADVANCE(332);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(13)
      END_STATE();
    case 14:
      if (lookahead == '.') ADVANCE(207);
      if (lookahead == '/') ADVANCE(7);
      if (sym_identifier_character_set_4(lookahead)) ADVANCE(394);
      if (lookahead == 'b') ADVANCE(298);
      if (lookahead == 'd') ADVANCE(350);
      if (lookahead == 'f') ADVANCE(295);
      if (lookahead == 'i') ADVANCE(344);
      if (lookahead == 's') ADVANCE(291);
      if (lookahead == 'u') ADVANCE(332);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(14)
      END_STATE();
    case 15:
      if (lookahead == '.') ADVANCE(207);
      if (lookahead == '/') ADVANCE(7);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'r') ||
          ('t' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 's') ADVANCE(378);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(15)
      END_STATE();
    case 16:
      if (lookahead == '.') ADVANCE(178);
      if (lookahead == '/') ADVANCE(7);
      if (lookahead == '0') ADVANCE(401);
      if (lookahead == 'i') ADVANCE(115);
      if (lookahead == 'n') ADVANCE(37);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(16)
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(399);
      END_STATE();
    case 17:
      if (lookahead == '/') ADVANCE(7);
      if (lookahead == ';') ADVANCE(192);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'n') ||
          lookahead == 'p' ||
          lookahead == 'q' ||
          ('s' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'o') ADVANCE(359);
      if (lookahead == 'r') ADVANCE(321);
      if (lookahead == '}') ADVANCE(212);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(17)
      END_STATE();
    case 18:
      if (lookahead == '2') ADVANCE(233);
      END_STATE();
    case 19:
      if (lookahead == '2') ADVANCE(241);
      END_STATE();
    case 20:
      if (lookahead == '2') ADVANCE(237);
      END_STATE();
    case 21:
      if (lookahead == '2') ADVANCE(245);
      END_STATE();
    case 22:
      if (lookahead == '2') ADVANCE(249);
      END_STATE();
    case 23:
      if (lookahead == '2') ADVANCE(4);
      if (lookahead == '3') ADVANCE(5);
      END_STATE();
    case 24:
      if (lookahead == '3') ADVANCE(18);
      if (lookahead == '6') ADVANCE(29);
      END_STATE();
    case 25:
      if (lookahead == '3') ADVANCE(19);
      if (lookahead == '6') ADVANCE(30);
      END_STATE();
    case 26:
      if (lookahead == '3') ADVANCE(20);
      if (lookahead == '6') ADVANCE(31);
      END_STATE();
    case 27:
      if (lookahead == '3') ADVANCE(21);
      if (lookahead == '6') ADVANCE(32);
      END_STATE();
    case 28:
      if (lookahead == '3') ADVANCE(22);
      if (lookahead == '6') ADVANCE(33);
      END_STATE();
    case 29:
      if (lookahead == '4') ADVANCE(235);
      END_STATE();
    case 30:
      if (lookahead == '4') ADVANCE(243);
      END_STATE();
    case 31:
      if (lookahead == '4') ADVANCE(239);
      END_STATE();
    case 32:
      if (lookahead == '4') ADVANCE(247);
      END_STATE();
    case 33:
      if (lookahead == '4') ADVANCE(251);
      END_STATE();
    case 34:
      if (lookahead == 'U') ADVANCE(188);
      if (lookahead == 'u') ADVANCE(184);
      if (lookahead == 'x') ADVANCE(182);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(425);
      if (lookahead != 0) ADVANCE(423);
      END_STATE();
    case 35:
      if (lookahead == 'a') ADVANCE(101);
      if (lookahead == 'i') ADVANCE(175);
      if (lookahead == 'l') ADVANCE(129);
      END_STATE();
    case 36:
      if (lookahead == 'a') ADVANCE(135);
      if (lookahead == 'e') ADVANCE(151);
      END_STATE();
    case 37:
      if (lookahead == 'a') ADVANCE(108);
      END_STATE();
    case 38:
      if (lookahead == 'a') ADVANCE(50);
      END_STATE();
    case 39:
      if (lookahead == 'a') ADVANCE(50);
      if (lookahead == 'u') ADVANCE(48);
      END_STATE();
    case 40:
      if (lookahead == 'a') ADVANCE(87);
      END_STATE();
    case 41:
      if (lookahead == 'a') ADVANCE(174);
      END_STATE();
    case 42:
      if (lookahead == 'a') ADVANCE(107);
      END_STATE();
    case 43:
      if (lookahead == 'a') ADVANCE(97);
      END_STATE();
    case 44:
      if (lookahead == 'a') ADVANCE(173);
      if (lookahead == 'e') ADVANCE(151);
      END_STATE();
    case 45:
      if (lookahead == 'a') ADVANCE(154);
      END_STATE();
    case 46:
      if (lookahead == 'a') ADVANCE(88);
      END_STATE();
    case 47:
      if (lookahead == 'a') ADVANCE(161);
      END_STATE();
    case 48:
      if (lookahead == 'b') ADVANCE(102);
      END_STATE();
    case 49:
      if (lookahead == 'b') ADVANCE(103);
      END_STATE();
    case 50:
      if (lookahead == 'c') ADVANCE(98);
      END_STATE();
    case 51:
      if (lookahead == 'c') ADVANCE(270);
      END_STATE();
    case 52:
      if (lookahead == 'c') ADVANCE(199);
      END_STATE();
    case 53:
      if (lookahead == 'c') ADVANCE(70);
      END_STATE();
    case 54:
      if (lookahead == 'd') ADVANCE(219);
      END_STATE();
    case 55:
      if (lookahead == 'd') ADVANCE(219);
      if (lookahead == 's') ADVANCE(93);
      END_STATE();
    case 56:
      if (lookahead == 'd') ADVANCE(225);
      END_STATE();
    case 57:
      if (lookahead == 'd') ADVANCE(223);
      END_STATE();
    case 58:
      if (lookahead == 'd') ADVANCE(263);
      END_STATE();
    case 59:
      if (lookahead == 'd') ADVANCE(27);
      END_STATE();
    case 60:
      if (lookahead == 'd') ADVANCE(28);
      END_STATE();
    case 61:
      if (lookahead == 'e') ADVANCE(139);
      if (lookahead == 'p') ADVANCE(51);
      END_STATE();
    case 62:
      if (lookahead == 'e') ADVANCE(141);
      if (lookahead == 'f') ADVANCE(95);
      if (lookahead == 'i') ADVANCE(117);
      if (lookahead == 't') ADVANCE(142);
      if (lookahead == 'y') ADVANCE(118);
      END_STATE();
    case 63:
      if (lookahead == 'e') ADVANCE(141);
      if (lookahead == 'y') ADVANCE(118);
      END_STATE();
    case 64:
      if (lookahead == 'e') ADVANCE(59);
      END_STATE();
    case 65:
      if (lookahead == 'e') ADVANCE(395);
      END_STATE();
    case 66:
      if (lookahead == 'e') ADVANCE(397);
      END_STATE();
    case 67:
      if (lookahead == 'e') ADVANCE(257);
      END_STATE();
    case 68:
      if (lookahead == 'e') ADVANCE(217);
      END_STATE();
    case 69:
      if (lookahead == 'e') ADVANCE(200);
      END_STATE();
    case 70:
      if (lookahead == 'e') ADVANCE(269);
      END_STATE();
    case 71:
      if (lookahead == 'e') ADVANCE(43);
      END_STATE();
    case 72:
      if (lookahead == 'e') ADVANCE(56);
      END_STATE();
    case 73:
      if (lookahead == 'e') ADVANCE(109);
      END_STATE();
    case 74:
      if (lookahead == 'e') ADVANCE(57);
      END_STATE();
    case 75:
      if (lookahead == 'e') ADVANCE(148);
      END_STATE();
    case 76:
      if (lookahead == 'e') ADVANCE(58);
      END_STATE();
    case 77:
      if (lookahead == 'e') ADVANCE(125);
      END_STATE();
    case 78:
      if (lookahead == 'e') ADVANCE(47);
      END_STATE();
    case 79:
      if (lookahead == 'e') ADVANCE(42);
      if (lookahead == 'i') ADVANCE(116);
      END_STATE();
    case 80:
      if (lookahead == 'e') ADVANCE(120);
      END_STATE();
    case 81:
      if (lookahead == 'e') ADVANCE(143);
      END_STATE();
    case 82:
      if (lookahead == 'e') ADVANCE(60);
      END_STATE();
    case 83:
      if (lookahead == 'f') ADVANCE(406);
      END_STATE();
    case 84:
      if (lookahead == 'f') ADVANCE(406);
      if (lookahead == 't') ADVANCE(24);
      END_STATE();
    case 85:
      if (lookahead == 'f') ADVANCE(227);
      END_STATE();
    case 86:
      if (lookahead == 'g') ADVANCE(255);
      END_STATE();
    case 87:
      if (lookahead == 'g') ADVANCE(68);
      END_STATE();
    case 88:
      if (lookahead == 'g') ADVANCE(69);
      END_STATE();
    case 89:
      if (lookahead == 'i') ADVANCE(52);
      END_STATE();
    case 90:
      if (lookahead == 'i') ADVANCE(53);
      END_STATE();
    case 91:
      if (lookahead == 'i') ADVANCE(147);
      END_STATE();
    case 92:
      if (lookahead == 'i') ADVANCE(131);
      END_STATE();
    case 93:
      if (lookahead == 'i') ADVANCE(133);
      END_STATE();
    case 94:
      if (lookahead == 'i') ADVANCE(134);
      END_STATE();
    case 95:
      if (lookahead == 'i') ADVANCE(176);
      END_STATE();
    case 96:
      if (lookahead == 'i') ADVANCE(122);
      END_STATE();
    case 97:
      if (lookahead == 'k') ADVANCE(198);
      END_STATE();
    case 98:
      if (lookahead == 'k') ADVANCE(46);
      END_STATE();
    case 99:
      if (lookahead == 'l') ADVANCE(253);
      END_STATE();
    case 100:
      if (lookahead == 'l') ADVANCE(221);
      END_STATE();
    case 101:
      if (lookahead == 'l') ADVANCE(153);
      END_STATE();
    case 102:
      if (lookahead == 'l') ADVANCE(89);
      END_STATE();
    case 103:
      if (lookahead == 'l') ADVANCE(67);
      END_STATE();
    case 104:
      if (lookahead == 'm') ADVANCE(137);
      END_STATE();
    case 105:
      if (lookahead == 'm') ADVANCE(137);
      if (lookahead == 'n') ADVANCE(84);
      END_STATE();
    case 106:
      if (lookahead == 'm') ADVANCE(209);
      END_STATE();
    case 107:
      if (lookahead == 'm') ADVANCE(271);
      END_STATE();
    case 108:
      if (lookahead == 'n') ADVANCE(406);
      END_STATE();
    case 109:
      if (lookahead == 'n') ADVANCE(55);
      END_STATE();
    case 110:
      if (lookahead == 'n') ADVANCE(202);
      END_STATE();
    case 111:
      if (lookahead == 'n') ADVANCE(201);
      END_STATE();
    case 112:
      if (lookahead == 'n') ADVANCE(77);
      if (lookahead == 'p') ADVANCE(156);
      END_STATE();
    case 113:
      if (lookahead == 'n') ADVANCE(166);
      if (lookahead == 'x') ADVANCE(158);
      END_STATE();
    case 114:
      if (lookahead == 'n') ADVANCE(166);
      if (lookahead == 'x') ADVANCE(162);
      END_STATE();
    case 115:
      if (lookahead == 'n') ADVANCE(83);
      END_STATE();
    case 116:
      if (lookahead == 'n') ADVANCE(86);
      END_STATE();
    case 117:
      if (lookahead == 'n') ADVANCE(163);
      END_STATE();
    case 118:
      if (lookahead == 'n') ADVANCE(159);
      END_STATE();
    case 119:
      if (lookahead == 'n') ADVANCE(149);
      END_STATE();
    case 120:
      if (lookahead == 'n') ADVANCE(54);
      END_STATE();
    case 121:
      if (lookahead == 'n') ADVANCE(150);
      END_STATE();
    case 122:
      if (lookahead == 'n') ADVANCE(164);
      END_STATE();
    case 123:
      if (lookahead == 'o') ADVANCE(170);
      END_STATE();
    case 124:
      if (lookahead == 'o') ADVANCE(267);
      if (lookahead == 'r') ADVANCE(169);
      END_STATE();
    case 125:
      if (lookahead == 'o') ADVANCE(85);
      END_STATE();
    case 126:
      if (lookahead == 'o') ADVANCE(23);
      END_STATE();
    case 127:
      if (lookahead == 'o') ADVANCE(128);
      if (lookahead == 'y') ADVANCE(157);
      END_STATE();
    case 128:
      if (lookahead == 'o') ADVANCE(99);
      END_STATE();
    case 129:
      if (lookahead == 'o') ADVANCE(45);
      END_STATE();
    case 130:
      if (lookahead == 'o') ADVANCE(144);
      END_STATE();
    case 131:
      if (lookahead == 'o') ADVANCE(110);
      END_STATE();
    case 132:
      if (lookahead == 'o') ADVANCE(160);
      END_STATE();
    case 133:
      if (lookahead == 'o') ADVANCE(121);
      END_STATE();
    case 134:
      if (lookahead == 'o') ADVANCE(111);
      END_STATE();
    case 135:
      if (lookahead == 'p') ADVANCE(229);
      if (lookahead == 'x') ADVANCE(268);
      END_STATE();
    case 136:
      if (lookahead == 'p') ADVANCE(51);
      END_STATE();
    case 137:
      if (lookahead == 'p') ADVANCE(130);
      END_STATE();
    case 138:
      if (lookahead == 'p') ADVANCE(146);
      END_STATE();
    case 139:
      if (lookahead == 'p') ADVANCE(78);
      if (lookahead == 'q') ADVANCE(168);
      if (lookahead == 's') ADVANCE(81);
      if (lookahead == 't') ADVANCE(167);
      END_STATE();
    case 140:
      if (lookahead == 'p') ADVANCE(165);
      END_STATE();
    case 141:
      if (lookahead == 'r') ADVANCE(171);
      END_STATE();
    case 142:
      if (lookahead == 'r') ADVANCE(79);
      END_STATE();
    case 143:
      if (lookahead == 'r') ADVANCE(172);
      END_STATE();
    case 144:
      if (lookahead == 'r') ADVANCE(155);
      END_STATE();
    case 145:
      if (lookahead == 'r') ADVANCE(119);
      END_STATE();
    case 146:
      if (lookahead == 'r') ADVANCE(132);
      END_STATE();
    case 147:
      if (lookahead == 'r') ADVANCE(74);
      END_STATE();
    case 148:
      if (lookahead == 's') ADVANCE(261);
      END_STATE();
    case 149:
      if (lookahead == 's') ADVANCE(273);
      END_STATE();
    case 150:
      if (lookahead == 's') ADVANCE(265);
      END_STATE();
    case 151:
      if (lookahead == 's') ADVANCE(152);
      END_STATE();
    case 152:
      if (lookahead == 's') ADVANCE(40);
      END_STATE();
    case 153:
      if (lookahead == 's') ADVANCE(66);
      END_STATE();
    case 154:
      if (lookahead == 't') ADVANCE(259);
      END_STATE();
    case 155:
      if (lookahead == 't') ADVANCE(197);
      END_STATE();
    case 156:
      if (lookahead == 't') ADVANCE(92);
      END_STATE();
    case 157:
      if (lookahead == 't') ADVANCE(75);
      END_STATE();
    case 158:
      if (lookahead == 't') ADVANCE(73);
      END_STATE();
    case 159:
      if (lookahead == 't') ADVANCE(41);
      END_STATE();
    case 160:
      if (lookahead == 't') ADVANCE(126);
      END_STATE();
    case 161:
      if (lookahead == 't') ADVANCE(72);
      END_STATE();
    case 162:
      if (lookahead == 't') ADVANCE(80);
      END_STATE();
    case 163:
      if (lookahead == 't') ADVANCE(25);
      END_STATE();
    case 164:
      if (lookahead == 't') ADVANCE(26);
      END_STATE();
    case 165:
      if (lookahead == 't') ADVANCE(94);
      END_STATE();
    case 166:
      if (lookahead == 'u') ADVANCE(106);
      END_STATE();
    case 167:
      if (lookahead == 'u') ADVANCE(145);
      END_STATE();
    case 168:
      if (lookahead == 'u') ADVANCE(91);
      END_STATE();
    case 169:
      if (lookahead == 'u') ADVANCE(65);
      END_STATE();
    case 170:
      if (lookahead == 'u') ADVANCE(49);
      END_STATE();
    case 171:
      if (lookahead == 'v') ADVANCE(90);
      END_STATE();
    case 172:
      if (lookahead == 'v') ADVANCE(76);
      END_STATE();
    case 173:
      if (lookahead == 'x') ADVANCE(268);
      END_STATE();
    case 174:
      if (lookahead == 'x') ADVANCE(193);
      END_STATE();
    case 175:
      if (lookahead == 'x') ADVANCE(64);
      END_STATE();
    case 176:
      if (lookahead == 'x') ADVANCE(82);
      END_STATE();
    case 177:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(179);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(408);
      END_STATE();
    case 178:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(407);
      END_STATE();
    case 179:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(408);
      END_STATE();
    case 180:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(423);
      END_STATE();
    case 181:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(405);
      END_STATE();
    case 182:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(180);
      END_STATE();
    case 183:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(182);
      END_STATE();
    case 184:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(183);
      END_STATE();
    case 185:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(184);
      END_STATE();
    case 186:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(185);
      END_STATE();
    case 187:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(186);
      END_STATE();
    case 188:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(187);
      END_STATE();
    case 189:
      if (eof) ADVANCE(191);
      if (lookahead == '"') ADVANCE(409);
      if (lookahead == '\'') ADVANCE(416);
      if (lookahead == '(') ADVANCE(205);
      if (lookahead == ')') ADVANCE(206);
      if (lookahead == '+') ADVANCE(274);
      if (lookahead == ',') ADVANCE(215);
      if (lookahead == '-') ADVANCE(213);
      if (lookahead == '.') ADVANCE(208);
      if (lookahead == '/') ADVANCE(7);
      if (lookahead == '0') ADVANCE(401);
      if (lookahead == ':') ADVANCE(275);
      if (lookahead == ';') ADVANCE(192);
      if (lookahead == '<') ADVANCE(231);
      if (lookahead == '=') ADVANCE(194);
      if (lookahead == '>') ADVANCE(232);
      if (lookahead == '[') ADVANCE(214);
      if (lookahead == ']') ADVANCE(216);
      if (lookahead == 'b') ADVANCE(127);
      if (lookahead == 'd') ADVANCE(123);
      if (lookahead == 'e') ADVANCE(113);
      if (lookahead == 'f') ADVANCE(35);
      if (lookahead == 'i') ADVANCE(105);
      if (lookahead == 'm') ADVANCE(36);
      if (lookahead == 'n') ADVANCE(37);
      if (lookahead == 'o') ADVANCE(112);
      if (lookahead == 'p') ADVANCE(39);
      if (lookahead == 'r') ADVANCE(61);
      if (lookahead == 's') ADVANCE(62);
      if (lookahead == 't') ADVANCE(124);
      if (lookahead == 'u') ADVANCE(96);
      if (lookahead == 'w') ADVANCE(71);
      if (lookahead == '{') ADVANCE(211);
      if (lookahead == '}') ADVANCE(212);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(189)
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(399);
      END_STATE();
    case 190:
      if (eof) ADVANCE(191);
      if (lookahead == '"') ADVANCE(138);
      if (lookahead == '-') ADVANCE(213);
      if (lookahead == '.') ADVANCE(207);
      if (lookahead == '/') ADVANCE(7);
      if (lookahead == '0') ADVANCE(403);
      if (lookahead == ';') ADVANCE(192);
      if (lookahead == '=') ADVANCE(194);
      if (lookahead == 'e') ADVANCE(114);
      if (lookahead == 'i') ADVANCE(104);
      if (lookahead == 'm') ADVANCE(44);
      if (lookahead == 'o') ADVANCE(140);
      if (lookahead == 'p') ADVANCE(38);
      if (lookahead == 'r') ADVANCE(136);
      if (lookahead == 's') ADVANCE(63);
      if (lookahead == '}') ADVANCE(212);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(190)
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(400);
      END_STATE();
    case 191:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 192:
      ACCEPT_TOKEN(anon_sym_SEMI);
      END_STATE();
    case 193:
      ACCEPT_TOKEN(anon_sym_syntax);
      END_STATE();
    case 194:
      ACCEPT_TOKEN(anon_sym_EQ);
      END_STATE();
    case 195:
      ACCEPT_TOKEN(anon_sym_DQUOTEproto3_DQUOTE);
      END_STATE();
    case 196:
      ACCEPT_TOKEN(anon_sym_DQUOTEproto2_DQUOTE);
      END_STATE();
    case 197:
      ACCEPT_TOKEN(anon_sym_import);
      END_STATE();
    case 198:
      ACCEPT_TOKEN(anon_sym_weak);
      END_STATE();
    case 199:
      ACCEPT_TOKEN(anon_sym_public);
      END_STATE();
    case 200:
      ACCEPT_TOKEN(anon_sym_package);
      END_STATE();
    case 201:
      ACCEPT_TOKEN(anon_sym_option);
      END_STATE();
    case 202:
      ACCEPT_TOKEN(anon_sym_option);
      if (lookahead == 'a') ADVANCE(100);
      END_STATE();
    case 203:
      ACCEPT_TOKEN(anon_sym_option);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'a') ADVANCE(335);
      END_STATE();
    case 204:
      ACCEPT_TOKEN(anon_sym_option);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 205:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      END_STATE();
    case 206:
      ACCEPT_TOKEN(anon_sym_RPAREN);
      END_STATE();
    case 207:
      ACCEPT_TOKEN(anon_sym_DOT);
      END_STATE();
    case 208:
      ACCEPT_TOKEN(anon_sym_DOT);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(407);
      END_STATE();
    case 209:
      ACCEPT_TOKEN(anon_sym_enum);
      END_STATE();
    case 210:
      ACCEPT_TOKEN(anon_sym_enum);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 211:
      ACCEPT_TOKEN(anon_sym_LBRACE);
      END_STATE();
    case 212:
      ACCEPT_TOKEN(anon_sym_RBRACE);
      END_STATE();
    case 213:
      ACCEPT_TOKEN(anon_sym_DASH);
      END_STATE();
    case 214:
      ACCEPT_TOKEN(anon_sym_LBRACK);
      END_STATE();
    case 215:
      ACCEPT_TOKEN(anon_sym_COMMA);
      END_STATE();
    case 216:
      ACCEPT_TOKEN(anon_sym_RBRACK);
      END_STATE();
    case 217:
      ACCEPT_TOKEN(anon_sym_message);
      END_STATE();
    case 218:
      ACCEPT_TOKEN(anon_sym_message);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 219:
      ACCEPT_TOKEN(anon_sym_extend);
      END_STATE();
    case 220:
      ACCEPT_TOKEN(anon_sym_extend);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 221:
      ACCEPT_TOKEN(anon_sym_optional);
      END_STATE();
    case 222:
      ACCEPT_TOKEN(anon_sym_optional);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 223:
      ACCEPT_TOKEN(anon_sym_required);
      END_STATE();
    case 224:
      ACCEPT_TOKEN(anon_sym_required);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 225:
      ACCEPT_TOKEN(anon_sym_repeated);
      END_STATE();
    case 226:
      ACCEPT_TOKEN(anon_sym_repeated);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 227:
      ACCEPT_TOKEN(anon_sym_oneof);
      END_STATE();
    case 228:
      ACCEPT_TOKEN(anon_sym_oneof);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 229:
      ACCEPT_TOKEN(anon_sym_map);
      END_STATE();
    case 230:
      ACCEPT_TOKEN(anon_sym_map);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 231:
      ACCEPT_TOKEN(anon_sym_LT);
      END_STATE();
    case 232:
      ACCEPT_TOKEN(anon_sym_GT);
      END_STATE();
    case 233:
      ACCEPT_TOKEN(anon_sym_int32);
      END_STATE();
    case 234:
      ACCEPT_TOKEN(anon_sym_int32);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 235:
      ACCEPT_TOKEN(anon_sym_int64);
      END_STATE();
    case 236:
      ACCEPT_TOKEN(anon_sym_int64);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 237:
      ACCEPT_TOKEN(anon_sym_uint32);
      END_STATE();
    case 238:
      ACCEPT_TOKEN(anon_sym_uint32);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 239:
      ACCEPT_TOKEN(anon_sym_uint64);
      END_STATE();
    case 240:
      ACCEPT_TOKEN(anon_sym_uint64);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 241:
      ACCEPT_TOKEN(anon_sym_sint32);
      END_STATE();
    case 242:
      ACCEPT_TOKEN(anon_sym_sint32);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 243:
      ACCEPT_TOKEN(anon_sym_sint64);
      END_STATE();
    case 244:
      ACCEPT_TOKEN(anon_sym_sint64);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 245:
      ACCEPT_TOKEN(anon_sym_fixed32);
      END_STATE();
    case 246:
      ACCEPT_TOKEN(anon_sym_fixed32);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 247:
      ACCEPT_TOKEN(anon_sym_fixed64);
      END_STATE();
    case 248:
      ACCEPT_TOKEN(anon_sym_fixed64);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 249:
      ACCEPT_TOKEN(anon_sym_sfixed32);
      END_STATE();
    case 250:
      ACCEPT_TOKEN(anon_sym_sfixed32);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 251:
      ACCEPT_TOKEN(anon_sym_sfixed64);
      END_STATE();
    case 252:
      ACCEPT_TOKEN(anon_sym_sfixed64);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 253:
      ACCEPT_TOKEN(anon_sym_bool);
      END_STATE();
    case 254:
      ACCEPT_TOKEN(anon_sym_bool);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 255:
      ACCEPT_TOKEN(anon_sym_string);
      END_STATE();
    case 256:
      ACCEPT_TOKEN(anon_sym_string);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 257:
      ACCEPT_TOKEN(anon_sym_double);
      END_STATE();
    case 258:
      ACCEPT_TOKEN(anon_sym_double);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 259:
      ACCEPT_TOKEN(anon_sym_float);
      END_STATE();
    case 260:
      ACCEPT_TOKEN(anon_sym_float);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 261:
      ACCEPT_TOKEN(anon_sym_bytes);
      END_STATE();
    case 262:
      ACCEPT_TOKEN(anon_sym_bytes);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 263:
      ACCEPT_TOKEN(anon_sym_reserved);
      END_STATE();
    case 264:
      ACCEPT_TOKEN(anon_sym_reserved);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 265:
      ACCEPT_TOKEN(anon_sym_extensions);
      END_STATE();
    case 266:
      ACCEPT_TOKEN(anon_sym_extensions);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 267:
      ACCEPT_TOKEN(anon_sym_to);
      END_STATE();
    case 268:
      ACCEPT_TOKEN(anon_sym_max);
      END_STATE();
    case 269:
      ACCEPT_TOKEN(anon_sym_service);
      END_STATE();
    case 270:
      ACCEPT_TOKEN(anon_sym_rpc);
      END_STATE();
    case 271:
      ACCEPT_TOKEN(anon_sym_stream);
      END_STATE();
    case 272:
      ACCEPT_TOKEN(anon_sym_stream);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 273:
      ACCEPT_TOKEN(anon_sym_returns);
      END_STATE();
    case 274:
      ACCEPT_TOKEN(anon_sym_PLUS);
      END_STATE();
    case 275:
      ACCEPT_TOKEN(anon_sym_COLON);
      END_STATE();
    case 276:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '2') ADVANCE(234);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 277:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '2') ADVANCE(242);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 278:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '2') ADVANCE(238);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 279:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '2') ADVANCE(246);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 280:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '2') ADVANCE(250);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 281:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '3') ADVANCE(276);
      if (lookahead == '6') ADVANCE(286);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 282:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '3') ADVANCE(277);
      if (lookahead == '6') ADVANCE(287);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 283:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '3') ADVANCE(278);
      if (lookahead == '6') ADVANCE(288);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 284:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '3') ADVANCE(279);
      if (lookahead == '6') ADVANCE(289);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 285:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '3') ADVANCE(280);
      if (lookahead == '6') ADVANCE(290);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 286:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '4') ADVANCE(236);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 287:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '4') ADVANCE(244);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 288:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '4') ADVANCE(240);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 289:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '4') ADVANCE(248);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 290:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '4') ADVANCE(252);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 291:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'e') ||
          lookahead == 'g' ||
          lookahead == 'h' ||
          ('j' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'f') ADVANCE(333);
      if (lookahead == 'i') ADVANCE(348);
      if (lookahead == 't') ADVANCE(361);
      END_STATE();
    case 292:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'o') ||
          lookahead == 'r' ||
          ('t' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'p') ADVANCE(316);
      if (lookahead == 'q') ADVANCE(383);
      if (lookahead == 's') ADVANCE(312);
      END_STATE();
    case 293:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'c') ||
          ('e' <= lookahead && lookahead <= 'r') ||
          ('t' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'd') ADVANCE(220);
      if (lookahead == 's') ADVANCE(330);
      END_STATE();
    case 294:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'a') ADVANCE(357);
      if (lookahead == 'e') ADVANCE(367);
      END_STATE();
    case 295:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'h') ||
          lookahead == 'j' ||
          lookahead == 'k' ||
          ('m' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'i') ADVANCE(386);
      if (lookahead == 'l') ADVANCE(352);
      END_STATE();
    case 296:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          lookahead == 'o' ||
          ('q' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(318);
      if (lookahead == 'p') ADVANCE(374);
      END_STATE();
    case 297:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'w') ||
          lookahead == 'y' ||
          lookahead == 'z') ADVANCE(394);
      if (lookahead == 'n') ADVANCE(382);
      if (lookahead == 'x') ADVANCE(375);
      END_STATE();
    case 298:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'n') ||
          ('p' <= lookahead && lookahead <= 'x') ||
          lookahead == 'z') ADVANCE(394);
      if (lookahead == 'o') ADVANCE(351);
      if (lookahead == 'y') ADVANCE(373);
      END_STATE();
    case 299:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          lookahead == 'a' ||
          ('c' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'b') ADVANCE(337);
      END_STATE();
    case 300:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'c') ||
          ('e' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'd') ADVANCE(226);
      END_STATE();
    case 301:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'c') ||
          ('e' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'd') ADVANCE(224);
      END_STATE();
    case 302:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'c') ||
          ('e' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'd') ADVANCE(264);
      END_STATE();
    case 303:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'c') ||
          ('e' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'd') ADVANCE(284);
      END_STATE();
    case 304:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'c') ||
          ('e' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'd') ADVANCE(285);
      END_STATE();
    case 305:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(292);
      END_STATE();
    case 306:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(303);
      END_STATE();
    case 307:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(258);
      END_STATE();
    case 308:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(218);
      END_STATE();
    case 309:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(396);
      END_STATE();
    case 310:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(398);
      END_STATE();
    case 311:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(341);
      END_STATE();
    case 312:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(360);
      END_STATE();
    case 313:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(300);
      END_STATE();
    case 314:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(358);
      END_STATE();
    case 315:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(365);
      END_STATE();
    case 316:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(393);
      END_STATE();
    case 317:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(301);
      END_STATE();
    case 318:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(353);
      END_STATE();
    case 319:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(302);
      END_STATE();
    case 320:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(389);
      END_STATE();
    case 321:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(369);
      END_STATE();
    case 322:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'd') ||
          ('f' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'e') ADVANCE(304);
      END_STATE();
    case 323:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'e') ||
          ('g' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'f') ADVANCE(394);
      END_STATE();
    case 324:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'e') ||
          ('g' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'f') ADVANCE(228);
      END_STATE();
    case 325:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'f') ||
          ('h' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'g') ADVANCE(256);
      END_STATE();
    case 326:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'f') ||
          ('h' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'g') ADVANCE(308);
      END_STATE();
    case 327:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'h') ||
          ('j' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'i') ADVANCE(346);
      END_STATE();
    case 328:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'h') ||
          ('j' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'i') ADVANCE(363);
      END_STATE();
    case 329:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'h') ||
          ('j' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'i') ADVANCE(354);
      END_STATE();
    case 330:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'h') ||
          ('j' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'i') ADVANCE(355);
      END_STATE();
    case 331:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'h') ||
          ('j' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'i') ADVANCE(356);
      END_STATE();
    case 332:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'h') ||
          ('j' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'i') ADVANCE(349);
      END_STATE();
    case 333:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'h') ||
          ('j' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'i') ADVANCE(387);
      END_STATE();
    case 334:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'k') ||
          ('m' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'l') ADVANCE(254);
      END_STATE();
    case 335:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'k') ||
          ('m' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'l') ADVANCE(222);
      END_STATE();
    case 336:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'k') ||
          ('m' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'l') ADVANCE(370);
      END_STATE();
    case 337:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'k') ||
          ('m' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'l') ADVANCE(307);
      END_STATE();
    case 338:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'l') ||
          ('n' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'm') ADVANCE(210);
      END_STATE();
    case 339:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'l') ||
          ('n' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'm') ADVANCE(272);
      END_STATE();
    case 340:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(394);
      END_STATE();
    case 341:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(293);
      END_STATE();
    case 342:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(203);
      END_STATE();
    case 343:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(204);
      END_STATE();
    case 344:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(371);
      END_STATE();
    case 345:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(323);
      END_STATE();
    case 346:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(325);
      END_STATE();
    case 347:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(366);
      END_STATE();
    case 348:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(377);
      END_STATE();
    case 349:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'm') ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'n') ADVANCE(379);
      END_STATE();
    case 350:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'n') ||
          ('p' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'o') ADVANCE(381);
      END_STATE();
    case 351:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'n') ||
          ('p' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'o') ADVANCE(334);
      END_STATE();
    case 352:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'n') ||
          ('p' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'o') ADVANCE(390);
      END_STATE();
    case 353:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'n') ||
          ('p' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'o') ADVANCE(324);
      END_STATE();
    case 354:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'n') ||
          ('p' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'o') ADVANCE(342);
      END_STATE();
    case 355:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'n') ||
          ('p' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'o') ADVANCE(347);
      END_STATE();
    case 356:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'n') ||
          ('p' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'o') ADVANCE(343);
      END_STATE();
    case 357:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'o') ||
          ('q' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'p') ADVANCE(230);
      END_STATE();
    case 358:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'o') ||
          ('q' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'p') ADVANCE(316);
      END_STATE();
    case 359:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'o') ||
          ('q' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'p') ADVANCE(380);
      END_STATE();
    case 360:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'q') ||
          ('s' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'r') ADVANCE(385);
      END_STATE();
    case 361:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'q') ||
          ('s' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'r') ADVANCE(327);
      END_STATE();
    case 362:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'q') ||
          ('s' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'r') ADVANCE(384);
      END_STATE();
    case 363:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'q') ||
          ('s' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'r') ADVANCE(317);
      END_STATE();
    case 364:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'q') ||
          ('s' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'r') ADVANCE(320);
      END_STATE();
    case 365:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'r') ||
          ('t' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 's') ADVANCE(262);
      END_STATE();
    case 366:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'r') ||
          ('t' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 's') ADVANCE(266);
      END_STATE();
    case 367:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'r') ||
          ('t' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 's') ADVANCE(368);
      END_STATE();
    case 368:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'r') ||
          ('t' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 's') ADVANCE(388);
      END_STATE();
    case 369:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'r') ||
          ('t' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 's') ADVANCE(312);
      END_STATE();
    case 370:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'r') ||
          ('t' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 's') ADVANCE(310);
      END_STATE();
    case 371:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(281);
      END_STATE();
    case 372:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(260);
      END_STATE();
    case 373:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(315);
      END_STATE();
    case 374:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(329);
      END_STATE();
    case 375:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(311);
      END_STATE();
    case 376:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(313);
      END_STATE();
    case 377:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(282);
      END_STATE();
    case 378:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(364);
      END_STATE();
    case 379:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(283);
      END_STATE();
    case 380:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 's') ||
          ('u' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 't') ADVANCE(331);
      END_STATE();
    case 381:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 't') ||
          ('v' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'u') ADVANCE(299);
      END_STATE();
    case 382:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 't') ||
          ('v' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'u') ADVANCE(338);
      END_STATE();
    case 383:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 't') ||
          ('v' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'u') ADVANCE(328);
      END_STATE();
    case 384:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 't') ||
          ('v' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'u') ADVANCE(309);
      END_STATE();
    case 385:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'u') ||
          ('w' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'v') ADVANCE(319);
      END_STATE();
    case 386:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'w') ||
          lookahead == 'y' ||
          lookahead == 'z') ADVANCE(394);
      if (lookahead == 'x') ADVANCE(306);
      END_STATE();
    case 387:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'w') ||
          lookahead == 'y' ||
          lookahead == 'z') ADVANCE(394);
      if (lookahead == 'x') ADVANCE(322);
      END_STATE();
    case 388:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'a') ADVANCE(326);
      END_STATE();
    case 389:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'a') ADVANCE(339);
      END_STATE();
    case 390:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'a') ADVANCE(372);
      END_STATE();
    case 391:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'a') ADVANCE(336);
      END_STATE();
    case 392:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'a') ADVANCE(340);
      END_STATE();
    case 393:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      if (lookahead == 'a') ADVANCE(376);
      END_STATE();
    case 394:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 395:
      ACCEPT_TOKEN(sym_true);
      END_STATE();
    case 396:
      ACCEPT_TOKEN(sym_true);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 397:
      ACCEPT_TOKEN(sym_false);
      END_STATE();
    case 398:
      ACCEPT_TOKEN(sym_false);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(394);
      END_STATE();
    case 399:
      ACCEPT_TOKEN(sym_decimal_lit);
      if (lookahead == '.') ADVANCE(407);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(177);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(399);
      END_STATE();
    case 400:
      ACCEPT_TOKEN(sym_decimal_lit);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(400);
      END_STATE();
    case 401:
      ACCEPT_TOKEN(sym_octal_lit);
      if (lookahead == '.') ADVANCE(407);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(177);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(181);
      if (lookahead == '8' ||
          lookahead == '9') ADVANCE(10);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(402);
      END_STATE();
    case 402:
      ACCEPT_TOKEN(sym_octal_lit);
      if (lookahead == '.') ADVANCE(407);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(177);
      if (lookahead == '8' ||
          lookahead == '9') ADVANCE(10);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(402);
      END_STATE();
    case 403:
      ACCEPT_TOKEN(sym_octal_lit);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(181);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(404);
      END_STATE();
    case 404:
      ACCEPT_TOKEN(sym_octal_lit);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(404);
      END_STATE();
    case 405:
      ACCEPT_TOKEN(sym_hex_lit);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(405);
      END_STATE();
    case 406:
      ACCEPT_TOKEN(sym_float_lit);
      END_STATE();
    case 407:
      ACCEPT_TOKEN(sym_float_lit);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(177);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(407);
      END_STATE();
    case 408:
      ACCEPT_TOKEN(sym_float_lit);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(408);
      END_STATE();
    case 409:
      ACCEPT_TOKEN(anon_sym_DQUOTE);
      END_STATE();
    case 410:
      ACCEPT_TOKEN(aux_sym_string_token1);
      if (lookahead == '\n') ADVANCE(415);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(410);
      END_STATE();
    case 411:
      ACCEPT_TOKEN(aux_sym_string_token1);
      if (lookahead == '*') ADVANCE(413);
      if (lookahead == '/') ADVANCE(410);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(415);
      END_STATE();
    case 412:
      ACCEPT_TOKEN(aux_sym_string_token1);
      if (lookahead == '*') ADVANCE(412);
      if (lookahead == '/') ADVANCE(415);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(413);
      END_STATE();
    case 413:
      ACCEPT_TOKEN(aux_sym_string_token1);
      if (lookahead == '*') ADVANCE(412);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(413);
      END_STATE();
    case 414:
      ACCEPT_TOKEN(aux_sym_string_token1);
      if (lookahead == '/') ADVANCE(411);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(414);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(415);
      END_STATE();
    case 415:
      ACCEPT_TOKEN(aux_sym_string_token1);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(415);
      END_STATE();
    case 416:
      ACCEPT_TOKEN(anon_sym_SQUOTE);
      END_STATE();
    case 417:
      ACCEPT_TOKEN(aux_sym_string_token2);
      if (lookahead == '\n') ADVANCE(422);
      if (lookahead != 0 &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(417);
      END_STATE();
    case 418:
      ACCEPT_TOKEN(aux_sym_string_token2);
      if (lookahead == '*') ADVANCE(420);
      if (lookahead == '/') ADVANCE(417);
      if (lookahead != 0 &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(422);
      END_STATE();
    case 419:
      ACCEPT_TOKEN(aux_sym_string_token2);
      if (lookahead == '*') ADVANCE(419);
      if (lookahead == '/') ADVANCE(422);
      if (lookahead != 0 &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(420);
      END_STATE();
    case 420:
      ACCEPT_TOKEN(aux_sym_string_token2);
      if (lookahead == '*') ADVANCE(419);
      if (lookahead != 0 &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(420);
      END_STATE();
    case 421:
      ACCEPT_TOKEN(aux_sym_string_token2);
      if (lookahead == '/') ADVANCE(418);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(421);
      if (lookahead != 0 &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(422);
      END_STATE();
    case 422:
      ACCEPT_TOKEN(aux_sym_string_token2);
      if (lookahead != 0 &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(422);
      END_STATE();
    case 423:
      ACCEPT_TOKEN(sym_escape_sequence);
      END_STATE();
    case 424:
      ACCEPT_TOKEN(sym_escape_sequence);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(423);
      END_STATE();
    case 425:
      ACCEPT_TOKEN(sym_escape_sequence);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(424);
      END_STATE();
    case 426:
      ACCEPT_TOKEN(sym_comment);
      END_STATE();
    case 427:
      ACCEPT_TOKEN(sym_comment);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(427);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 190},
  [2] = {.lex_state = 12},
  [3] = {.lex_state = 12},
  [4] = {.lex_state = 12},
  [5] = {.lex_state = 12},
  [6] = {.lex_state = 12},
  [7] = {.lex_state = 12},
  [8] = {.lex_state = 12},
  [9] = {.lex_state = 12},
  [10] = {.lex_state = 12},
  [11] = {.lex_state = 12},
  [12] = {.lex_state = 12},
  [13] = {.lex_state = 12},
  [14] = {.lex_state = 12},
  [15] = {.lex_state = 12},
  [16] = {.lex_state = 12},
  [17] = {.lex_state = 12},
  [18] = {.lex_state = 12},
  [19] = {.lex_state = 12},
  [20] = {.lex_state = 12},
  [21] = {.lex_state = 12},
  [22] = {.lex_state = 12},
  [23] = {.lex_state = 12},
  [24] = {.lex_state = 12},
  [25] = {.lex_state = 12},
  [26] = {.lex_state = 12},
  [27] = {.lex_state = 12},
  [28] = {.lex_state = 11},
  [29] = {.lex_state = 11},
  [30] = {.lex_state = 11},
  [31] = {.lex_state = 11},
  [32] = {.lex_state = 2},
  [33] = {.lex_state = 11},
  [34] = {.lex_state = 2},
  [35] = {.lex_state = 11},
  [36] = {.lex_state = 13},
  [37] = {.lex_state = 2},
  [38] = {.lex_state = 2},
  [39] = {.lex_state = 14},
  [40] = {.lex_state = 2},
  [41] = {.lex_state = 2},
  [42] = {.lex_state = 11},
  [43] = {.lex_state = 14},
  [44] = {.lex_state = 14},
  [45] = {.lex_state = 11},
  [46] = {.lex_state = 11},
  [47] = {.lex_state = 2},
  [48] = {.lex_state = 2},
  [49] = {.lex_state = 2},
  [50] = {.lex_state = 2},
  [51] = {.lex_state = 2},
  [52] = {.lex_state = 2},
  [53] = {.lex_state = 2},
  [54] = {.lex_state = 2},
  [55] = {.lex_state = 2},
  [56] = {.lex_state = 190},
  [57] = {.lex_state = 190},
  [58] = {.lex_state = 190},
  [59] = {.lex_state = 190},
  [60] = {.lex_state = 0},
  [61] = {.lex_state = 1},
  [62] = {.lex_state = 1},
  [63] = {.lex_state = 1},
  [64] = {.lex_state = 190},
  [65] = {.lex_state = 190},
  [66] = {.lex_state = 17},
  [67] = {.lex_state = 1},
  [68] = {.lex_state = 1},
  [69] = {.lex_state = 17},
  [70] = {.lex_state = 17},
  [71] = {.lex_state = 1},
  [72] = {.lex_state = 17},
  [73] = {.lex_state = 17},
  [74] = {.lex_state = 190},
  [75] = {.lex_state = 190},
  [76] = {.lex_state = 1},
  [77] = {.lex_state = 190},
  [78] = {.lex_state = 190},
  [79] = {.lex_state = 190},
  [80] = {.lex_state = 190},
  [81] = {.lex_state = 190},
  [82] = {.lex_state = 190},
  [83] = {.lex_state = 190},
  [84] = {.lex_state = 190},
  [85] = {.lex_state = 1},
  [86] = {.lex_state = 190},
  [87] = {.lex_state = 190},
  [88] = {.lex_state = 190},
  [89] = {.lex_state = 190},
  [90] = {.lex_state = 1},
  [91] = {.lex_state = 190},
  [92] = {.lex_state = 1},
  [93] = {.lex_state = 190},
  [94] = {.lex_state = 1},
  [95] = {.lex_state = 190},
  [96] = {.lex_state = 1},
  [97] = {.lex_state = 190},
  [98] = {.lex_state = 190},
  [99] = {.lex_state = 0},
  [100] = {.lex_state = 1},
  [101] = {.lex_state = 1},
  [102] = {.lex_state = 190},
  [103] = {.lex_state = 190},
  [104] = {.lex_state = 1},
  [105] = {.lex_state = 190},
  [106] = {.lex_state = 190},
  [107] = {.lex_state = 190},
  [108] = {.lex_state = 1},
  [109] = {.lex_state = 17},
  [110] = {.lex_state = 17},
  [111] = {.lex_state = 17},
  [112] = {.lex_state = 1},
  [113] = {.lex_state = 1},
  [114] = {.lex_state = 1},
  [115] = {.lex_state = 1},
  [116] = {.lex_state = 190},
  [117] = {.lex_state = 17},
  [118] = {.lex_state = 15},
  [119] = {.lex_state = 1},
  [120] = {.lex_state = 1},
  [121] = {.lex_state = 1},
  [122] = {.lex_state = 17},
  [123] = {.lex_state = 190},
  [124] = {.lex_state = 190},
  [125] = {.lex_state = 1},
  [126] = {.lex_state = 15},
  [127] = {.lex_state = 190},
  [128] = {.lex_state = 17},
  [129] = {.lex_state = 1},
  [130] = {.lex_state = 1},
  [131] = {.lex_state = 190},
  [132] = {.lex_state = 16},
  [133] = {.lex_state = 190},
  [134] = {.lex_state = 15},
  [135] = {.lex_state = 0},
  [136] = {.lex_state = 1},
  [137] = {.lex_state = 1},
  [138] = {.lex_state = 17},
  [139] = {.lex_state = 190},
  [140] = {.lex_state = 16},
  [141] = {.lex_state = 1},
  [142] = {.lex_state = 17},
  [143] = {.lex_state = 190},
  [144] = {.lex_state = 1},
  [145] = {.lex_state = 190},
  [146] = {.lex_state = 0},
  [147] = {.lex_state = 3},
  [148] = {.lex_state = 6},
  [149] = {.lex_state = 190},
  [150] = {.lex_state = 3},
  [151] = {.lex_state = 190},
  [152] = {.lex_state = 1},
  [153] = {.lex_state = 6},
  [154] = {.lex_state = 190},
  [155] = {.lex_state = 3},
  [156] = {.lex_state = 6},
  [157] = {.lex_state = 190},
  [158] = {.lex_state = 190},
  [159] = {.lex_state = 1},
  [160] = {.lex_state = 1},
  [161] = {.lex_state = 1},
  [162] = {.lex_state = 1},
  [163] = {.lex_state = 1},
  [164] = {.lex_state = 1},
  [165] = {.lex_state = 1},
  [166] = {.lex_state = 1},
  [167] = {.lex_state = 1},
  [168] = {.lex_state = 1},
  [169] = {.lex_state = 1},
  [170] = {.lex_state = 0},
  [171] = {.lex_state = 0},
  [172] = {.lex_state = 1},
  [173] = {.lex_state = 0},
  [174] = {.lex_state = 0},
  [175] = {.lex_state = 1},
  [176] = {.lex_state = 0},
  [177] = {.lex_state = 0},
  [178] = {.lex_state = 1},
  [179] = {.lex_state = 1},
  [180] = {.lex_state = 0},
  [181] = {.lex_state = 1},
  [182] = {.lex_state = 1},
  [183] = {.lex_state = 1},
  [184] = {.lex_state = 190},
  [185] = {.lex_state = 0},
  [186] = {.lex_state = 190},
  [187] = {.lex_state = 0},
  [188] = {.lex_state = 0},
  [189] = {.lex_state = 1},
  [190] = {.lex_state = 190},
  [191] = {.lex_state = 1},
  [192] = {.lex_state = 0},
  [193] = {.lex_state = 0},
  [194] = {.lex_state = 0},
  [195] = {.lex_state = 0},
  [196] = {.lex_state = 0},
  [197] = {.lex_state = 0},
  [198] = {.lex_state = 190},
  [199] = {.lex_state = 0},
  [200] = {.lex_state = 0},
  [201] = {.lex_state = 0},
  [202] = {.lex_state = 0},
  [203] = {.lex_state = 0},
  [204] = {.lex_state = 0},
  [205] = {.lex_state = 0},
  [206] = {.lex_state = 1},
  [207] = {.lex_state = 1},
  [208] = {.lex_state = 0},
  [209] = {.lex_state = 1},
  [210] = {.lex_state = 0},
  [211] = {.lex_state = 0},
  [212] = {.lex_state = 0},
  [213] = {.lex_state = 0},
  [214] = {.lex_state = 0},
  [215] = {.lex_state = 1},
  [216] = {.lex_state = 0},
  [217] = {.lex_state = 190},
  [218] = {.lex_state = 1},
  [219] = {.lex_state = 1},
  [220] = {.lex_state = 1},
  [221] = {.lex_state = 0},
  [222] = {.lex_state = 0},
  [223] = {.lex_state = 0},
  [224] = {.lex_state = 1},
  [225] = {.lex_state = 0},
  [226] = {.lex_state = 0},
  [227] = {.lex_state = 0},
  [228] = {.lex_state = 0},
  [229] = {.lex_state = 1},
  [230] = {.lex_state = 1},
  [231] = {.lex_state = 0},
  [232] = {.lex_state = 1},
  [233] = {.lex_state = 0},
  [234] = {.lex_state = 1},
  [235] = {.lex_state = 1},
  [236] = {.lex_state = 0},
  [237] = {.lex_state = 1},
  [238] = {.lex_state = 0},
  [239] = {.lex_state = 1},
  [240] = {.lex_state = 1},
  [241] = {.lex_state = 1},
  [242] = {.lex_state = 0},
  [243] = {.lex_state = 0},
  [244] = {.lex_state = 1},
  [245] = {.lex_state = 0},
  [246] = {.lex_state = 0},
  [247] = {.lex_state = 0},
  [248] = {.lex_state = 0},
  [249] = {.lex_state = 0},
  [250] = {.lex_state = 0},
  [251] = {.lex_state = 0},
  [252] = {.lex_state = 1},
  [253] = {.lex_state = 0},
  [254] = {.lex_state = 0},
  [255] = {.lex_state = 0},
  [256] = {.lex_state = 0},
  [257] = {.lex_state = 1},
  [258] = {.lex_state = 0},
  [259] = {.lex_state = 0},
  [260] = {.lex_state = 0},
  [261] = {.lex_state = 0},
  [262] = {.lex_state = 0},
  [263] = {.lex_state = 1},
  [264] = {.lex_state = 0},
  [265] = {.lex_state = 0},
  [266] = {.lex_state = 1},
  [267] = {.lex_state = 0},
  [268] = {.lex_state = 0},
  [269] = {.lex_state = 0},
  [270] = {.lex_state = 0},
  [271] = {.lex_state = 1},
  [272] = {.lex_state = 0},
  [273] = {.lex_state = 0},
  [274] = {.lex_state = 190},
  [275] = {.lex_state = 0},
  [276] = {.lex_state = 0},
  [277] = {.lex_state = 0},
  [278] = {.lex_state = 0},
  [279] = {.lex_state = 0},
  [280] = {.lex_state = 0},
  [281] = {.lex_state = 0},
  [282] = {.lex_state = 0},
  [283] = {.lex_state = 0},
  [284] = {.lex_state = 0},
  [285] = {.lex_state = 0},
  [286] = {.lex_state = 0},
  [287] = {.lex_state = 0},
  [288] = {.lex_state = 0},
  [289] = {.lex_state = 0},
  [290] = {.lex_state = 0},
  [291] = {.lex_state = 0},
  [292] = {.lex_state = 1},
  [293] = {.lex_state = 0},
  [294] = {.lex_state = 0},
  [295] = {.lex_state = 0},
  [296] = {.lex_state = 1},
  [297] = {.lex_state = 0},
  [298] = {.lex_state = 0},
  [299] = {.lex_state = 0},
  [300] = {.lex_state = 0},
  [301] = {.lex_state = 0},
  [302] = {.lex_state = 0},
  [303] = {.lex_state = 0},
  [304] = {.lex_state = 1},
  [305] = {.lex_state = 0},
  [306] = {.lex_state = 0},
  [307] = {.lex_state = 0},
  [308] = {.lex_state = 0},
  [309] = {.lex_state = 0},
  [310] = {.lex_state = 0},
  [311] = {.lex_state = 0},
  [312] = {.lex_state = 0},
  [313] = {.lex_state = 0},
  [314] = {.lex_state = 0},
  [315] = {.lex_state = 0},
  [316] = {.lex_state = 0},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [ts_builtin_sym_end] = ACTIONS(1),
    [anon_sym_SEMI] = ACTIONS(1),
    [anon_sym_syntax] = ACTIONS(1),
    [anon_sym_EQ] = ACTIONS(1),
    [anon_sym_import] = ACTIONS(1),
    [anon_sym_weak] = ACTIONS(1),
    [anon_sym_public] = ACTIONS(1),
    [anon_sym_package] = ACTIONS(1),
    [anon_sym_option] = ACTIONS(1),
    [anon_sym_LPAREN] = ACTIONS(1),
    [anon_sym_RPAREN] = ACTIONS(1),
    [anon_sym_DOT] = ACTIONS(1),
    [anon_sym_enum] = ACTIONS(1),
    [anon_sym_LBRACE] = ACTIONS(1),
    [anon_sym_RBRACE] = ACTIONS(1),
    [anon_sym_DASH] = ACTIONS(1),
    [anon_sym_LBRACK] = ACTIONS(1),
    [anon_sym_COMMA] = ACTIONS(1),
    [anon_sym_RBRACK] = ACTIONS(1),
    [anon_sym_message] = ACTIONS(1),
    [anon_sym_extend] = ACTIONS(1),
    [anon_sym_optional] = ACTIONS(1),
    [anon_sym_required] = ACTIONS(1),
    [anon_sym_repeated] = ACTIONS(1),
    [anon_sym_oneof] = ACTIONS(1),
    [anon_sym_map] = ACTIONS(1),
    [anon_sym_LT] = ACTIONS(1),
    [anon_sym_GT] = ACTIONS(1),
    [anon_sym_int32] = ACTIONS(1),
    [anon_sym_int64] = ACTIONS(1),
    [anon_sym_uint32] = ACTIONS(1),
    [anon_sym_uint64] = ACTIONS(1),
    [anon_sym_sint32] = ACTIONS(1),
    [anon_sym_sint64] = ACTIONS(1),
    [anon_sym_fixed32] = ACTIONS(1),
    [anon_sym_fixed64] = ACTIONS(1),
    [anon_sym_sfixed32] = ACTIONS(1),
    [anon_sym_sfixed64] = ACTIONS(1),
    [anon_sym_bool] = ACTIONS(1),
    [anon_sym_string] = ACTIONS(1),
    [anon_sym_double] = ACTIONS(1),
    [anon_sym_float] = ACTIONS(1),
    [anon_sym_bytes] = ACTIONS(1),
    [anon_sym_reserved] = ACTIONS(1),
    [anon_sym_extensions] = ACTIONS(1),
    [anon_sym_to] = ACTIONS(1),
    [anon_sym_max] = ACTIONS(1),
    [anon_sym_service] = ACTIONS(1),
    [anon_sym_rpc] = ACTIONS(1),
    [anon_sym_stream] = ACTIONS(1),
    [anon_sym_returns] = ACTIONS(1),
    [anon_sym_PLUS] = ACTIONS(1),
    [anon_sym_COLON] = ACTIONS(1),
    [sym_true] = ACTIONS(1),
    [sym_false] = ACTIONS(1),
    [sym_decimal_lit] = ACTIONS(1),
    [sym_octal_lit] = ACTIONS(1),
    [sym_hex_lit] = ACTIONS(1),
    [sym_float_lit] = ACTIONS(1),
    [anon_sym_DQUOTE] = ACTIONS(1),
    [anon_sym_SQUOTE] = ACTIONS(1),
    [sym_escape_sequence] = ACTIONS(1),
    [sym_comment] = ACTIONS(3),
  },
  [1] = {
    [sym_source_file] = STATE(297),
    [sym_empty_statement] = STATE(56),
    [sym_syntax] = STATE(57),
    [sym_import] = STATE(56),
    [sym_package] = STATE(56),
    [sym_option] = STATE(56),
    [sym_enum] = STATE(56),
    [sym_message] = STATE(56),
    [sym_extend] = STATE(56),
    [sym_service] = STATE(56),
    [aux_sym_source_file_repeat1] = STATE(56),
    [ts_builtin_sym_end] = ACTIONS(5),
    [anon_sym_SEMI] = ACTIONS(7),
    [anon_sym_syntax] = ACTIONS(9),
    [anon_sym_import] = ACTIONS(11),
    [anon_sym_package] = ACTIONS(13),
    [anon_sym_option] = ACTIONS(15),
    [anon_sym_enum] = ACTIONS(17),
    [anon_sym_message] = ACTIONS(19),
    [anon_sym_extend] = ACTIONS(21),
    [anon_sym_service] = ACTIONS(23),
    [sym_comment] = ACTIONS(3),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 20,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(25), 1,
      anon_sym_SEMI,
    ACTIONS(27), 1,
      anon_sym_option,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(31), 1,
      anon_sym_enum,
    ACTIONS(33), 1,
      anon_sym_RBRACE,
    ACTIONS(35), 1,
      anon_sym_message,
    ACTIONS(37), 1,
      anon_sym_extend,
    ACTIONS(41), 1,
      anon_sym_repeated,
    ACTIONS(43), 1,
      anon_sym_oneof,
    ACTIONS(45), 1,
      anon_sym_map,
    ACTIONS(49), 1,
      anon_sym_reserved,
    ACTIONS(51), 1,
      anon_sym_extensions,
    ACTIONS(53), 1,
      sym_identifier,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(263), 1,
      sym_type,
    ACTIONS(39), 2,
      anon_sym_optional,
      anon_sym_required,
    STATE(6), 11,
      sym_empty_statement,
      sym_option,
      sym_enum,
      sym_message,
      sym_extend,
      sym_field,
      sym_oneof,
      sym_map_field,
      sym_reserved,
      sym_extensions,
      aux_sym_message_body_repeat1,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [86] = 20,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(25), 1,
      anon_sym_SEMI,
    ACTIONS(27), 1,
      anon_sym_option,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(31), 1,
      anon_sym_enum,
    ACTIONS(35), 1,
      anon_sym_message,
    ACTIONS(37), 1,
      anon_sym_extend,
    ACTIONS(41), 1,
      anon_sym_repeated,
    ACTIONS(43), 1,
      anon_sym_oneof,
    ACTIONS(45), 1,
      anon_sym_map,
    ACTIONS(49), 1,
      anon_sym_reserved,
    ACTIONS(51), 1,
      anon_sym_extensions,
    ACTIONS(53), 1,
      sym_identifier,
    ACTIONS(55), 1,
      anon_sym_RBRACE,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(263), 1,
      sym_type,
    ACTIONS(39), 2,
      anon_sym_optional,
      anon_sym_required,
    STATE(5), 11,
      sym_empty_statement,
      sym_option,
      sym_enum,
      sym_message,
      sym_extend,
      sym_field,
      sym_oneof,
      sym_map_field,
      sym_reserved,
      sym_extensions,
      aux_sym_message_body_repeat1,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [172] = 20,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(25), 1,
      anon_sym_SEMI,
    ACTIONS(27), 1,
      anon_sym_option,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(31), 1,
      anon_sym_enum,
    ACTIONS(35), 1,
      anon_sym_message,
    ACTIONS(37), 1,
      anon_sym_extend,
    ACTIONS(41), 1,
      anon_sym_repeated,
    ACTIONS(43), 1,
      anon_sym_oneof,
    ACTIONS(45), 1,
      anon_sym_map,
    ACTIONS(49), 1,
      anon_sym_reserved,
    ACTIONS(51), 1,
      anon_sym_extensions,
    ACTIONS(53), 1,
      sym_identifier,
    ACTIONS(57), 1,
      anon_sym_RBRACE,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(263), 1,
      sym_type,
    ACTIONS(39), 2,
      anon_sym_optional,
      anon_sym_required,
    STATE(3), 11,
      sym_empty_statement,
      sym_option,
      sym_enum,
      sym_message,
      sym_extend,
      sym_field,
      sym_oneof,
      sym_map_field,
      sym_reserved,
      sym_extensions,
      aux_sym_message_body_repeat1,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [258] = 20,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(59), 1,
      anon_sym_SEMI,
    ACTIONS(62), 1,
      anon_sym_option,
    ACTIONS(65), 1,
      anon_sym_DOT,
    ACTIONS(68), 1,
      anon_sym_enum,
    ACTIONS(71), 1,
      anon_sym_RBRACE,
    ACTIONS(73), 1,
      anon_sym_message,
    ACTIONS(76), 1,
      anon_sym_extend,
    ACTIONS(82), 1,
      anon_sym_repeated,
    ACTIONS(85), 1,
      anon_sym_oneof,
    ACTIONS(88), 1,
      anon_sym_map,
    ACTIONS(94), 1,
      anon_sym_reserved,
    ACTIONS(97), 1,
      anon_sym_extensions,
    ACTIONS(100), 1,
      sym_identifier,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(263), 1,
      sym_type,
    ACTIONS(79), 2,
      anon_sym_optional,
      anon_sym_required,
    STATE(5), 11,
      sym_empty_statement,
      sym_option,
      sym_enum,
      sym_message,
      sym_extend,
      sym_field,
      sym_oneof,
      sym_map_field,
      sym_reserved,
      sym_extensions,
      aux_sym_message_body_repeat1,
    ACTIONS(91), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [344] = 20,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(25), 1,
      anon_sym_SEMI,
    ACTIONS(27), 1,
      anon_sym_option,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(31), 1,
      anon_sym_enum,
    ACTIONS(35), 1,
      anon_sym_message,
    ACTIONS(37), 1,
      anon_sym_extend,
    ACTIONS(41), 1,
      anon_sym_repeated,
    ACTIONS(43), 1,
      anon_sym_oneof,
    ACTIONS(45), 1,
      anon_sym_map,
    ACTIONS(49), 1,
      anon_sym_reserved,
    ACTIONS(51), 1,
      anon_sym_extensions,
    ACTIONS(53), 1,
      sym_identifier,
    ACTIONS(103), 1,
      anon_sym_RBRACE,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(263), 1,
      sym_type,
    ACTIONS(39), 2,
      anon_sym_optional,
      anon_sym_required,
    STATE(5), 11,
      sym_empty_statement,
      sym_option,
      sym_enum,
      sym_message,
      sym_extend,
      sym_field,
      sym_oneof,
      sym_map_field,
      sym_reserved,
      sym_extensions,
      aux_sym_message_body_repeat1,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [430] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(105), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(107), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [468] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(109), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(111), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [506] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(113), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(115), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [544] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(117), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(119), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [582] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(121), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(123), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [620] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(125), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(127), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [658] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(129), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(131), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [696] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(133), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(135), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [734] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(137), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(139), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [772] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(141), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(143), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [810] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(145), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(147), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [848] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(149), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(151), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [886] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(153), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(155), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [924] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(157), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(159), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [962] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(161), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(163), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [1000] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(165), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(167), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [1038] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(169), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(171), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [1076] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(173), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(175), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [1114] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(177), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(179), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [1152] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(181), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(183), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [1190] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(185), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(187), 27,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_optional,
      anon_sym_required,
      anon_sym_repeated,
      anon_sym_oneof,
      anon_sym_map,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      anon_sym_reserved,
      anon_sym_extensions,
      sym_identifier,
  [1228] = 11,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(189), 1,
      anon_sym_SEMI,
    ACTIONS(192), 1,
      anon_sym_option,
    ACTIONS(195), 1,
      anon_sym_DOT,
    ACTIONS(198), 1,
      anon_sym_RBRACE,
    ACTIONS(203), 1,
      sym_identifier,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(266), 1,
      sym_type,
    STATE(28), 4,
      sym_empty_statement,
      sym_option,
      sym_oneof_field,
      aux_sym_oneof_repeat1,
    ACTIONS(200), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [1279] = 11,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(53), 1,
      sym_identifier,
    ACTIONS(206), 1,
      anon_sym_SEMI,
    ACTIONS(208), 1,
      anon_sym_option,
    ACTIONS(210), 1,
      anon_sym_RBRACE,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(266), 1,
      sym_type,
    STATE(28), 4,
      sym_empty_statement,
      sym_option,
      sym_oneof_field,
      aux_sym_oneof_repeat1,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [1330] = 11,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(53), 1,
      sym_identifier,
    ACTIONS(206), 1,
      anon_sym_SEMI,
    ACTIONS(208), 1,
      anon_sym_option,
    ACTIONS(212), 1,
      anon_sym_RBRACE,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(266), 1,
      sym_type,
    STATE(29), 4,
      sym_empty_statement,
      sym_option,
      sym_oneof_field,
      aux_sym_oneof_repeat1,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [1381] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(214), 4,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
    ACTIONS(216), 17,
      anon_sym_option,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      sym_identifier,
  [1410] = 15,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(222), 1,
      anon_sym_LBRACK,
    ACTIONS(224), 1,
      anon_sym_COLON,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(232), 1,
      sym_hex_lit,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(129), 1,
      sym_constant,
    ACTIONS(220), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(230), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [1463] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(244), 1,
      anon_sym_LBRACK,
    ACTIONS(240), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(242), 17,
      anon_sym_option,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      sym_identifier,
  [1494] = 15,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(232), 1,
      sym_hex_lit,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(246), 1,
      anon_sym_LBRACK,
    ACTIONS(248), 1,
      anon_sym_COLON,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(119), 1,
      sym_constant,
    ACTIONS(220), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(230), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [1547] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(250), 4,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
    ACTIONS(252), 17,
      anon_sym_option,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      sym_identifier,
  [1576] = 8,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(53), 1,
      sym_identifier,
    ACTIONS(254), 1,
      anon_sym_repeated,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(292), 1,
      sym_type,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [1615] = 14,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(258), 1,
      anon_sym_RBRACK,
    ACTIONS(262), 1,
      sym_hex_lit,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(203), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [1665] = 14,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    ACTIONS(264), 1,
      anon_sym_RBRACK,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(187), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [1715] = 7,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(53), 1,
      sym_identifier,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(257), 1,
      sym_type,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [1751] = 14,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    ACTIONS(266), 1,
      anon_sym_RBRACK,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(174), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [1801] = 14,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    ACTIONS(268), 1,
      anon_sym_RBRACK,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(188), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [1851] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(161), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(163), 17,
      anon_sym_option,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      sym_identifier,
  [1879] = 7,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(53), 1,
      sym_identifier,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(315), 1,
      sym_type,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [1915] = 7,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(53), 1,
      sym_identifier,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(244), 1,
      sym_message_or_enum_type,
    STATE(292), 1,
      sym_type,
    ACTIONS(47), 15,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
  [1951] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(145), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(147), 17,
      anon_sym_option,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      sym_identifier,
  [1979] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(270), 3,
      anon_sym_SEMI,
      anon_sym_DOT,
      anon_sym_RBRACE,
    ACTIONS(272), 17,
      anon_sym_option,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
      anon_sym_double,
      anon_sym_float,
      anon_sym_bytes,
      sym_identifier,
  [2007] = 14,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(232), 1,
      sym_hex_lit,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(274), 1,
      anon_sym_LBRACK,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(136), 1,
      sym_constant,
    ACTIONS(220), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(230), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [2057] = 14,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(232), 1,
      sym_hex_lit,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(276), 1,
      anon_sym_LBRACK,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(113), 1,
      sym_constant,
    ACTIONS(220), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(230), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [2107] = 13,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(213), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [2154] = 13,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(303), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [2201] = 13,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(256), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [2248] = 13,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(236), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [2295] = 13,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(298), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [2342] = 13,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(231), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [2389] = 13,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(218), 1,
      anon_sym_LBRACE,
    ACTIONS(226), 1,
      sym_identifier,
    ACTIONS(234), 1,
      sym_float_lit,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(262), 1,
      sym_hex_lit,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(302), 1,
      sym_constant,
    ACTIONS(228), 2,
      sym_true,
      sym_false,
    ACTIONS(256), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
    STATE(101), 5,
      sym_block_lit,
      sym_full_ident,
      sym_bool,
      sym_int_lit,
      sym_string,
  [2436] = 11,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(11), 1,
      anon_sym_import,
    ACTIONS(13), 1,
      anon_sym_package,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(17), 1,
      anon_sym_enum,
    ACTIONS(19), 1,
      anon_sym_message,
    ACTIONS(21), 1,
      anon_sym_extend,
    ACTIONS(23), 1,
      anon_sym_service,
    ACTIONS(278), 1,
      ts_builtin_sym_end,
    STATE(58), 9,
      sym_empty_statement,
      sym_import,
      sym_package,
      sym_option,
      sym_enum,
      sym_message,
      sym_extend,
      sym_service,
      aux_sym_source_file_repeat1,
  [2478] = 11,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(11), 1,
      anon_sym_import,
    ACTIONS(13), 1,
      anon_sym_package,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(17), 1,
      anon_sym_enum,
    ACTIONS(19), 1,
      anon_sym_message,
    ACTIONS(21), 1,
      anon_sym_extend,
    ACTIONS(23), 1,
      anon_sym_service,
    ACTIONS(278), 1,
      ts_builtin_sym_end,
    STATE(59), 9,
      sym_empty_statement,
      sym_import,
      sym_package,
      sym_option,
      sym_enum,
      sym_message,
      sym_extend,
      sym_service,
      aux_sym_source_file_repeat1,
  [2520] = 11,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(280), 1,
      ts_builtin_sym_end,
    ACTIONS(282), 1,
      anon_sym_SEMI,
    ACTIONS(285), 1,
      anon_sym_import,
    ACTIONS(288), 1,
      anon_sym_package,
    ACTIONS(291), 1,
      anon_sym_option,
    ACTIONS(294), 1,
      anon_sym_enum,
    ACTIONS(297), 1,
      anon_sym_message,
    ACTIONS(300), 1,
      anon_sym_extend,
    ACTIONS(303), 1,
      anon_sym_service,
    STATE(58), 9,
      sym_empty_statement,
      sym_import,
      sym_package,
      sym_option,
      sym_enum,
      sym_message,
      sym_extend,
      sym_service,
      aux_sym_source_file_repeat1,
  [2562] = 11,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(11), 1,
      anon_sym_import,
    ACTIONS(13), 1,
      anon_sym_package,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(17), 1,
      anon_sym_enum,
    ACTIONS(19), 1,
      anon_sym_message,
    ACTIONS(21), 1,
      anon_sym_extend,
    ACTIONS(23), 1,
      anon_sym_service,
    ACTIONS(306), 1,
      ts_builtin_sym_end,
    STATE(58), 9,
      sym_empty_statement,
      sym_import,
      sym_package,
      sym_option,
      sym_enum,
      sym_message,
      sym_extend,
      sym_service,
      aux_sym_source_file_repeat1,
  [2604] = 3,
    ACTIONS(3), 1,
      sym_comment,
    STATE(268), 1,
      sym_key_type,
    ACTIONS(308), 12,
      anon_sym_int32,
      anon_sym_int64,
      anon_sym_uint32,
      anon_sym_uint64,
      anon_sym_sint32,
      anon_sym_sint64,
      anon_sym_fixed32,
      anon_sym_fixed64,
      anon_sym_sfixed32,
      anon_sym_sfixed64,
      anon_sym_bool,
      anon_sym_string,
  [2625] = 11,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(260), 1,
      sym_octal_lit,
    ACTIONS(310), 1,
      sym_identifier,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(197), 1,
      sym_range,
    STATE(200), 1,
      sym_int_lit,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
    STATE(199), 2,
      sym__identifier_or_string,
      sym_string,
    STATE(301), 2,
      sym_ranges,
      sym_field_names,
  [2662] = 11,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(260), 1,
      sym_octal_lit,
    ACTIONS(310), 1,
      sym_identifier,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(197), 1,
      sym_range,
    STATE(200), 1,
      sym_int_lit,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
    STATE(199), 2,
      sym__identifier_or_string,
      sym_string,
    STATE(281), 2,
      sym_ranges,
      sym_field_names,
  [2699] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(314), 1,
      anon_sym_DOT,
    STATE(63), 1,
      aux_sym__option_name_repeat1,
    ACTIONS(312), 9,
      anon_sym_SEMI,
      anon_sym_EQ,
      anon_sym_RPAREN,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [2720] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(145), 11,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_RBRACE,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
      anon_sym_rpc,
  [2737] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(161), 11,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_RBRACE,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
      anon_sym_rpc,
  [2754] = 7,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(317), 1,
      anon_sym_SEMI,
    ACTIONS(319), 1,
      anon_sym_option,
    ACTIONS(321), 1,
      anon_sym_RBRACE,
    ACTIONS(323), 1,
      anon_sym_reserved,
    ACTIONS(325), 1,
      sym_identifier,
    STATE(72), 5,
      sym_empty_statement,
      sym_option,
      sym_enum_field,
      sym_reserved,
      aux_sym_enum_body_repeat1,
  [2780] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(312), 10,
      anon_sym_SEMI,
      anon_sym_EQ,
      anon_sym_RPAREN,
      anon_sym_DOT,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [2796] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(329), 1,
      anon_sym_DOT,
    STATE(63), 1,
      aux_sym__option_name_repeat1,
    ACTIONS(327), 8,
      anon_sym_SEMI,
      anon_sym_RPAREN,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [2816] = 7,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(317), 1,
      anon_sym_SEMI,
    ACTIONS(319), 1,
      anon_sym_option,
    ACTIONS(323), 1,
      anon_sym_reserved,
    ACTIONS(325), 1,
      sym_identifier,
    ACTIONS(331), 1,
      anon_sym_RBRACE,
    STATE(70), 5,
      sym_empty_statement,
      sym_option,
      sym_enum_field,
      sym_reserved,
      aux_sym_enum_body_repeat1,
  [2842] = 7,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(333), 1,
      anon_sym_SEMI,
    ACTIONS(336), 1,
      anon_sym_option,
    ACTIONS(339), 1,
      anon_sym_RBRACE,
    ACTIONS(341), 1,
      anon_sym_reserved,
    ACTIONS(344), 1,
      sym_identifier,
    STATE(70), 5,
      sym_empty_statement,
      sym_option,
      sym_enum_field,
      sym_reserved,
      aux_sym_enum_body_repeat1,
  [2868] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(329), 1,
      anon_sym_DOT,
    STATE(68), 1,
      aux_sym__option_name_repeat1,
    ACTIONS(347), 8,
      anon_sym_SEMI,
      anon_sym_RPAREN,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [2888] = 7,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(317), 1,
      anon_sym_SEMI,
    ACTIONS(319), 1,
      anon_sym_option,
    ACTIONS(323), 1,
      anon_sym_reserved,
    ACTIONS(325), 1,
      sym_identifier,
    ACTIONS(349), 1,
      anon_sym_RBRACE,
    STATE(70), 5,
      sym_empty_statement,
      sym_option,
      sym_enum_field,
      sym_reserved,
      aux_sym_enum_body_repeat1,
  [2914] = 7,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(317), 1,
      anon_sym_SEMI,
    ACTIONS(319), 1,
      anon_sym_option,
    ACTIONS(323), 1,
      anon_sym_reserved,
    ACTIONS(325), 1,
      sym_identifier,
    ACTIONS(351), 1,
      anon_sym_RBRACE,
    STATE(69), 5,
      sym_empty_statement,
      sym_option,
      sym_enum_field,
      sym_reserved,
      aux_sym_enum_body_repeat1,
  [2940] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(181), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [2955] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(149), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [2970] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    STATE(85), 1,
      aux_sym_string_repeat3,
    ACTIONS(353), 6,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [2991] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(355), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3006] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(357), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3021] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(185), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3036] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(359), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3051] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(361), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3066] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(169), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3081] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(363), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3096] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(365), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3111] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(369), 1,
      anon_sym_DQUOTE,
    ACTIONS(372), 1,
      anon_sym_SQUOTE,
    STATE(85), 1,
      aux_sym_string_repeat3,
    ACTIONS(367), 6,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [3132] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(173), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3147] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(141), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3162] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(121), 9,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_import,
      anon_sym_package,
      anon_sym_option,
      anon_sym_enum,
      anon_sym_message,
      anon_sym_extend,
      anon_sym_service,
  [3177] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(375), 1,
      anon_sym_SEMI,
    ACTIONS(378), 1,
      anon_sym_option,
    ACTIONS(381), 1,
      anon_sym_RBRACE,
    ACTIONS(383), 1,
      anon_sym_rpc,
    STATE(89), 4,
      sym_empty_statement,
      sym_option,
      sym_rpc,
      aux_sym_service_repeat1,
  [3199] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(386), 8,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
  [3213] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(388), 1,
      anon_sym_RBRACE,
    ACTIONS(390), 1,
      anon_sym_rpc,
    STATE(89), 4,
      sym_empty_statement,
      sym_option,
      sym_rpc,
      aux_sym_service_repeat1,
  [3235] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(367), 8,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
  [3249] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(390), 1,
      anon_sym_rpc,
    ACTIONS(392), 1,
      anon_sym_RBRACE,
    STATE(91), 4,
      sym_empty_statement,
      sym_option,
      sym_rpc,
      aux_sym_service_repeat1,
  [3271] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(394), 6,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [3283] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(396), 1,
      anon_sym_RBRACE,
    STATE(98), 3,
      sym_empty_statement,
      sym_option,
      aux_sym_rpc_repeat1,
  [3301] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(398), 6,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [3313] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(400), 1,
      anon_sym_RBRACE,
    STATE(105), 3,
      sym_empty_statement,
      sym_option,
      aux_sym_rpc_repeat1,
  [3331] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(402), 1,
      anon_sym_SEMI,
    ACTIONS(405), 1,
      anon_sym_option,
    ACTIONS(408), 1,
      anon_sym_RBRACE,
    STATE(98), 3,
      sym_empty_statement,
      sym_option,
      aux_sym_rpc_repeat1,
  [3349] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(289), 1,
      sym_string,
    ACTIONS(410), 2,
      anon_sym_weak,
      anon_sym_public,
  [3369] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(412), 6,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [3381] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(414), 6,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [3393] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(416), 1,
      anon_sym_RBRACE,
    STATE(95), 3,
      sym_empty_statement,
      sym_option,
      aux_sym_rpc_repeat1,
  [3411] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(396), 1,
      anon_sym_RBRACE,
    STATE(106), 3,
      sym_empty_statement,
      sym_option,
      aux_sym_rpc_repeat1,
  [3429] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    ACTIONS(418), 1,
      sym_identifier,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(216), 2,
      sym__identifier_or_string,
      sym_string,
  [3449] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(416), 1,
      anon_sym_RBRACE,
    STATE(98), 3,
      sym_empty_statement,
      sym_option,
      aux_sym_rpc_repeat1,
  [3467] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_SEMI,
    ACTIONS(15), 1,
      anon_sym_option,
    ACTIONS(420), 1,
      anon_sym_RBRACE,
    STATE(98), 3,
      sym_empty_statement,
      sym_option,
      aux_sym_rpc_repeat1,
  [3485] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(260), 1,
      sym_octal_lit,
    STATE(197), 1,
      sym_range,
    STATE(200), 1,
      sym_int_lit,
    STATE(300), 1,
      sym_ranges,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [3505] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(422), 6,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      sym_identifier,
  [3517] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(161), 2,
      anon_sym_SEMI,
      anon_sym_RBRACE,
    ACTIONS(163), 3,
      anon_sym_option,
      anon_sym_reserved,
      sym_identifier,
  [3530] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(424), 2,
      anon_sym_SEMI,
      anon_sym_RBRACE,
    ACTIONS(426), 3,
      anon_sym_option,
      anon_sym_reserved,
      sym_identifier,
  [3543] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(428), 2,
      anon_sym_SEMI,
      anon_sym_RBRACE,
    ACTIONS(430), 3,
      anon_sym_option,
      anon_sym_reserved,
      sym_identifier,
  [3556] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(204), 1,
      sym_field_option,
    STATE(253), 1,
      sym_field_options,
    STATE(295), 1,
      sym__option_name,
  [3575] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(436), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
    ACTIONS(438), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [3588] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(204), 1,
      sym_field_option,
    STATE(294), 1,
      sym_field_options,
    STATE(295), 1,
      sym__option_name,
  [3607] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(250), 5,
      anon_sym_SEMI,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      sym_identifier,
  [3618] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(260), 1,
      sym_octal_lit,
    STATE(31), 1,
      sym_int_lit,
    STATE(246), 1,
      sym_field_number,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [3635] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(145), 2,
      anon_sym_SEMI,
      anon_sym_RBRACE,
    ACTIONS(147), 3,
      anon_sym_option,
      anon_sym_reserved,
      sym_identifier,
  [3648] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(53), 1,
      sym_identifier,
    ACTIONS(440), 1,
      anon_sym_stream,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(251), 1,
      sym_message_or_enum_type,
  [3667] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(442), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
    ACTIONS(444), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [3680] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(204), 1,
      sym_field_option,
    STATE(269), 1,
      sym_field_options,
    STATE(295), 1,
      sym__option_name,
  [3699] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(446), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
    ACTIONS(448), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [3712] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(165), 2,
      anon_sym_SEMI,
      anon_sym_RBRACE,
    ACTIONS(167), 3,
      anon_sym_option,
      anon_sym_reserved,
      sym_identifier,
  [3725] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(260), 1,
      sym_octal_lit,
    STATE(200), 1,
      sym_int_lit,
    STATE(243), 1,
      sym_range,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [3742] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(260), 1,
      sym_octal_lit,
    STATE(31), 1,
      sym_int_lit,
    STATE(221), 1,
      sym_field_number,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [3759] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(450), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
    ACTIONS(452), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [3772] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(53), 1,
      sym_identifier,
    ACTIONS(454), 1,
      anon_sym_stream,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(261), 1,
      sym_message_or_enum_type,
  [3791] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(260), 1,
      sym_octal_lit,
    ACTIONS(456), 1,
      anon_sym_max,
    STATE(211), 1,
      sym_int_lit,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [3808] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(458), 2,
      anon_sym_SEMI,
      anon_sym_RBRACE,
    ACTIONS(460), 3,
      anon_sym_option,
      anon_sym_reserved,
      sym_identifier,
  [3821] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(462), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
    ACTIONS(464), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [3834] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(204), 1,
      sym_field_option,
    STATE(290), 1,
      sym_field_options,
    STATE(295), 1,
      sym__option_name,
  [3853] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(468), 1,
      sym_octal_lit,
    STATE(31), 1,
      sym_int_lit,
    STATE(33), 1,
      sym_field_number,
    ACTIONS(466), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [3870] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(232), 1,
      sym_hex_lit,
    ACTIONS(470), 1,
      sym_float_lit,
    STATE(96), 1,
      sym_int_lit,
    ACTIONS(230), 2,
      sym_decimal_lit,
      sym_octal_lit,
  [3887] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(260), 1,
      sym_octal_lit,
    STATE(31), 1,
      sym_int_lit,
    STATE(242), 1,
      sym_field_number,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [3904] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(53), 1,
      sym_identifier,
    ACTIONS(472), 1,
      anon_sym_stream,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(250), 1,
      sym_message_or_enum_type,
  [3923] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(250), 5,
      anon_sym_SEMI,
      anon_sym_LBRACK,
      anon_sym_COMMA,
      anon_sym_RBRACK,
      anon_sym_to,
  [3934] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(474), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
    ACTIONS(476), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [3947] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(478), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
    ACTIONS(480), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [3960] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(482), 2,
      anon_sym_SEMI,
      anon_sym_RBRACE,
    ACTIONS(484), 3,
      anon_sym_option,
      anon_sym_reserved,
      sym_identifier,
  [3973] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(260), 1,
      sym_octal_lit,
    STATE(31), 1,
      sym_int_lit,
    STATE(227), 1,
      sym_field_number,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [3990] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(262), 1,
      sym_hex_lit,
    ACTIONS(470), 1,
      sym_float_lit,
    STATE(96), 1,
      sym_int_lit,
    ACTIONS(260), 2,
      sym_decimal_lit,
      sym_octal_lit,
  [4007] = 6,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(204), 1,
      sym_field_option,
    STATE(262), 1,
      sym_field_options,
    STATE(295), 1,
      sym__option_name,
  [4026] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(486), 2,
      anon_sym_SEMI,
      anon_sym_RBRACE,
    ACTIONS(488), 3,
      anon_sym_option,
      anon_sym_reserved,
      sym_identifier,
  [4039] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(260), 1,
      sym_octal_lit,
    ACTIONS(490), 1,
      anon_sym_DASH,
    STATE(248), 1,
      sym_int_lit,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [4056] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(494), 1,
      anon_sym_DOT,
    ACTIONS(492), 3,
      anon_sym_RPAREN,
      anon_sym_GT,
      sym_identifier,
  [4068] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(260), 1,
      sym_octal_lit,
    STATE(210), 1,
      sym_int_lit,
    ACTIONS(262), 2,
      sym_decimal_lit,
      sym_hex_lit,
  [4082] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(236), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_SQUOTE,
    STATE(76), 1,
      aux_sym_string_repeat3,
    STATE(276), 1,
      sym_string,
  [4098] = 4,
    ACTIONS(496), 1,
      anon_sym_DQUOTE,
    ACTIONS(500), 1,
      sym_comment,
    STATE(150), 1,
      aux_sym_string_repeat1,
    ACTIONS(498), 2,
      aux_sym_string_token1,
      sym_escape_sequence,
  [4112] = 4,
    ACTIONS(496), 1,
      anon_sym_SQUOTE,
    ACTIONS(500), 1,
      sym_comment,
    STATE(153), 1,
      aux_sym_string_repeat2,
    ACTIONS(502), 2,
      aux_sym_string_token2,
      sym_escape_sequence,
  [4126] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(504), 4,
      anon_sym_SEMI,
      anon_sym_option,
      anon_sym_RBRACE,
      anon_sym_rpc,
  [4136] = 4,
    ACTIONS(500), 1,
      sym_comment,
    ACTIONS(506), 1,
      anon_sym_DQUOTE,
    STATE(155), 1,
      aux_sym_string_repeat1,
    ACTIONS(508), 2,
      aux_sym_string_token1,
      sym_escape_sequence,
  [4150] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(510), 4,
      anon_sym_SEMI,
      anon_sym_option,
      anon_sym_RBRACE,
      anon_sym_rpc,
  [4160] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(176), 1,
      sym_enum_value_option,
    STATE(259), 1,
      sym__option_name,
  [4176] = 4,
    ACTIONS(500), 1,
      sym_comment,
    ACTIONS(506), 1,
      anon_sym_SQUOTE,
    STATE(156), 1,
      aux_sym_string_repeat2,
    ACTIONS(512), 2,
      aux_sym_string_token2,
      sym_escape_sequence,
  [4190] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(514), 4,
      anon_sym_SEMI,
      anon_sym_option,
      anon_sym_RBRACE,
      anon_sym_rpc,
  [4200] = 4,
    ACTIONS(500), 1,
      sym_comment,
    ACTIONS(516), 1,
      anon_sym_DQUOTE,
    STATE(155), 1,
      aux_sym_string_repeat1,
    ACTIONS(518), 2,
      aux_sym_string_token1,
      sym_escape_sequence,
  [4214] = 4,
    ACTIONS(500), 1,
      sym_comment,
    ACTIONS(521), 1,
      anon_sym_SQUOTE,
    STATE(156), 1,
      aux_sym_string_repeat2,
    ACTIONS(523), 2,
      aux_sym_string_token2,
      sym_escape_sequence,
  [4228] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(526), 4,
      anon_sym_SEMI,
      anon_sym_option,
      anon_sym_RBRACE,
      anon_sym_rpc,
  [4238] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(528), 4,
      anon_sym_SEMI,
      anon_sym_option,
      anon_sym_RBRACE,
      anon_sym_rpc,
  [4248] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(530), 1,
      anon_sym_RBRACE,
    ACTIONS(532), 1,
      anon_sym_LBRACK,
    ACTIONS(534), 1,
      sym_identifier,
    STATE(163), 1,
      aux_sym_block_lit_repeat2,
  [4264] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(536), 1,
      sym_identifier,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(258), 1,
      sym_message_or_enum_type,
  [4280] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(536), 1,
      sym_identifier,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(309), 1,
      sym_message_or_enum_type,
  [4296] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(29), 1,
      anon_sym_DOT,
    ACTIONS(536), 1,
      sym_identifier,
    STATE(237), 1,
      aux_sym_message_or_enum_type_repeat1,
    STATE(251), 1,
      sym_message_or_enum_type,
  [4312] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(532), 1,
      anon_sym_LBRACK,
    ACTIONS(534), 1,
      sym_identifier,
    ACTIONS(538), 1,
      anon_sym_RBRACE,
    STATE(167), 1,
      aux_sym_block_lit_repeat2,
  [4328] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(238), 1,
      sym_field_option,
    STATE(295), 1,
      sym__option_name,
  [4344] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(494), 1,
      anon_sym_DOT,
    ACTIONS(540), 3,
      anon_sym_RPAREN,
      anon_sym_GT,
      sym_identifier,
  [4356] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(193), 1,
      sym_enum_value_option,
    STATE(259), 1,
      sym__option_name,
  [4372] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(444), 1,
      anon_sym_RBRACE,
    ACTIONS(542), 1,
      anon_sym_LBRACK,
    ACTIONS(545), 1,
      sym_identifier,
    STATE(167), 1,
      aux_sym_block_lit_repeat2,
  [4388] = 5,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(214), 1,
      sym_enum_value_option,
    STATE(259), 1,
      sym__option_name,
  [4404] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(494), 1,
      anon_sym_DOT,
    ACTIONS(548), 3,
      anon_sym_RPAREN,
      anon_sym_GT,
      sym_identifier,
  [4416] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(550), 1,
      anon_sym_COMMA,
    ACTIONS(553), 1,
      anon_sym_RBRACK,
    STATE(170), 1,
      aux_sym_block_lit_repeat1,
  [4429] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(555), 1,
      anon_sym_COMMA,
    ACTIONS(557), 1,
      anon_sym_RBRACK,
    STATE(170), 1,
      aux_sym_block_lit_repeat1,
  [4442] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(559), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [4451] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(264), 1,
      anon_sym_RBRACK,
    ACTIONS(555), 1,
      anon_sym_COMMA,
    STATE(170), 1,
      aux_sym_block_lit_repeat1,
  [4464] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(264), 1,
      anon_sym_RBRACK,
    ACTIONS(555), 1,
      anon_sym_COMMA,
    STATE(192), 1,
      aux_sym_block_lit_repeat1,
  [4477] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(313), 1,
      sym__option_name,
  [4490] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(561), 1,
      anon_sym_COMMA,
    ACTIONS(563), 1,
      anon_sym_RBRACK,
    STATE(196), 1,
      aux_sym_enum_field_repeat1,
  [4503] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(565), 1,
      anon_sym_SEMI,
    ACTIONS(567), 1,
      anon_sym_COMMA,
    STATE(208), 1,
      aux_sym_field_names_repeat1,
  [4516] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(448), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [4525] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(312), 1,
      sym__option_name,
  [4538] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(569), 1,
      anon_sym_SEMI,
    ACTIONS(571), 1,
      anon_sym_COMMA,
    STATE(195), 1,
      aux_sym_ranges_repeat1,
  [4551] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(311), 1,
      sym__option_name,
  [4564] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(476), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [4573] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(432), 1,
      anon_sym_LPAREN,
    ACTIONS(434), 1,
      sym_identifier,
    STATE(285), 1,
      sym__option_name,
  [4586] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(329), 1,
      anon_sym_DOT,
    ACTIONS(573), 1,
      anon_sym_EQ,
    STATE(186), 1,
      aux_sym__option_name_repeat1,
  [4599] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(575), 1,
      anon_sym_COMMA,
    ACTIONS(578), 1,
      anon_sym_RBRACK,
    STATE(185), 1,
      aux_sym_field_options_repeat1,
  [4612] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(329), 1,
      anon_sym_DOT,
    ACTIONS(580), 1,
      anon_sym_EQ,
    STATE(63), 1,
      aux_sym__option_name_repeat1,
  [4625] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(258), 1,
      anon_sym_RBRACK,
    ACTIONS(555), 1,
      anon_sym_COMMA,
    STATE(205), 1,
      aux_sym_block_lit_repeat1,
  [4638] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(266), 1,
      anon_sym_RBRACK,
    ACTIONS(555), 1,
      anon_sym_COMMA,
    STATE(173), 1,
      aux_sym_block_lit_repeat1,
  [4651] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(438), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [4660] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(329), 1,
      anon_sym_DOT,
    ACTIONS(582), 1,
      anon_sym_EQ,
    STATE(63), 1,
      aux_sym__option_name_repeat1,
  [4673] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(452), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [4682] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(258), 1,
      anon_sym_RBRACK,
    ACTIONS(555), 1,
      anon_sym_COMMA,
    STATE(170), 1,
      aux_sym_block_lit_repeat1,
  [4695] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(561), 1,
      anon_sym_COMMA,
    ACTIONS(584), 1,
      anon_sym_RBRACK,
    STATE(202), 1,
      aux_sym_enum_field_repeat1,
  [4708] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(586), 1,
      anon_sym_COMMA,
    ACTIONS(588), 1,
      anon_sym_RBRACK,
    STATE(185), 1,
      aux_sym_field_options_repeat1,
  [4721] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(590), 1,
      anon_sym_SEMI,
    ACTIONS(592), 1,
      anon_sym_COMMA,
    STATE(195), 1,
      aux_sym_ranges_repeat1,
  [4734] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(561), 1,
      anon_sym_COMMA,
    ACTIONS(584), 1,
      anon_sym_RBRACK,
    STATE(201), 1,
      aux_sym_enum_field_repeat1,
  [4747] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(571), 1,
      anon_sym_COMMA,
    ACTIONS(595), 1,
      anon_sym_SEMI,
    STATE(180), 1,
      aux_sym_ranges_repeat1,
  [4760] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(329), 1,
      anon_sym_DOT,
    ACTIONS(597), 1,
      anon_sym_EQ,
    STATE(190), 1,
      aux_sym__option_name_repeat1,
  [4773] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(567), 1,
      anon_sym_COMMA,
    ACTIONS(599), 1,
      anon_sym_SEMI,
    STATE(177), 1,
      aux_sym_field_names_repeat1,
  [4786] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(603), 1,
      anon_sym_to,
    ACTIONS(601), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
  [4797] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(605), 1,
      anon_sym_COMMA,
    ACTIONS(608), 1,
      anon_sym_RBRACK,
    STATE(201), 1,
      aux_sym_enum_field_repeat1,
  [4810] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(561), 1,
      anon_sym_COMMA,
    ACTIONS(610), 1,
      anon_sym_RBRACK,
    STATE(201), 1,
      aux_sym_enum_field_repeat1,
  [4823] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(555), 1,
      anon_sym_COMMA,
    ACTIONS(612), 1,
      anon_sym_RBRACK,
    STATE(171), 1,
      aux_sym_block_lit_repeat1,
  [4836] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(586), 1,
      anon_sym_COMMA,
    ACTIONS(614), 1,
      anon_sym_RBRACK,
    STATE(194), 1,
      aux_sym_field_options_repeat1,
  [4849] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(555), 1,
      anon_sym_COMMA,
    ACTIONS(612), 1,
      anon_sym_RBRACK,
    STATE(170), 1,
      aux_sym_block_lit_repeat1,
  [4862] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(464), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [4871] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(480), 3,
      anon_sym_RBRACE,
      anon_sym_LBRACK,
      sym_identifier,
  [4880] = 4,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(616), 1,
      anon_sym_SEMI,
    ACTIONS(618), 1,
      anon_sym_COMMA,
    STATE(208), 1,
      aux_sym_field_names_repeat1,
  [4893] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(621), 1,
      sym_identifier,
    STATE(215), 1,
      aux_sym_message_or_enum_type_repeat1,
  [4903] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(623), 1,
      anon_sym_SEMI,
    ACTIONS(625), 1,
      anon_sym_LBRACK,
  [4913] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(627), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
  [4921] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(629), 1,
      anon_sym_LBRACE,
    STATE(16), 1,
      sym_message_body,
  [4931] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(631), 2,
      anon_sym_COMMA,
      anon_sym_RBRACK,
  [4939] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(608), 2,
      anon_sym_COMMA,
      anon_sym_RBRACK,
  [4947] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(633), 1,
      sym_identifier,
    STATE(232), 1,
      aux_sym_message_or_enum_type_repeat1,
  [4957] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(616), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
  [4965] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(635), 2,
      anon_sym_DQUOTEproto3_DQUOTE,
      anon_sym_DQUOTEproto2_DQUOTE,
  [4973] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(637), 1,
      sym_identifier,
    STATE(270), 1,
      sym_full_ident,
  [4983] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(639), 1,
      sym_identifier,
    STATE(278), 1,
      sym_service_name,
  [4993] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(637), 1,
      sym_identifier,
    STATE(226), 1,
      sym_full_ident,
  [5003] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(641), 1,
      anon_sym_SEMI,
    ACTIONS(643), 1,
      anon_sym_LBRACK,
  [5013] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(645), 1,
      anon_sym_LBRACE,
    STATE(75), 1,
      sym_enum_body,
  [5023] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(647), 1,
      anon_sym_LBRACE,
    STATE(18), 1,
      sym_enum_body,
  [5033] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(649), 1,
      sym_identifier,
    STATE(228), 1,
      sym_message_name,
  [5043] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(651), 1,
      anon_sym_LBRACE,
    STATE(88), 1,
      sym_message_body,
  [5053] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(651), 1,
      anon_sym_LBRACE,
    STATE(87), 1,
      sym_message_body,
  [5063] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(653), 1,
      anon_sym_SEMI,
    ACTIONS(655), 1,
      anon_sym_LBRACK,
  [5073] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(629), 1,
      anon_sym_LBRACE,
    STATE(11), 1,
      sym_message_body,
  [5083] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(657), 1,
      sym_identifier,
    STATE(223), 1,
      sym_enum_name,
  [5093] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(637), 1,
      sym_identifier,
    STATE(314), 1,
      sym_full_ident,
  [5103] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(553), 2,
      anon_sym_COMMA,
      anon_sym_RBRACK,
  [5111] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(659), 1,
      sym_identifier,
    STATE(232), 1,
      aux_sym_message_or_enum_type_repeat1,
  [5121] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(416), 1,
      anon_sym_SEMI,
    ACTIONS(662), 1,
      anon_sym_LBRACE,
  [5131] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(637), 1,
      sym_identifier,
    STATE(212), 1,
      sym_full_ident,
  [5141] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(664), 1,
      sym_identifier,
    STATE(310), 1,
      sym_rpc_name,
  [5151] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(666), 2,
      anon_sym_COMMA,
      anon_sym_RBRACK,
  [5159] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(621), 1,
      sym_identifier,
    STATE(232), 1,
      aux_sym_message_or_enum_type_repeat1,
  [5169] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(578), 2,
      anon_sym_COMMA,
      anon_sym_RBRACK,
  [5177] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(649), 1,
      sym_identifier,
    STATE(225), 1,
      sym_message_name,
  [5187] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(657), 1,
      sym_identifier,
    STATE(222), 1,
      sym_enum_name,
  [5197] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(637), 1,
      sym_identifier,
    STATE(288), 1,
      sym_full_ident,
  [5207] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(668), 1,
      anon_sym_SEMI,
    ACTIONS(670), 1,
      anon_sym_LBRACK,
  [5217] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(590), 2,
      anon_sym_SEMI,
      anon_sym_COMMA,
  [5225] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(672), 2,
      anon_sym_GT,
      sym_identifier,
  [5233] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(400), 1,
      anon_sym_SEMI,
    ACTIONS(674), 1,
      anon_sym_LBRACE,
  [5243] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(676), 1,
      anon_sym_SEMI,
    ACTIONS(678), 1,
      anon_sym_LBRACK,
  [5253] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(680), 1,
      anon_sym_SEMI,
    ACTIONS(682), 1,
      anon_sym_LBRACE,
  [5263] = 3,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(684), 1,
      anon_sym_SEMI,
    ACTIONS(686), 1,
      anon_sym_LBRACK,
  [5273] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(688), 1,
      anon_sym_LPAREN,
  [5280] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(690), 1,
      anon_sym_RPAREN,
  [5287] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(692), 1,
      anon_sym_RPAREN,
  [5294] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(694), 1,
      sym_identifier,
  [5301] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(696), 1,
      anon_sym_RBRACK,
  [5308] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(698), 1,
      anon_sym_SEMI,
  [5315] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(700), 1,
      anon_sym_EQ,
  [5322] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(702), 1,
      anon_sym_SEMI,
  [5329] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(704), 1,
      sym_identifier,
  [5336] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(706), 1,
      anon_sym_RPAREN,
  [5343] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(708), 1,
      anon_sym_EQ,
  [5350] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(710), 1,
      anon_sym_LT,
  [5357] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(712), 1,
      anon_sym_RPAREN,
  [5364] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(714), 1,
      anon_sym_RBRACK,
  [5371] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(716), 1,
      sym_identifier,
  [5378] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(718), 1,
      anon_sym_SEMI,
  [5385] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(720), 1,
      anon_sym_EQ,
  [5392] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(722), 1,
      sym_identifier,
  [5399] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(724), 1,
      anon_sym_SEMI,
  [5406] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(726), 1,
      anon_sym_COMMA,
  [5413] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(728), 1,
      anon_sym_RBRACK,
  [5420] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(730), 1,
      anon_sym_RPAREN,
  [5427] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(732), 1,
      sym_identifier,
  [5434] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(734), 1,
      anon_sym_COMMA,
  [5441] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(736), 1,
      anon_sym_EQ,
  [5448] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(494), 1,
      anon_sym_DOT,
  [5455] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(738), 1,
      anon_sym_SEMI,
  [5462] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(740), 1,
      anon_sym_SEMI,
  [5469] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(742), 1,
      anon_sym_SEMI,
  [5476] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(744), 1,
      anon_sym_LBRACE,
  [5483] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(746), 1,
      anon_sym_LBRACE,
  [5490] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(748), 1,
      anon_sym_LPAREN,
  [5497] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(750), 1,
      anon_sym_SEMI,
  [5504] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(752), 1,
      anon_sym_LBRACE,
  [5511] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(754), 1,
      anon_sym_LPAREN,
  [5518] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(756), 1,
      anon_sym_LBRACE,
  [5525] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(758), 1,
      anon_sym_EQ,
  [5532] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(760), 1,
      anon_sym_SEMI,
  [5539] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(762), 1,
      anon_sym_EQ,
  [5546] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(764), 1,
      anon_sym_SEMI,
  [5553] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(766), 1,
      anon_sym_SEMI,
  [5560] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(768), 1,
      anon_sym_RBRACK,
  [5567] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(770), 1,
      anon_sym_SEMI,
  [5574] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(772), 1,
      sym_identifier,
  [5581] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(774), 1,
      anon_sym_returns,
  [5588] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(776), 1,
      anon_sym_RBRACK,
  [5595] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(778), 1,
      anon_sym_EQ,
  [5602] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(780), 1,
      sym_identifier,
  [5609] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(782), 1,
      ts_builtin_sym_end,
  [5616] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(784), 1,
      anon_sym_SEMI,
  [5623] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(786), 1,
      anon_sym_LBRACE,
  [5630] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(788), 1,
      anon_sym_SEMI,
  [5637] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(790), 1,
      anon_sym_SEMI,
  [5644] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(792), 1,
      anon_sym_SEMI,
  [5651] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(794), 1,
      anon_sym_SEMI,
  [5658] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(796), 1,
      sym_identifier,
  [5665] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(798), 1,
      anon_sym_SEMI,
  [5672] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(800), 1,
      anon_sym_EQ,
  [5679] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(802), 1,
      anon_sym_EQ,
  [5686] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(804), 1,
      anon_sym_returns,
  [5693] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(806), 1,
      anon_sym_RPAREN,
  [5700] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(808), 1,
      anon_sym_LPAREN,
  [5707] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(810), 1,
      anon_sym_EQ,
  [5714] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(812), 1,
      anon_sym_EQ,
  [5721] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(814), 1,
      anon_sym_EQ,
  [5728] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(816), 1,
      anon_sym_RBRACK,
  [5735] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(818), 1,
      anon_sym_GT,
  [5742] = 2,
    ACTIONS(3), 1,
      sym_comment,
    ACTIONS(820), 1,
      anon_sym_EQ,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(2)] = 0,
  [SMALL_STATE(3)] = 86,
  [SMALL_STATE(4)] = 172,
  [SMALL_STATE(5)] = 258,
  [SMALL_STATE(6)] = 344,
  [SMALL_STATE(7)] = 430,
  [SMALL_STATE(8)] = 468,
  [SMALL_STATE(9)] = 506,
  [SMALL_STATE(10)] = 544,
  [SMALL_STATE(11)] = 582,
  [SMALL_STATE(12)] = 620,
  [SMALL_STATE(13)] = 658,
  [SMALL_STATE(14)] = 696,
  [SMALL_STATE(15)] = 734,
  [SMALL_STATE(16)] = 772,
  [SMALL_STATE(17)] = 810,
  [SMALL_STATE(18)] = 848,
  [SMALL_STATE(19)] = 886,
  [SMALL_STATE(20)] = 924,
  [SMALL_STATE(21)] = 962,
  [SMALL_STATE(22)] = 1000,
  [SMALL_STATE(23)] = 1038,
  [SMALL_STATE(24)] = 1076,
  [SMALL_STATE(25)] = 1114,
  [SMALL_STATE(26)] = 1152,
  [SMALL_STATE(27)] = 1190,
  [SMALL_STATE(28)] = 1228,
  [SMALL_STATE(29)] = 1279,
  [SMALL_STATE(30)] = 1330,
  [SMALL_STATE(31)] = 1381,
  [SMALL_STATE(32)] = 1410,
  [SMALL_STATE(33)] = 1463,
  [SMALL_STATE(34)] = 1494,
  [SMALL_STATE(35)] = 1547,
  [SMALL_STATE(36)] = 1576,
  [SMALL_STATE(37)] = 1615,
  [SMALL_STATE(38)] = 1665,
  [SMALL_STATE(39)] = 1715,
  [SMALL_STATE(40)] = 1751,
  [SMALL_STATE(41)] = 1801,
  [SMALL_STATE(42)] = 1851,
  [SMALL_STATE(43)] = 1879,
  [SMALL_STATE(44)] = 1915,
  [SMALL_STATE(45)] = 1951,
  [SMALL_STATE(46)] = 1979,
  [SMALL_STATE(47)] = 2007,
  [SMALL_STATE(48)] = 2057,
  [SMALL_STATE(49)] = 2107,
  [SMALL_STATE(50)] = 2154,
  [SMALL_STATE(51)] = 2201,
  [SMALL_STATE(52)] = 2248,
  [SMALL_STATE(53)] = 2295,
  [SMALL_STATE(54)] = 2342,
  [SMALL_STATE(55)] = 2389,
  [SMALL_STATE(56)] = 2436,
  [SMALL_STATE(57)] = 2478,
  [SMALL_STATE(58)] = 2520,
  [SMALL_STATE(59)] = 2562,
  [SMALL_STATE(60)] = 2604,
  [SMALL_STATE(61)] = 2625,
  [SMALL_STATE(62)] = 2662,
  [SMALL_STATE(63)] = 2699,
  [SMALL_STATE(64)] = 2720,
  [SMALL_STATE(65)] = 2737,
  [SMALL_STATE(66)] = 2754,
  [SMALL_STATE(67)] = 2780,
  [SMALL_STATE(68)] = 2796,
  [SMALL_STATE(69)] = 2816,
  [SMALL_STATE(70)] = 2842,
  [SMALL_STATE(71)] = 2868,
  [SMALL_STATE(72)] = 2888,
  [SMALL_STATE(73)] = 2914,
  [SMALL_STATE(74)] = 2940,
  [SMALL_STATE(75)] = 2955,
  [SMALL_STATE(76)] = 2970,
  [SMALL_STATE(77)] = 2991,
  [SMALL_STATE(78)] = 3006,
  [SMALL_STATE(79)] = 3021,
  [SMALL_STATE(80)] = 3036,
  [SMALL_STATE(81)] = 3051,
  [SMALL_STATE(82)] = 3066,
  [SMALL_STATE(83)] = 3081,
  [SMALL_STATE(84)] = 3096,
  [SMALL_STATE(85)] = 3111,
  [SMALL_STATE(86)] = 3132,
  [SMALL_STATE(87)] = 3147,
  [SMALL_STATE(88)] = 3162,
  [SMALL_STATE(89)] = 3177,
  [SMALL_STATE(90)] = 3199,
  [SMALL_STATE(91)] = 3213,
  [SMALL_STATE(92)] = 3235,
  [SMALL_STATE(93)] = 3249,
  [SMALL_STATE(94)] = 3271,
  [SMALL_STATE(95)] = 3283,
  [SMALL_STATE(96)] = 3301,
  [SMALL_STATE(97)] = 3313,
  [SMALL_STATE(98)] = 3331,
  [SMALL_STATE(99)] = 3349,
  [SMALL_STATE(100)] = 3369,
  [SMALL_STATE(101)] = 3381,
  [SMALL_STATE(102)] = 3393,
  [SMALL_STATE(103)] = 3411,
  [SMALL_STATE(104)] = 3429,
  [SMALL_STATE(105)] = 3449,
  [SMALL_STATE(106)] = 3467,
  [SMALL_STATE(107)] = 3485,
  [SMALL_STATE(108)] = 3505,
  [SMALL_STATE(109)] = 3517,
  [SMALL_STATE(110)] = 3530,
  [SMALL_STATE(111)] = 3543,
  [SMALL_STATE(112)] = 3556,
  [SMALL_STATE(113)] = 3575,
  [SMALL_STATE(114)] = 3588,
  [SMALL_STATE(115)] = 3607,
  [SMALL_STATE(116)] = 3618,
  [SMALL_STATE(117)] = 3635,
  [SMALL_STATE(118)] = 3648,
  [SMALL_STATE(119)] = 3667,
  [SMALL_STATE(120)] = 3680,
  [SMALL_STATE(121)] = 3699,
  [SMALL_STATE(122)] = 3712,
  [SMALL_STATE(123)] = 3725,
  [SMALL_STATE(124)] = 3742,
  [SMALL_STATE(125)] = 3759,
  [SMALL_STATE(126)] = 3772,
  [SMALL_STATE(127)] = 3791,
  [SMALL_STATE(128)] = 3808,
  [SMALL_STATE(129)] = 3821,
  [SMALL_STATE(130)] = 3834,
  [SMALL_STATE(131)] = 3853,
  [SMALL_STATE(132)] = 3870,
  [SMALL_STATE(133)] = 3887,
  [SMALL_STATE(134)] = 3904,
  [SMALL_STATE(135)] = 3923,
  [SMALL_STATE(136)] = 3934,
  [SMALL_STATE(137)] = 3947,
  [SMALL_STATE(138)] = 3960,
  [SMALL_STATE(139)] = 3973,
  [SMALL_STATE(140)] = 3990,
  [SMALL_STATE(141)] = 4007,
  [SMALL_STATE(142)] = 4026,
  [SMALL_STATE(143)] = 4039,
  [SMALL_STATE(144)] = 4056,
  [SMALL_STATE(145)] = 4068,
  [SMALL_STATE(146)] = 4082,
  [SMALL_STATE(147)] = 4098,
  [SMALL_STATE(148)] = 4112,
  [SMALL_STATE(149)] = 4126,
  [SMALL_STATE(150)] = 4136,
  [SMALL_STATE(151)] = 4150,
  [SMALL_STATE(152)] = 4160,
  [SMALL_STATE(153)] = 4176,
  [SMALL_STATE(154)] = 4190,
  [SMALL_STATE(155)] = 4200,
  [SMALL_STATE(156)] = 4214,
  [SMALL_STATE(157)] = 4228,
  [SMALL_STATE(158)] = 4238,
  [SMALL_STATE(159)] = 4248,
  [SMALL_STATE(160)] = 4264,
  [SMALL_STATE(161)] = 4280,
  [SMALL_STATE(162)] = 4296,
  [SMALL_STATE(163)] = 4312,
  [SMALL_STATE(164)] = 4328,
  [SMALL_STATE(165)] = 4344,
  [SMALL_STATE(166)] = 4356,
  [SMALL_STATE(167)] = 4372,
  [SMALL_STATE(168)] = 4388,
  [SMALL_STATE(169)] = 4404,
  [SMALL_STATE(170)] = 4416,
  [SMALL_STATE(171)] = 4429,
  [SMALL_STATE(172)] = 4442,
  [SMALL_STATE(173)] = 4451,
  [SMALL_STATE(174)] = 4464,
  [SMALL_STATE(175)] = 4477,
  [SMALL_STATE(176)] = 4490,
  [SMALL_STATE(177)] = 4503,
  [SMALL_STATE(178)] = 4516,
  [SMALL_STATE(179)] = 4525,
  [SMALL_STATE(180)] = 4538,
  [SMALL_STATE(181)] = 4551,
  [SMALL_STATE(182)] = 4564,
  [SMALL_STATE(183)] = 4573,
  [SMALL_STATE(184)] = 4586,
  [SMALL_STATE(185)] = 4599,
  [SMALL_STATE(186)] = 4612,
  [SMALL_STATE(187)] = 4625,
  [SMALL_STATE(188)] = 4638,
  [SMALL_STATE(189)] = 4651,
  [SMALL_STATE(190)] = 4660,
  [SMALL_STATE(191)] = 4673,
  [SMALL_STATE(192)] = 4682,
  [SMALL_STATE(193)] = 4695,
  [SMALL_STATE(194)] = 4708,
  [SMALL_STATE(195)] = 4721,
  [SMALL_STATE(196)] = 4734,
  [SMALL_STATE(197)] = 4747,
  [SMALL_STATE(198)] = 4760,
  [SMALL_STATE(199)] = 4773,
  [SMALL_STATE(200)] = 4786,
  [SMALL_STATE(201)] = 4797,
  [SMALL_STATE(202)] = 4810,
  [SMALL_STATE(203)] = 4823,
  [SMALL_STATE(204)] = 4836,
  [SMALL_STATE(205)] = 4849,
  [SMALL_STATE(206)] = 4862,
  [SMALL_STATE(207)] = 4871,
  [SMALL_STATE(208)] = 4880,
  [SMALL_STATE(209)] = 4893,
  [SMALL_STATE(210)] = 4903,
  [SMALL_STATE(211)] = 4913,
  [SMALL_STATE(212)] = 4921,
  [SMALL_STATE(213)] = 4931,
  [SMALL_STATE(214)] = 4939,
  [SMALL_STATE(215)] = 4947,
  [SMALL_STATE(216)] = 4957,
  [SMALL_STATE(217)] = 4965,
  [SMALL_STATE(218)] = 4973,
  [SMALL_STATE(219)] = 4983,
  [SMALL_STATE(220)] = 4993,
  [SMALL_STATE(221)] = 5003,
  [SMALL_STATE(222)] = 5013,
  [SMALL_STATE(223)] = 5023,
  [SMALL_STATE(224)] = 5033,
  [SMALL_STATE(225)] = 5043,
  [SMALL_STATE(226)] = 5053,
  [SMALL_STATE(227)] = 5063,
  [SMALL_STATE(228)] = 5073,
  [SMALL_STATE(229)] = 5083,
  [SMALL_STATE(230)] = 5093,
  [SMALL_STATE(231)] = 5103,
  [SMALL_STATE(232)] = 5111,
  [SMALL_STATE(233)] = 5121,
  [SMALL_STATE(234)] = 5131,
  [SMALL_STATE(235)] = 5141,
  [SMALL_STATE(236)] = 5151,
  [SMALL_STATE(237)] = 5159,
  [SMALL_STATE(238)] = 5169,
  [SMALL_STATE(239)] = 5177,
  [SMALL_STATE(240)] = 5187,
  [SMALL_STATE(241)] = 5197,
  [SMALL_STATE(242)] = 5207,
  [SMALL_STATE(243)] = 5217,
  [SMALL_STATE(244)] = 5225,
  [SMALL_STATE(245)] = 5233,
  [SMALL_STATE(246)] = 5243,
  [SMALL_STATE(247)] = 5253,
  [SMALL_STATE(248)] = 5263,
  [SMALL_STATE(249)] = 5273,
  [SMALL_STATE(250)] = 5280,
  [SMALL_STATE(251)] = 5287,
  [SMALL_STATE(252)] = 5294,
  [SMALL_STATE(253)] = 5301,
  [SMALL_STATE(254)] = 5308,
  [SMALL_STATE(255)] = 5315,
  [SMALL_STATE(256)] = 5322,
  [SMALL_STATE(257)] = 5329,
  [SMALL_STATE(258)] = 5336,
  [SMALL_STATE(259)] = 5343,
  [SMALL_STATE(260)] = 5350,
  [SMALL_STATE(261)] = 5357,
  [SMALL_STATE(262)] = 5364,
  [SMALL_STATE(263)] = 5371,
  [SMALL_STATE(264)] = 5378,
  [SMALL_STATE(265)] = 5385,
  [SMALL_STATE(266)] = 5392,
  [SMALL_STATE(267)] = 5399,
  [SMALL_STATE(268)] = 5406,
  [SMALL_STATE(269)] = 5413,
  [SMALL_STATE(270)] = 5420,
  [SMALL_STATE(271)] = 5427,
  [SMALL_STATE(272)] = 5434,
  [SMALL_STATE(273)] = 5441,
  [SMALL_STATE(274)] = 5448,
  [SMALL_STATE(275)] = 5455,
  [SMALL_STATE(276)] = 5462,
  [SMALL_STATE(277)] = 5469,
  [SMALL_STATE(278)] = 5476,
  [SMALL_STATE(279)] = 5483,
  [SMALL_STATE(280)] = 5490,
  [SMALL_STATE(281)] = 5497,
  [SMALL_STATE(282)] = 5504,
  [SMALL_STATE(283)] = 5511,
  [SMALL_STATE(284)] = 5518,
  [SMALL_STATE(285)] = 5525,
  [SMALL_STATE(286)] = 5532,
  [SMALL_STATE(287)] = 5539,
  [SMALL_STATE(288)] = 5546,
  [SMALL_STATE(289)] = 5553,
  [SMALL_STATE(290)] = 5560,
  [SMALL_STATE(291)] = 5567,
  [SMALL_STATE(292)] = 5574,
  [SMALL_STATE(293)] = 5581,
  [SMALL_STATE(294)] = 5588,
  [SMALL_STATE(295)] = 5595,
  [SMALL_STATE(296)] = 5602,
  [SMALL_STATE(297)] = 5609,
  [SMALL_STATE(298)] = 5616,
  [SMALL_STATE(299)] = 5623,
  [SMALL_STATE(300)] = 5630,
  [SMALL_STATE(301)] = 5637,
  [SMALL_STATE(302)] = 5644,
  [SMALL_STATE(303)] = 5651,
  [SMALL_STATE(304)] = 5658,
  [SMALL_STATE(305)] = 5665,
  [SMALL_STATE(306)] = 5672,
  [SMALL_STATE(307)] = 5679,
  [SMALL_STATE(308)] = 5686,
  [SMALL_STATE(309)] = 5693,
  [SMALL_STATE(310)] = 5700,
  [SMALL_STATE(311)] = 5707,
  [SMALL_STATE(312)] = 5714,
  [SMALL_STATE(313)] = 5721,
  [SMALL_STATE(314)] = 5728,
  [SMALL_STATE(315)] = 5735,
  [SMALL_STATE(316)] = 5742,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, SHIFT_EXTRA(),
  [5] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 0),
  [7] = {.entry = {.count = 1, .reusable = true}}, SHIFT(65),
  [9] = {.entry = {.count = 1, .reusable = true}}, SHIFT(307),
  [11] = {.entry = {.count = 1, .reusable = true}}, SHIFT(99),
  [13] = {.entry = {.count = 1, .reusable = true}}, SHIFT(241),
  [15] = {.entry = {.count = 1, .reusable = true}}, SHIFT(183),
  [17] = {.entry = {.count = 1, .reusable = true}}, SHIFT(240),
  [19] = {.entry = {.count = 1, .reusable = true}}, SHIFT(239),
  [21] = {.entry = {.count = 1, .reusable = true}}, SHIFT(220),
  [23] = {.entry = {.count = 1, .reusable = true}}, SHIFT(219),
  [25] = {.entry = {.count = 1, .reusable = true}}, SHIFT(21),
  [27] = {.entry = {.count = 1, .reusable = false}}, SHIFT(179),
  [29] = {.entry = {.count = 1, .reusable = true}}, SHIFT(209),
  [31] = {.entry = {.count = 1, .reusable = false}}, SHIFT(229),
  [33] = {.entry = {.count = 1, .reusable = true}}, SHIFT(26),
  [35] = {.entry = {.count = 1, .reusable = false}}, SHIFT(224),
  [37] = {.entry = {.count = 1, .reusable = false}}, SHIFT(234),
  [39] = {.entry = {.count = 1, .reusable = false}}, SHIFT(36),
  [41] = {.entry = {.count = 1, .reusable = false}}, SHIFT(44),
  [43] = {.entry = {.count = 1, .reusable = false}}, SHIFT(252),
  [45] = {.entry = {.count = 1, .reusable = false}}, SHIFT(260),
  [47] = {.entry = {.count = 1, .reusable = false}}, SHIFT(244),
  [49] = {.entry = {.count = 1, .reusable = false}}, SHIFT(61),
  [51] = {.entry = {.count = 1, .reusable = false}}, SHIFT(107),
  [53] = {.entry = {.count = 1, .reusable = false}}, SHIFT(144),
  [55] = {.entry = {.count = 1, .reusable = true}}, SHIFT(82),
  [57] = {.entry = {.count = 1, .reusable = true}}, SHIFT(74),
  [59] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(21),
  [62] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(179),
  [65] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(209),
  [68] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(229),
  [71] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_message_body_repeat1, 2),
  [73] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(224),
  [76] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(234),
  [79] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(36),
  [82] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(44),
  [85] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(252),
  [88] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(260),
  [91] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(244),
  [94] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(61),
  [97] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(107),
  [100] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_message_body_repeat1, 2), SHIFT_REPEAT(144),
  [103] = {.entry = {.count = 1, .reusable = true}}, SHIFT(23),
  [105] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field, 7),
  [107] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_field, 7),
  [109] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_map_field, 13),
  [111] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_map_field, 13),
  [113] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_map_field, 10),
  [115] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_map_field, 10),
  [117] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_extensions, 3),
  [119] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_extensions, 3),
  [121] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message, 3),
  [123] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_message, 3),
  [125] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field, 10),
  [127] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_field, 10),
  [129] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_oneof, 5),
  [131] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_oneof, 5),
  [133] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field, 9),
  [135] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_field, 9),
  [137] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field, 8),
  [139] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_field, 8),
  [141] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_extend, 3),
  [143] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_extend, 3),
  [145] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_option, 5),
  [147] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_option, 5),
  [149] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum, 3),
  [151] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_enum, 3),
  [153] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_oneof, 4),
  [155] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_oneof, 4),
  [157] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field, 6),
  [159] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_field, 6),
  [161] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_empty_statement, 1),
  [163] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_empty_statement, 1),
  [165] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_reserved, 3),
  [167] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_reserved, 3),
  [169] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message_body, 3),
  [171] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_message_body, 3),
  [173] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum_body, 3),
  [175] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_enum_body, 3),
  [177] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field, 5),
  [179] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_field, 5),
  [181] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message_body, 2),
  [183] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_message_body, 2),
  [185] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum_body, 2),
  [187] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_enum_body, 2),
  [189] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_oneof_repeat1, 2), SHIFT_REPEAT(42),
  [192] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_oneof_repeat1, 2), SHIFT_REPEAT(175),
  [195] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_oneof_repeat1, 2), SHIFT_REPEAT(209),
  [198] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_oneof_repeat1, 2),
  [200] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_oneof_repeat1, 2), SHIFT_REPEAT(244),
  [203] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_oneof_repeat1, 2), SHIFT_REPEAT(144),
  [206] = {.entry = {.count = 1, .reusable = true}}, SHIFT(42),
  [208] = {.entry = {.count = 1, .reusable = false}}, SHIFT(175),
  [210] = {.entry = {.count = 1, .reusable = true}}, SHIFT(13),
  [212] = {.entry = {.count = 1, .reusable = true}}, SHIFT(19),
  [214] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_number, 1),
  [216] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_field_number, 1),
  [218] = {.entry = {.count = 1, .reusable = true}}, SHIFT(159),
  [220] = {.entry = {.count = 1, .reusable = true}}, SHIFT(132),
  [222] = {.entry = {.count = 1, .reusable = true}}, SHIFT(38),
  [224] = {.entry = {.count = 1, .reusable = true}}, SHIFT(48),
  [226] = {.entry = {.count = 1, .reusable = false}}, SHIFT(71),
  [228] = {.entry = {.count = 1, .reusable = false}}, SHIFT(100),
  [230] = {.entry = {.count = 1, .reusable = false}}, SHIFT(115),
  [232] = {.entry = {.count = 1, .reusable = true}}, SHIFT(115),
  [234] = {.entry = {.count = 1, .reusable = false}}, SHIFT(101),
  [236] = {.entry = {.count = 1, .reusable = true}}, SHIFT(147),
  [238] = {.entry = {.count = 1, .reusable = true}}, SHIFT(148),
  [240] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_oneof_field, 4),
  [242] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_oneof_field, 4),
  [244] = {.entry = {.count = 1, .reusable = true}}, SHIFT(112),
  [246] = {.entry = {.count = 1, .reusable = true}}, SHIFT(41),
  [248] = {.entry = {.count = 1, .reusable = true}}, SHIFT(47),
  [250] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_int_lit, 1),
  [252] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_int_lit, 1),
  [254] = {.entry = {.count = 1, .reusable = false}}, SHIFT(39),
  [256] = {.entry = {.count = 1, .reusable = true}}, SHIFT(140),
  [258] = {.entry = {.count = 1, .reusable = true}}, SHIFT(137),
  [260] = {.entry = {.count = 1, .reusable = false}}, SHIFT(135),
  [262] = {.entry = {.count = 1, .reusable = true}}, SHIFT(135),
  [264] = {.entry = {.count = 1, .reusable = true}}, SHIFT(113),
  [266] = {.entry = {.count = 1, .reusable = true}}, SHIFT(129),
  [268] = {.entry = {.count = 1, .reusable = true}}, SHIFT(136),
  [270] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_oneof_field, 7),
  [272] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_oneof_field, 7),
  [274] = {.entry = {.count = 1, .reusable = true}}, SHIFT(40),
  [276] = {.entry = {.count = 1, .reusable = true}}, SHIFT(37),
  [278] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 1),
  [280] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2),
  [282] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(65),
  [285] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(99),
  [288] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(241),
  [291] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(183),
  [294] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(240),
  [297] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(239),
  [300] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(220),
  [303] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(219),
  [306] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 2),
  [308] = {.entry = {.count = 1, .reusable = true}}, SHIFT(272),
  [310] = {.entry = {.count = 1, .reusable = true}}, SHIFT(199),
  [312] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__option_name_repeat1, 2),
  [314] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__option_name_repeat1, 2), SHIFT_REPEAT(271),
  [317] = {.entry = {.count = 1, .reusable = true}}, SHIFT(109),
  [319] = {.entry = {.count = 1, .reusable = false}}, SHIFT(181),
  [321] = {.entry = {.count = 1, .reusable = true}}, SHIFT(79),
  [323] = {.entry = {.count = 1, .reusable = false}}, SHIFT(62),
  [325] = {.entry = {.count = 1, .reusable = false}}, SHIFT(255),
  [327] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_full_ident, 2),
  [329] = {.entry = {.count = 1, .reusable = true}}, SHIFT(271),
  [331] = {.entry = {.count = 1, .reusable = true}}, SHIFT(24),
  [333] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_enum_body_repeat1, 2), SHIFT_REPEAT(109),
  [336] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_enum_body_repeat1, 2), SHIFT_REPEAT(181),
  [339] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_enum_body_repeat1, 2),
  [341] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_enum_body_repeat1, 2), SHIFT_REPEAT(62),
  [344] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_enum_body_repeat1, 2), SHIFT_REPEAT(255),
  [347] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_full_ident, 1),
  [349] = {.entry = {.count = 1, .reusable = true}}, SHIFT(86),
  [351] = {.entry = {.count = 1, .reusable = true}}, SHIFT(27),
  [353] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_string, 1),
  [355] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_import, 3, .production_id = 1),
  [357] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_package, 3),
  [359] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_service, 5),
  [361] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_service, 4),
  [363] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_import, 4, .production_id = 2),
  [365] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_syntax, 4),
  [367] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_string_repeat3, 2),
  [369] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_string_repeat3, 2), SHIFT_REPEAT(147),
  [372] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_string_repeat3, 2), SHIFT_REPEAT(148),
  [375] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_service_repeat1, 2), SHIFT_REPEAT(65),
  [378] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_service_repeat1, 2), SHIFT_REPEAT(183),
  [381] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_service_repeat1, 2),
  [383] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_service_repeat1, 2), SHIFT_REPEAT(235),
  [386] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_string_repeat3, 3),
  [388] = {.entry = {.count = 1, .reusable = true}}, SHIFT(80),
  [390] = {.entry = {.count = 1, .reusable = true}}, SHIFT(235),
  [392] = {.entry = {.count = 1, .reusable = true}}, SHIFT(81),
  [394] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_block_lit, 3),
  [396] = {.entry = {.count = 1, .reusable = true}}, SHIFT(151),
  [398] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_constant, 2),
  [400] = {.entry = {.count = 1, .reusable = true}}, SHIFT(157),
  [402] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_rpc_repeat1, 2), SHIFT_REPEAT(65),
  [405] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_rpc_repeat1, 2), SHIFT_REPEAT(183),
  [408] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_rpc_repeat1, 2),
  [410] = {.entry = {.count = 1, .reusable = true}}, SHIFT(146),
  [412] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_bool, 1),
  [414] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_constant, 1),
  [416] = {.entry = {.count = 1, .reusable = true}}, SHIFT(154),
  [418] = {.entry = {.count = 1, .reusable = true}}, SHIFT(216),
  [420] = {.entry = {.count = 1, .reusable = true}}, SHIFT(149),
  [422] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_block_lit, 2),
  [424] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum_field, 5),
  [426] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_enum_field, 5),
  [428] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum_field, 8),
  [430] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_enum_field, 8),
  [432] = {.entry = {.count = 1, .reusable = true}}, SHIFT(218),
  [434] = {.entry = {.count = 1, .reusable = true}}, SHIFT(198),
  [436] = {.entry = {.count = 1, .reusable = true}}, SHIFT(207),
  [438] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 5),
  [440] = {.entry = {.count = 1, .reusable = false}}, SHIFT(160),
  [442] = {.entry = {.count = 1, .reusable = true}}, SHIFT(182),
  [444] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 2),
  [446] = {.entry = {.count = 1, .reusable = true}}, SHIFT(172),
  [448] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 8),
  [450] = {.entry = {.count = 1, .reusable = true}}, SHIFT(178),
  [452] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 7),
  [454] = {.entry = {.count = 1, .reusable = false}}, SHIFT(162),
  [456] = {.entry = {.count = 1, .reusable = true}}, SHIFT(211),
  [458] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum_field, 9),
  [460] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_enum_field, 9),
  [462] = {.entry = {.count = 1, .reusable = true}}, SHIFT(189),
  [464] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 4),
  [466] = {.entry = {.count = 1, .reusable = true}}, SHIFT(35),
  [468] = {.entry = {.count = 1, .reusable = false}}, SHIFT(35),
  [470] = {.entry = {.count = 1, .reusable = true}}, SHIFT(96),
  [472] = {.entry = {.count = 1, .reusable = false}}, SHIFT(161),
  [474] = {.entry = {.count = 1, .reusable = true}}, SHIFT(206),
  [476] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 3),
  [478] = {.entry = {.count = 1, .reusable = true}}, SHIFT(191),
  [480] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 6),
  [482] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum_field, 4),
  [484] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_enum_field, 4),
  [486] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum_field, 7),
  [488] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_enum_field, 7),
  [490] = {.entry = {.count = 1, .reusable = true}}, SHIFT(145),
  [492] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message_or_enum_type, 1),
  [494] = {.entry = {.count = 1, .reusable = true}}, SHIFT(304),
  [496] = {.entry = {.count = 1, .reusable = false}}, SHIFT(92),
  [498] = {.entry = {.count = 1, .reusable = true}}, SHIFT(150),
  [500] = {.entry = {.count = 1, .reusable = false}}, SHIFT_EXTRA(),
  [502] = {.entry = {.count = 1, .reusable = true}}, SHIFT(153),
  [504] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_rpc, 14),
  [506] = {.entry = {.count = 1, .reusable = false}}, SHIFT(90),
  [508] = {.entry = {.count = 1, .reusable = true}}, SHIFT(155),
  [510] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_rpc, 13),
  [512] = {.entry = {.count = 1, .reusable = true}}, SHIFT(156),
  [514] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_rpc, 12),
  [516] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_string_repeat1, 2),
  [518] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_string_repeat1, 2), SHIFT_REPEAT(155),
  [521] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_string_repeat2, 2),
  [523] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_string_repeat2, 2), SHIFT_REPEAT(156),
  [526] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_rpc, 11),
  [528] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_rpc, 10),
  [530] = {.entry = {.count = 1, .reusable = true}}, SHIFT(108),
  [532] = {.entry = {.count = 1, .reusable = true}}, SHIFT(230),
  [534] = {.entry = {.count = 1, .reusable = true}}, SHIFT(34),
  [536] = {.entry = {.count = 1, .reusable = true}}, SHIFT(144),
  [538] = {.entry = {.count = 1, .reusable = true}}, SHIFT(94),
  [540] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message_or_enum_type, 3),
  [542] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 2), SHIFT_REPEAT(230),
  [545] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 2), SHIFT_REPEAT(34),
  [548] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message_or_enum_type, 2),
  [550] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat1, 2), SHIFT_REPEAT(54),
  [553] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat1, 2),
  [555] = {.entry = {.count = 1, .reusable = true}}, SHIFT(54),
  [557] = {.entry = {.count = 1, .reusable = true}}, SHIFT(121),
  [559] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_lit_repeat2, 9),
  [561] = {.entry = {.count = 1, .reusable = true}}, SHIFT(168),
  [563] = {.entry = {.count = 1, .reusable = true}}, SHIFT(305),
  [565] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_names, 2),
  [567] = {.entry = {.count = 1, .reusable = true}}, SHIFT(104),
  [569] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_ranges, 2),
  [571] = {.entry = {.count = 1, .reusable = true}}, SHIFT(123),
  [573] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__option_name, 3),
  [575] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_field_options_repeat1, 2), SHIFT_REPEAT(164),
  [578] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_field_options_repeat1, 2),
  [580] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__option_name, 4),
  [582] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__option_name, 2),
  [584] = {.entry = {.count = 1, .reusable = true}}, SHIFT(291),
  [586] = {.entry = {.count = 1, .reusable = true}}, SHIFT(164),
  [588] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_options, 2),
  [590] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_ranges_repeat1, 2),
  [592] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_ranges_repeat1, 2), SHIFT_REPEAT(123),
  [595] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_ranges, 1),
  [597] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__option_name, 1),
  [599] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_names, 1),
  [601] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_range, 1),
  [603] = {.entry = {.count = 1, .reusable = true}}, SHIFT(127),
  [605] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_enum_field_repeat1, 2), SHIFT_REPEAT(168),
  [608] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_enum_field_repeat1, 2),
  [610] = {.entry = {.count = 1, .reusable = true}}, SHIFT(275),
  [612] = {.entry = {.count = 1, .reusable = true}}, SHIFT(125),
  [614] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_options, 1),
  [616] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_field_names_repeat1, 2),
  [618] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_field_names_repeat1, 2), SHIFT_REPEAT(104),
  [621] = {.entry = {.count = 1, .reusable = true}}, SHIFT(169),
  [623] = {.entry = {.count = 1, .reusable = true}}, SHIFT(110),
  [625] = {.entry = {.count = 1, .reusable = true}}, SHIFT(166),
  [627] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_range, 3),
  [629] = {.entry = {.count = 1, .reusable = true}}, SHIFT(2),
  [631] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum_value_option, 3),
  [633] = {.entry = {.count = 1, .reusable = true}}, SHIFT(165),
  [635] = {.entry = {.count = 1, .reusable = true}}, SHIFT(277),
  [637] = {.entry = {.count = 1, .reusable = true}}, SHIFT(71),
  [639] = {.entry = {.count = 1, .reusable = true}}, SHIFT(279),
  [641] = {.entry = {.count = 1, .reusable = true}}, SHIFT(7),
  [643] = {.entry = {.count = 1, .reusable = true}}, SHIFT(120),
  [645] = {.entry = {.count = 1, .reusable = true}}, SHIFT(66),
  [647] = {.entry = {.count = 1, .reusable = true}}, SHIFT(73),
  [649] = {.entry = {.count = 1, .reusable = true}}, SHIFT(282),
  [651] = {.entry = {.count = 1, .reusable = true}}, SHIFT(4),
  [653] = {.entry = {.count = 1, .reusable = true}}, SHIFT(20),
  [655] = {.entry = {.count = 1, .reusable = true}}, SHIFT(130),
  [657] = {.entry = {.count = 1, .reusable = true}}, SHIFT(284),
  [659] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_message_or_enum_type_repeat1, 2), SHIFT_REPEAT(274),
  [662] = {.entry = {.count = 1, .reusable = true}}, SHIFT(103),
  [664] = {.entry = {.count = 1, .reusable = true}}, SHIFT(280),
  [666] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_option, 3),
  [668] = {.entry = {.count = 1, .reusable = true}}, SHIFT(25),
  [670] = {.entry = {.count = 1, .reusable = true}}, SHIFT(114),
  [672] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_type, 1),
  [674] = {.entry = {.count = 1, .reusable = true}}, SHIFT(102),
  [676] = {.entry = {.count = 1, .reusable = true}}, SHIFT(9),
  [678] = {.entry = {.count = 1, .reusable = true}}, SHIFT(141),
  [680] = {.entry = {.count = 1, .reusable = true}}, SHIFT(158),
  [682] = {.entry = {.count = 1, .reusable = true}}, SHIFT(97),
  [684] = {.entry = {.count = 1, .reusable = true}}, SHIFT(138),
  [686] = {.entry = {.count = 1, .reusable = true}}, SHIFT(152),
  [688] = {.entry = {.count = 1, .reusable = true}}, SHIFT(126),
  [690] = {.entry = {.count = 1, .reusable = true}}, SHIFT(308),
  [692] = {.entry = {.count = 1, .reusable = true}}, SHIFT(245),
  [694] = {.entry = {.count = 1, .reusable = true}}, SHIFT(299),
  [696] = {.entry = {.count = 1, .reusable = true}}, SHIFT(46),
  [698] = {.entry = {.count = 1, .reusable = true}}, SHIFT(12),
  [700] = {.entry = {.count = 1, .reusable = true}}, SHIFT(143),
  [702] = {.entry = {.count = 1, .reusable = true}}, SHIFT(64),
  [704] = {.entry = {.count = 1, .reusable = true}}, SHIFT(273),
  [706] = {.entry = {.count = 1, .reusable = true}}, SHIFT(233),
  [708] = {.entry = {.count = 1, .reusable = true}}, SHIFT(49),
  [710] = {.entry = {.count = 1, .reusable = true}}, SHIFT(60),
  [712] = {.entry = {.count = 1, .reusable = true}}, SHIFT(247),
  [714] = {.entry = {.count = 1, .reusable = true}}, SHIFT(267),
  [716] = {.entry = {.count = 1, .reusable = true}}, SHIFT(306),
  [718] = {.entry = {.count = 1, .reusable = true}}, SHIFT(14),
  [720] = {.entry = {.count = 1, .reusable = true}}, SHIFT(139),
  [722] = {.entry = {.count = 1, .reusable = true}}, SHIFT(316),
  [724] = {.entry = {.count = 1, .reusable = true}}, SHIFT(8),
  [726] = {.entry = {.count = 1, .reusable = true}}, SHIFT(43),
  [728] = {.entry = {.count = 1, .reusable = true}}, SHIFT(254),
  [730] = {.entry = {.count = 1, .reusable = true}}, SHIFT(184),
  [732] = {.entry = {.count = 1, .reusable = true}}, SHIFT(67),
  [734] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_key_type, 1),
  [736] = {.entry = {.count = 1, .reusable = true}}, SHIFT(124),
  [738] = {.entry = {.count = 1, .reusable = true}}, SHIFT(128),
  [740] = {.entry = {.count = 1, .reusable = true}}, SHIFT(83),
  [742] = {.entry = {.count = 1, .reusable = true}}, SHIFT(84),
  [744] = {.entry = {.count = 1, .reusable = true}}, SHIFT(93),
  [746] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_service_name, 1),
  [748] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_rpc_name, 1),
  [750] = {.entry = {.count = 1, .reusable = true}}, SHIFT(122),
  [752] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message_name, 1),
  [754] = {.entry = {.count = 1, .reusable = true}}, SHIFT(118),
  [756] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_enum_name, 1),
  [758] = {.entry = {.count = 1, .reusable = true}}, SHIFT(51),
  [760] = {.entry = {.count = 1, .reusable = true}}, SHIFT(15),
  [762] = {.entry = {.count = 1, .reusable = true}}, SHIFT(116),
  [764] = {.entry = {.count = 1, .reusable = true}}, SHIFT(78),
  [766] = {.entry = {.count = 1, .reusable = true}}, SHIFT(77),
  [768] = {.entry = {.count = 1, .reusable = true}}, SHIFT(264),
  [770] = {.entry = {.count = 1, .reusable = true}}, SHIFT(111),
  [772] = {.entry = {.count = 1, .reusable = true}}, SHIFT(265),
  [774] = {.entry = {.count = 1, .reusable = true}}, SHIFT(283),
  [776] = {.entry = {.count = 1, .reusable = true}}, SHIFT(286),
  [778] = {.entry = {.count = 1, .reusable = true}}, SHIFT(52),
  [780] = {.entry = {.count = 1, .reusable = true}}, SHIFT(287),
  [782] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
  [784] = {.entry = {.count = 1, .reusable = true}}, SHIFT(117),
  [786] = {.entry = {.count = 1, .reusable = true}}, SHIFT(30),
  [788] = {.entry = {.count = 1, .reusable = true}}, SHIFT(10),
  [790] = {.entry = {.count = 1, .reusable = true}}, SHIFT(22),
  [792] = {.entry = {.count = 1, .reusable = true}}, SHIFT(17),
  [794] = {.entry = {.count = 1, .reusable = true}}, SHIFT(45),
  [796] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_message_or_enum_type_repeat1, 2),
  [798] = {.entry = {.count = 1, .reusable = true}}, SHIFT(142),
  [800] = {.entry = {.count = 1, .reusable = true}}, SHIFT(133),
  [802] = {.entry = {.count = 1, .reusable = true}}, SHIFT(217),
  [804] = {.entry = {.count = 1, .reusable = true}}, SHIFT(249),
  [806] = {.entry = {.count = 1, .reusable = true}}, SHIFT(293),
  [808] = {.entry = {.count = 1, .reusable = true}}, SHIFT(134),
  [810] = {.entry = {.count = 1, .reusable = true}}, SHIFT(53),
  [812] = {.entry = {.count = 1, .reusable = true}}, SHIFT(55),
  [814] = {.entry = {.count = 1, .reusable = true}}, SHIFT(50),
  [816] = {.entry = {.count = 1, .reusable = true}}, SHIFT(32),
  [818] = {.entry = {.count = 1, .reusable = true}}, SHIFT(296),
  [820] = {.entry = {.count = 1, .reusable = true}}, SHIFT(131),
};

#ifdef __cplusplus
extern "C" {
#endif
#ifdef _WIN32
#define extern __declspec(dllexport)
#endif

extern const TSLanguage *tree_sitter_proto(void) {
  static const TSLanguage language = {
    .version = LANGUAGE_VERSION,
    .symbol_count = SYMBOL_COUNT,
    .alias_count = ALIAS_COUNT,
    .token_count = TOKEN_COUNT,
    .external_token_count = EXTERNAL_TOKEN_COUNT,
    .state_count = STATE_COUNT,
    .large_state_count = LARGE_STATE_COUNT,
    .production_id_count = PRODUCTION_ID_COUNT,
    .field_count = FIELD_COUNT,
    .max_alias_sequence_length = MAX_ALIAS_SEQUENCE_LENGTH,
    .parse_table = &ts_parse_table[0][0],
    .small_parse_table = ts_small_parse_table,
    .small_parse_table_map = ts_small_parse_table_map,
    .parse_actions = ts_parse_actions,
    .symbol_names = ts_symbol_names,
    .field_names = ts_field_names,
    .field_map_slices = ts_field_map_slices,
    .field_map_entries = ts_field_map_entries,
    .symbol_metadata = ts_symbol_metadata,
    .public_symbol_map = ts_symbol_map,
    .alias_map = ts_non_terminal_alias_map,
    .alias_sequences = &ts_alias_sequences[0][0],
    .lex_modes = ts_lex_modes,
    .lex_fn = ts_lex,
    .primary_state_ids = ts_primary_state_ids,
  };
  return &language;
}
#ifdef __cplusplus
}
#endif
