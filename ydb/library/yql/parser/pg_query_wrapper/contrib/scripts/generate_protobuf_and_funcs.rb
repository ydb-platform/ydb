#!/usr/bin/env ruby

# rubocop:disable Metrics/MethodLength, Style/WordArray, Metrics/LineLength, Style/Documentation, Style/PerlBackrefs, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

require 'bundler'
require 'json'

class Generator
  def initialize
    @nodetypes = JSON.parse(File.read('./srcdata/nodetypes.json'))
    @struct_defs = JSON.parse(File.read('./srcdata/struct_defs.json'))
    @enum_defs = JSON.parse(File.read('./srcdata/enum_defs.json'))
    @typedefs = JSON.parse(File.read('./srcdata/typedefs.json'))
  end

  def underscore(camel_cased_word)
    return camel_cased_word unless /[A-Z-]|::/.match?(camel_cased_word)
    word = camel_cased_word.to_s.gsub("::", "/")
    word.gsub!(/^([A-Z\d])([A-Z][a-z])/, '\1__\2')
    word.gsub!(/([A-Z\d]+[a-z]+)([A-Z][a-z])/, '\1_\2')
    word.gsub!(/([a-z\d])([A-Z])/, '\1_\2')
    word.tr!("-", "_")
    word.downcase!
    word
  end

  TYPE_OVERRIDES = {
    ['Query', 'queryId'] => :skip, # we intentionally do not print the queryId field
  }
  OUTNAME_OVERRIDES = {
    ['CreateForeignTableStmt', 'base'] => 'base_stmt',
  }

  def generate_outmethods!
    @outmethods = {}
    @readmethods = {}
    @protobuf_messages = {}
    @protobuf_enums = {}
    @scan_protobuf_tokens = []
    @enum_to_strings = {}
    @enum_to_ints = {}
    @int_to_enums = {}

    ['nodes/parsenodes', 'nodes/primnodes'].each do |group|
      @struct_defs[group].each do |node_type, struct_def|
        @outmethods[node_type] = ''
        @readmethods[node_type] = ''
        @protobuf_messages[node_type] = ''
        protobuf_field_count = 1

        struct_def['fields'].each do |field_def|
          name = field_def['name']
          orig_type = field_def['c_type']

          # TODO: Add comments to protobuf definitions

          next unless name && orig_type

          type = TYPE_OVERRIDES[[node_type, name]] || orig_type
          outname = OUTNAME_OVERRIDES[[node_type, name]] || underscore(name)
          outname_json = name

          if type == :skip
            # Ignore
          elsif type == 'NodeTag'
            # Nothing
          elsif ['char'].include?(type)
            @outmethods[node_type] += format("  WRITE_CHAR_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_CHAR_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  string %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['bool'].include?(type)
            @outmethods[node_type] += format("  WRITE_BOOL_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_BOOL_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  bool %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['long'].include?(type)
            @outmethods[node_type] += format("  WRITE_LONG_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_LONG_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  int64 %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['int', 'int16', 'int32', 'AttrNumber'].include?(type)
            @outmethods[node_type] += format("  WRITE_INT_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_INT_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  int32 %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['uint', 'uint16', 'uint32', 'Index', 'bits32', 'Oid', 'AclMode', 'SubTransactionId'].include?(type)
            @outmethods[node_type] += format("  WRITE_UINT_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_UINT_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  uint32 %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif type == 'char*'
            @outmethods[node_type] += format("  WRITE_STRING_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_STRING_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  string %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['float', 'double', 'Cost', 'Selectivity'].include?(type)
            @outmethods[node_type] += format("  WRITE_FLOAT_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_FLOAT_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  double %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['Bitmapset*', 'Relids'].include?(type)
            @outmethods[node_type] += format("  WRITE_BITMAPSET_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_BITMAPSET_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  repeated uint64 %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['Value'].include?(type)
            @outmethods[node_type] += format("  WRITE_NODE_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_VALUE_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  Node %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['Value*'].include?(type)
            @outmethods[node_type] += format("  WRITE_NODE_PTR_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_VALUE_PTR_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  Node %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['List*'].include?(type)
            @outmethods[node_type] += format("  WRITE_LIST_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_LIST_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  repeated Node %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['Node*'].include?(type)
            @outmethods[node_type] += format("  WRITE_NODE_PTR_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_NODE_PTR_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  Node %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['Expr*'].include?(type)
            @outmethods[node_type] += format("  WRITE_NODE_PTR_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @readmethods[node_type] += format("  READ_EXPR_PTR_FIELD(%s, %s, %s);\n", outname, outname_json, name)
            @protobuf_messages[node_type] += format("  Node %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['Expr'].include?(type)
            # FIXME
            @protobuf_messages[node_type] += format("  Node %s = %d [json_name=\"%s\"];\n", outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif ['CreateStmt'].include?(type)
            @outmethods[node_type] += format("  WRITE_SPECIFIC_NODE_FIELD(%s, %s, %s, %s, %s);\n", type.gsub('*', ''), underscore(type.gsub('*', '')).downcase, outname, outname_json, name)
            @readmethods[node_type] += format("  READ_SPECIFIC_NODE_FIELD(%s, %s, %s, %s, %s);\n", type.gsub('*', ''), underscore(type.gsub('*', '')).downcase, outname, outname_json, name)
            @protobuf_messages[node_type] += format("  %s %s = %d [json_name=\"%s\"];\n", type.gsub('*', ''), outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif @nodetypes.include?(type[0..-2])
            @outmethods[node_type] += format("  WRITE_SPECIFIC_NODE_PTR_FIELD(%s, %s, %s, %s, %s);\n", type.gsub('*', ''), underscore(type.gsub('*', '')).downcase, outname, outname_json, name)
            @readmethods[node_type] += format("  READ_SPECIFIC_NODE_PTR_FIELD(%s, %s, %s, %s, %s);\n", type.gsub('*', ''), underscore(type.gsub('*', '')).downcase, outname, outname_json, name)
            @protobuf_messages[node_type] += format("  %s %s = %d [json_name=\"%s\"];\n", type.gsub('*', ''), outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elsif type.end_with?('*')
            puts format('ERR: %s %s', name, type)
          else # Enum
            @outmethods[node_type] += format("  WRITE_ENUM_FIELD(%s, %s, %s, %s);\n", type, outname, outname_json, name)
            @readmethods[node_type] += format("  READ_ENUM_FIELD(%s, %s, %s, %s);\n", type, outname, outname_json, name)
            @protobuf_messages[node_type] += format("  %s %s = %d [json_name=\"%s\"];\n", type, outname, protobuf_field_count, name)
            protobuf_field_count += 1
          end
        end
      end
    end

    ['nodes/parsenodes', 'nodes/primnodes', 'nodes/nodes', 'nodes/lockoptions'].each do |group|
      @enum_defs[group].each do |enum_type, enum_def|
        next if enum_type == 'NodeTag'

        @protobuf_enums[enum_type] = format("enum %s\n{\n", enum_type)
        @enum_to_strings[enum_type] = format("static const char*\n_enumToString%s(%s value) {\n  switch(value) {\n", enum_type, enum_type)
        @enum_to_ints[enum_type] = format("static int\n_enumToInt%s(%s value) {\n  switch(value) {\n", enum_type, enum_type)
        @int_to_enums[enum_type] = format("static %s\n_intToEnum%s(int value) {\n  switch(value) {\n", enum_type, enum_type)

        # We intentionally add a dummy field for the zero value, that actually is not used in practice
        # - this ensures that the JSON output always includes the enum value (and doesn't skip it because its the zero value)
        @protobuf_enums[enum_type] += format("  %s_UNDEFINED = 0;\n", underscore(enum_type).upcase)
        protobuf_field = 1

        enum_def['values'].each do |value|
          next unless value['name']

          @protobuf_enums[enum_type] += format("  %s = %d;\n", value['name'], protobuf_field)
          @enum_to_strings[enum_type] += format("    case %s: return \"%s\";\n", value['name'], value['name'])
          @enum_to_ints[enum_type] += format("    case %s: return %d;\n", value['name'], protobuf_field)
          @int_to_enums[enum_type] += format("    case %d: return %s;\n", protobuf_field, value['name'])
          protobuf_field += 1
        end

        @protobuf_enums[enum_type] += "}"
        @enum_to_strings[enum_type] += "  }\n  Assert(false);\n  return NULL;\n}"
        @enum_to_ints[enum_type] += "  }\n  Assert(false);\n  return -1;\n}"
        @int_to_enums[enum_type] += format("  }\n  Assert(false);\n  return %s;\n}",  enum_def['values'].map { |v| v['name'] }.compact.first)
      end
    end

    scan_values = @enum_defs['../backend/parser/gram']['yytokentype']['values']
    scan_values.each do |value|
      next unless value['name']
      @scan_protobuf_tokens << format('%s = %d;', value['name'], value['value'])
    end

    @typedefs.each do |typedef|
      next unless @outmethods[typedef['source_type']]

      @outmethods[typedef['new_type_name']] = @outmethods[typedef['source_type']]
      @readmethods[typedef['new_type_name']] = @readmethods[typedef['source_type']]
      @protobuf_messages[typedef['new_type_name']] = @protobuf_messages[typedef['source_type']]
    end
  end

  IGNORE_LIST = [
    'Value', # Special case
    'Const', # Only needed in post-parse analysis (and it introduces Datums, which we can't output)
  ]
  EXPLICT_TAG_SETS = [ # These nodes need an explicit NodeSetTag during read funcs because they are a superset of another node
    'CreateForeignTableStmt',
  ]
  def generate!
    generate_outmethods!

    out_defs = ''
    out_impls = ''
    out_conds = "case T_Integer:
  OUT_NODE(Integer, Integer, integer, INTEGER, Value, integer);
  break;
case T_Float:
  OUT_NODE(Float, Float, float, FLOAT, Value, float_);
  break;
case T_String:
  OUT_NODE(String, String, string, STRING, Value, string);
  break;
case T_BitString:
  OUT_NODE(BitString, BitString, bit_string, BIT_STRING, Value, bit_string);
  break;
case T_Null:
  OUT_NODE(Null, Null, null, NULL, Value, null);
  break;
case T_List:
  OUT_NODE(List, List, list, LIST, List, list);
  break;
case T_IntList:
  OUT_NODE(IntList, IntList, int_list, INT_LIST, List, int_list);
  break;
case T_OidList:
  OUT_NODE(OidList, OidList, oid_list, OID_LIST, List, oid_list);
  break;
"
    read_defs = ''
    read_impls = ''
    read_conds = ''
    protobuf_messages = ''
    protobuf_nodes = []

    @nodetypes.each do |type|
      next if IGNORE_LIST.include?(type)
      outmethod = @outmethods[type]
      readmethod = @readmethods[type]
      next unless outmethod && readmethod

      c_type = type.gsub(/_/, '')

      out_defs += format("static void _out%s(OUT_TYPE(%s, %s) out_node, const %s *node);\n", c_type, type, c_type, type)

      out_impls += "static void\n"
      out_impls += format("_out%s(OUT_TYPE(%s, %s) out, const %s *node)\n", c_type, type, c_type, type)
      out_impls += "{\n"
      out_impls += outmethod
      out_impls += "}\n"
      out_impls += "\n"

      out_conds += format("case T_%s:\n", type)
      out_conds += format("  OUT_NODE(%s, %s, %s, %s, %s, %s);\n", type, c_type, underscore(c_type), underscore(c_type).upcase.gsub('__', '_'), type, underscore(type))
      out_conds += "  break;\n"

      read_defs += format("static %s * _read%s(OUT_TYPE(%s, %s) msg);\n", type, c_type, type, c_type)

      read_impls += format("static %s *\n", type)
      read_impls += format("_read%s(OUT_TYPE(%s, %s) msg)\n", c_type, type, c_type)
      read_impls += "{\n"
      read_impls += format("  %s *node = makeNode(%s);\n", type, type)
      read_impls += readmethod
      read_impls += format("  NodeSetTag(node, T_%s);\n", type) if EXPLICT_TAG_SETS.include?(type)
      read_impls += "  return node;\n"
      read_impls += "}\n"
      read_impls += "\n"

      read_conds += format("  READ_COND(%s, %s, %s, %s, %s, %s);\n", type, c_type, underscore(c_type), underscore(c_type).upcase.gsub('__', '_'), type, underscore(type))

      protobuf_messages += format("message %s\n{\n", type)
      protobuf_messages += @protobuf_messages[type] || ''
      protobuf_messages += "}\n\n"

      protobuf_nodes << format("%s %s = %d [json_name=\"%s\"];", type, underscore(type), protobuf_nodes.size + 1, type)
    end

    ['Integer', 'Float', 'String', 'BitString', 'Null', 'List', 'IntList', 'OidList'].each do |type|
      protobuf_nodes << format("%s %s = %d [json_name=\"%s\"];", type, underscore(type), protobuf_nodes.size + 1, type)
    end

    protobuf_messages += @protobuf_enums.values.join("\n\n")

    File.write('./src/pg_query_enum_defs.c', "// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.rb\n\n" +
      @enum_to_strings.values.join("\n\n") + @enum_to_ints.values.join("\n\n") + @int_to_enums.values.join("\n\n"))

    File.write('./src/pg_query_outfuncs_defs.c', "// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.rb\n\n" +
      out_defs + "\n\n" + out_impls)

    File.write('./src/pg_query_outfuncs_conds.c', "// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.rb\n\n" + out_conds)

    File.write('./src/pg_query_readfuncs_defs.c', "// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.rb\n\n" +
      read_defs + "\n\n" + read_impls)

    File.write('./src/pg_query_readfuncs_conds.c', "// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.rb\n\n" + read_conds)

    protobuf = "// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.rb

syntax = \"proto3\";

package pg_query;

message ParseResult {
  int32 version = 1;
  repeated RawStmt stmts = 2;
}

message ScanResult {
  int32 version = 1;
  repeated ScanToken tokens = 2;
}

message Node {
  oneof node {
		#{protobuf_nodes.join("\n    ")}
  }
}

message Integer
{
  int32 ival = 1; /* machine integer */
}

message Float
{
  string str = 1; /* string */
}

message String
{
  string str = 1; /* string */
}

message BitString
{
  string str = 1; /* string */
}

message Null
{
  // intentionally empty
}

message List
{
  repeated Node items = 1;
}

message OidList
{
  repeated Node items = 1;
}

message IntList
{
  repeated Node items = 1;
}

#{protobuf_messages}

message ScanToken {
  int32 start = 1;
  int32 end = 2;
  Token token = 4;
  KeywordKind keyword_kind = 5;
}

enum KeywordKind {
  NO_KEYWORD = 0;
  UNRESERVED_KEYWORD = 1;
  COL_NAME_KEYWORD = 2;
  TYPE_FUNC_NAME_KEYWORD = 3;
  RESERVED_KEYWORD = 4;
}

enum Token {
  NUL = 0;
  // Single-character tokens that are returned 1:1 (identical with \"self\" list in scan.l)
  // Either supporting syntax, or single-character operators (some can be both)
  // Also see https://www.postgresql.org/docs/12/sql-syntax-lexical.html#SQL-SYNTAX-SPECIAL-CHARS
  ASCII_37 = 37; // \"%\"
  ASCII_40 = 40; // \"\(\"
  ASCII_41 = 41; // \")\"
  ASCII_42 = 42; // \"*\"
  ASCII_43 = 43; // \"+\"
  ASCII_44 = 44; // \",\"
  ASCII_45 = 45; // \"-\"
  ASCII_46 = 46; // \".\"
  ASCII_47 = 47; // \"/\"
  ASCII_58 = 58; // \":\"
  ASCII_59 = 59; // \";\"
  ASCII_60 = 60; // \"<\"
  ASCII_61 = 61; // \"=\"
  ASCII_62 = 62; // \">\"
  ASCII_63 = 63; // \"?\"
  ASCII_91 = 91; // \"[\"
  ASCII_92 = 92; // \"\\\"
  ASCII_93 = 93; // \"]\"
  ASCII_94 = 94; // \"^\"
  // Named tokens in scan.l
  #{@scan_protobuf_tokens.join("\n  ")}
}
"

    File.write('./protobuf/pg_query.proto', protobuf)
  end
end

Generator.new.generate!
