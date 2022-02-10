#!/usr/bin/env ruby

# rubocop:disable Metrics/AbcSize, Metrics/LineLength, Metrics/MethodLength, Style/WordArray, Metrics/ClassLength, Style/Documentation, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Style/MutableConstant, Style/TrailingCommaInLiteral

require 'bundler'
require 'json'

class Generator
  def initialize
    @nodetypes = JSON.parse(File.read('./srcdata/nodetypes.json'))
    @struct_defs = JSON.parse(File.read('./srcdata/struct_defs.json'))
    @enum_defs = JSON.parse(File.read('./srcdata/enum_defs.json'))
    @typedefs = JSON.parse(File.read('./srcdata/typedefs.json'))
    @all_known_enums = JSON.parse(File.read('./srcdata/all_known_enums.json'))
  end

  FINGERPRINT_RES_TARGET_NAME = <<-EOL
  if (node->name != NULL && (field_name == NULL || parent == NULL || !IsA(parent, SelectStmt) || strcmp(field_name, "targetList") != 0)) {
    _fingerprintString(ctx, "name");
    _fingerprintString(ctx, node->name);
  }

  EOL

  FINGERPRINT_RANGE_VAR_RELNAME = <<-EOL
  if (node->relname != NULL && node->relpersistence != 't') {
    int len = strlen(node->relname);
    char *r = palloc0((len + 1) * sizeof(char));
    char *p = r;
    for (int i = 0; i < len; i++) {
      if (node->relname[i] >= '0' && node->relname[i] <= '9' &&
          ((i + 1 < len && node->relname[i + 1] >= '0' && node->relname[i + 1] <= '9') ||
           (i > 0 && node->relname[i - 1] >= '0' && node->relname[i - 1] <= '9'))) {
        // Skip
      } else {
        *p = node->relname[i];
        p++;
      }
    }
    *p = 0;
    _fingerprintString(ctx, "relname");
    _fingerprintString(ctx, r);
    pfree(r);
  }

  EOL

  FINGERPRINT_A_EXPR_KIND = <<-EOL
  if (true) {
    _fingerprintString(ctx, "kind");
    if (node->kind == AEXPR_OP_ANY || node->kind == AEXPR_IN)
      _fingerprintString(ctx, "AEXPR_OP");
    else
      _fingerprintString(ctx, _enumToStringA_Expr_Kind(node->kind));
  }

  EOL

  FINGERPRINT_NODE = <<-EOL
  if (true) {
    XXH3_state_t* prev = XXH3_createState();
    XXH64_hash_t hash;

    XXH3_copyState(prev, ctx->xxh_state);
    _fingerprintString(ctx, "%<name>s");

    hash = XXH3_64bits_digest(ctx->xxh_state);
    _fingerprintNode(ctx, &node->%<name>s, node, "%<name>s", depth + 1);
    if (hash == XXH3_64bits_digest(ctx->xxh_state)) {
      XXH3_copyState(ctx->xxh_state, prev);
      if (ctx->write_tokens)
        dlist_delete(dlist_tail_node(&ctx->tokens));
    }
    XXH3_freeState(prev);
  }

  EOL

  FINGERPRINT_NODE_PTR = <<-EOL
  if (node->%<name>s != NULL) {
    XXH3_state_t* prev = XXH3_createState();
    XXH64_hash_t hash;

    XXH3_copyState(prev, ctx->xxh_state);
    _fingerprintString(ctx, "%<name>s");

    hash = XXH3_64bits_digest(ctx->xxh_state);
    _fingerprintNode(ctx, node->%<name>s, node, "%<name>s", depth + 1);
    if (hash == XXH3_64bits_digest(ctx->xxh_state)) {
      XXH3_copyState(ctx->xxh_state, prev);
      if (ctx->write_tokens)
        dlist_delete(dlist_tail_node(&ctx->tokens));
    }
    XXH3_freeState(prev);
  }

  EOL

  FINGERPRINT_SPECIFIC_NODE_PTR = <<-EOL
  if (node->%<name>s != NULL) {
    XXH3_state_t* prev = XXH3_createState();
    XXH64_hash_t hash;

    XXH3_copyState(prev, ctx->xxh_state);
    _fingerprintString(ctx, "%<name>s");

    hash = XXH3_64bits_digest(ctx->xxh_state);
    _fingerprint%<typename>s(ctx, node->%<name>s, node, "%<name>s", depth + 1);
    if (hash == XXH3_64bits_digest(ctx->xxh_state)) {
      XXH3_copyState(ctx->xxh_state, prev);
      if (ctx->write_tokens)
        dlist_delete(dlist_tail_node(&ctx->tokens));
    }
    XXH3_freeState(prev);
  }

  EOL

  FINGERPRINT_LIST = <<-EOL
  if (node->%<name>s != NULL && node->%<name>s->length > 0) {
    XXH3_state_t* prev = XXH3_createState();
    XXH64_hash_t hash;

    XXH3_copyState(prev, ctx->xxh_state);
    _fingerprintString(ctx, "%<name>s");

    hash = XXH3_64bits_digest(ctx->xxh_state);
    _fingerprintNode(ctx, node->%<name>s, node, "%<name>s", depth + 1);
    if (hash == XXH3_64bits_digest(ctx->xxh_state)) {
      XXH3_copyState(ctx->xxh_state, prev);
      if (ctx->write_tokens)
        dlist_delete(dlist_tail_node(&ctx->tokens));
    }
    XXH3_freeState(prev);
  }
  EOL

  FINGERPRINT_INT = <<-EOL
  if (node->%<name>s != 0) {
    char buffer[50];
    sprintf(buffer, "%%d", node->%<name>s);
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, buffer);
  }

  EOL

  FINGERPRINT_LONG_INT = <<-EOL
  if (node->%<name>s != 0) {
    char buffer[50];
    sprintf(buffer, "%%ld", node->%<name>s);
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, buffer);
  }

  EOL

  FINGERPRINT_FLOAT = <<-EOL
  if (node->%<name>s != 0) {
    char buffer[50];
    sprintf(buffer, "%%f", node->%<name>s);
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, buffer);
  }

  EOL

  FINGERPRINT_CHAR = <<-EOL
  if (node->%<name>s != 0) {
    char buffer[2] = {node->%<name>s, '\\0'};
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, buffer);
  }

  EOL

  FINGERPRINT_CHAR_PTR = <<-EOL
  if (node->%<name>s != NULL) {
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, node->%<name>s);
  }

  EOL

  FINGERPRINT_STRING = <<-EOL
  if (strlen(node->%<name>s) > 0) {
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, node->%<name>s);
  }

  EOL

  FINGERPRINT_BOOL = <<-EOL
  if (node->%<name>s) {
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, "true");
  }

  EOL

  FINGERPRINT_INT_ARRAY = <<-EOL
  if (true) {
    int x;
    Bitmapset	*bms = bms_copy(node->%<name>s);

    _fingerprintString(ctx, "%<name>s");

  	while ((x = bms_first_member(bms)) >= 0) {
      char buffer[50];
      sprintf(buffer, "%%d", x);
      _fingerprintString(ctx, buffer);
    }

    bms_free(bms);
  }

  EOL

  FINGERPRINT_ENUM = <<-EOL
  if (true) {
    _fingerprintString(ctx, "%<name>s");
    _fingerprintString(ctx, _enumToString%<typename>s(node->%<name>s));
  }

  EOL

  # Fingerprinting additional code to be inserted
  FINGERPRINT_SKIP_NODES = [
    'A_Const',
    'Alias',
    'ParamRef',
    'SetToDefault',
    'IntList',
    'OidList',
    'Null',
  ]
  FINGERPRINT_OVERRIDE_FIELDS = {
    [nil, 'location'] => :skip,
    ['ResTarget', 'name'] => FINGERPRINT_RES_TARGET_NAME,
    ['RangeVar', 'relname'] => FINGERPRINT_RANGE_VAR_RELNAME,
    ['A_Expr', 'kind'] => FINGERPRINT_A_EXPR_KIND,
    ['PrepareStmt', 'name'] => :skip,
    ['ExecuteStmt', 'name'] => :skip,
    ['DeallocateStmt', 'name'] => :skip,
    ['TransactionStmt', 'options'] => :skip,
    ['TransactionStmt', 'gid'] => :skip,
    ['TransactionStmt', 'savepoint_name'] => :skip,
    ['DeclareCursorStmt', 'portalname'] => :skip,
    ['FetchStmt', 'portalname'] => :skip,
    ['ClosePortalStmt', 'portalname'] => :skip,
    ['RawStmt', 'stmt_len'] => :skip,
    ['RawStmt', 'stmt_location'] => :skip,
  }
  INT_TYPES = ['bits32', 'uint32', 'int', 'int32', 'uint16', 'int16', 'Oid', 'Index', 'AclMode', 'AttrNumber', 'SubTransactionId']
  LONG_INT_TYPES = ['long', 'uint64']
  INT_ARRAY_TYPES = ['Bitmapset*', 'Bitmapset', 'Relids']
  FLOAT_TYPES = ['Cost', 'double']

  IGNORE_FOR_GENERATOR = ['Integer', 'Float', 'String', 'BitString', 'List']

  def generate_fingerprint_defs!
    @fingerprint_defs = {}

    ['nodes/parsenodes', 'nodes/primnodes'].each do |group|
      @struct_defs[group].each do |type, struct_def|
        next if struct_def['fields'].nil?
        next if IGNORE_FOR_GENERATOR.include?(type)

        if FINGERPRINT_SKIP_NODES.include?(type)
          fingerprint_def = "  // Intentionally ignoring all fields for fingerprinting\n"
        else
          fingerprint_def = ''
          struct_def['fields'].reject { |f| f['name'].nil? }.sort_by { |f| f['name'] }.each do |field|
            name = field['name']
            field_type = field['c_type']

            fp_override = FINGERPRINT_OVERRIDE_FIELDS[[type, field['name']]] || FINGERPRINT_OVERRIDE_FIELDS[[nil, field['name']]]
            if fp_override
              if fp_override == :skip
                fp_override = format("  // Intentionally ignoring node->%s for fingerprinting\n\n", name)
              end
              fingerprint_def += fp_override
              next
            end

            case field_type
            # when '[][]Node'
            #  fingerprint_def += format(FINGERPRINT_NODE_ARRAY_ARRAY, name: name)
            # when '[]Node'
            #  fingerprint_def += format(FINGERPRINT_NODE_ARRAY, name: name)
            when 'Node'
              fingerprint_def += format(FINGERPRINT_NODE, name: name)
            when 'Node*', 'Expr*'
              fingerprint_def += format(FINGERPRINT_NODE_PTR, name: name)
            when 'List*'
              fingerprint_def += format(FINGERPRINT_LIST, name: name)
            when 'CreateStmt'
              fingerprint_def += format("  _fingerprintString(ctx, \"%s\");\n", name)
              fingerprint_def += format("  _fingerprintCreateStmt(ctx, (const CreateStmt*) &node->%s, node, \"%s\", depth);\n", name, name)
            when 'char'
              fingerprint_def += format(FINGERPRINT_CHAR, name: name)
            when 'char*'
              fingerprint_def += format(FINGERPRINT_CHAR_PTR, name: name)
            when 'string'
              fingerprint_def += format(FINGERPRINT_STRING, name: name)
            when 'bool'
              fingerprint_def += format(FINGERPRINT_BOOL, name: name)
            when 'Datum', 'void*', 'Expr', 'NodeTag'
              # Ignore
            when *INT_TYPES
              fingerprint_def += format(FINGERPRINT_INT, name: name)
            when *LONG_INT_TYPES
              fingerprint_def += format(FINGERPRINT_LONG_INT, name: name)
            when *INT_ARRAY_TYPES
              fingerprint_def += format(FINGERPRINT_INT_ARRAY, name: name)
            when *FLOAT_TYPES
              fingerprint_def += format(FINGERPRINT_FLOAT, name: name)
            else
              if field_type.end_with?('*') && @nodetypes.include?(field_type[0..-2])
                typename = field_type[0..-2]
                typename = 'Node' if typename == 'Value'
                fingerprint_def += format(FINGERPRINT_SPECIFIC_NODE_PTR, name: name, typename: typename)
              elsif @all_known_enums.include?(field_type)
                fingerprint_def += format(FINGERPRINT_ENUM, name: name, typename: field_type)
              else
                # This shouldn't happen - if it does the above is missing something :-)
                puts type
                puts name
                puts field_type
                raise type
              end
            end
          end
        end

        @fingerprint_defs[type] = fingerprint_def
      end
    end
  end

  def generate!
    generate_fingerprint_defs!

    defs = ''
    conds = ''

    @nodetypes.each do |type|
      fingerprint_def = @fingerprint_defs[type]
      next unless fingerprint_def
      defs += format("static void _fingerprint%s(FingerprintContext *ctx, const %s *node, const void *parent, const char *field_name, unsigned int depth);\n", type, type)
    end
    defs += "\n\n"

    @nodetypes.each do |type|
      # next if IGNORE_LIST.include?(type)
      fingerprint_def = @fingerprint_defs[type]
      next unless fingerprint_def

      defs += "static void\n"
      defs += format("_fingerprint%s(FingerprintContext *ctx, const %s *node, const void *parent, const char *field_name, unsigned int depth)\n", type, type)
      defs += "{\n"
      defs += fingerprint_def
      defs += "}\n"
      defs += "\n"

      conds += format("case T_%s:\n", type)
      if FINGERPRINT_SKIP_NODES.include?(type)
        conds += format("  // Intentionally ignoring for fingerprinting\n")
      else
        conds += format("  if (!IsA(castNode(TypeCast, (void*) obj)->arg, A_Const) && !IsA(castNode(TypeCast, (void*) obj)->arg, ParamRef))\n  {\n") if type == 'TypeCast'
        conds += format("  _fingerprintString(ctx, \"%s\");\n", type)
        conds += format("  _fingerprint%s(ctx, obj, parent, field_name, depth);\n", type)
        conds += "  }\n" if type == 'TypeCast'
      end
      conds += "  break;\n"
    end

    File.write('./src/pg_query_fingerprint_defs.c', defs)
    File.write('./src/pg_query_fingerprint_conds.c', conds)
  end
end

Generator.new.generate!
