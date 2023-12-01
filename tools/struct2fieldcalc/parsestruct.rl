#include <library/cpp/getopt/small/opt2.h>

#include <library/cpp/deprecated/fgood/fgood.h>
#include <util/generic/hash.h>
#include <util/string/split.h>
#include <util/string/printf.h>

#include <map>
#include <set>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#include <fcntl.h>

// For ragel
static char *tokstart, *tokend;
static int act, cs;

static int curly_level = 0;
static int wanted_curly_level = -1;
static int wanted_curly_level2 = -1; // for nested anonymous union / struct

static char *dmp_name_start = 0;
static char *dmp_name_end = 0;
static char *dmp_from_start = 0;
static char *dmp_from_end = 0;
static char *dmp_to_start = 0;
static char *dmp_to_end = 0;
static char *dmp_mode_start = 0;

// Parser state
static TString current_struct;
static TString current_enum;
static TString last_expression;

int line;
int last_entry_line;
TString current_file;

// List of all emitted callbacks
typedef std::vector<std::pair<TString, int> > callback_list;
static callback_list getvals;
static callback_list prints;

// Enum -> vector of enumerators name map
typedef std::vector<TString> enumerators_list;
typedef std::map<TString, enumerators_list> enums_map;
static enums_map enums;

struct dump_items_list : public std::vector<std::pair<TString, TString> > {
    std::set<TString> wanted_enums;
    std::vector<TString> efunctions;
};
typedef std::map<TString, dump_items_list> structs_map;
static structs_map structs;

// Token types for parser
enum TokenType {
    TT_IDENTIFIER,
    TT_ENUM,
    TT_STRUCT,
    TT_UNION,
    TT_CONST,
    TT_PUBLIC,
    TT_DECIMAL,
    TT_COMMA,
    TT_EQUALS,
    TT_SEMICOLON,
    TT_COLON,
    TT_OPENSQUARE,
    TT_CLOSESQUARE,
    TT_OPENPAREN,
    TT_CLOSEPAREN,
    TT_OPENCURLY,
    TT_CLOSECURLY,
    TT_PIPE,
    TT_COMPLEXTYPE,

    TT_OTHER,
};

// Structure for single token found by ragel
struct Token {
    Token(int t, char *begin, int length): type(t), string(begin, length) {}

    bool check(int t) {
        return t == type;
    }

    int type;
    TString string;
};

// Stack of currently accumilated tokens
typedef std::vector<Token> TokenStack;
TokenStack token_stack;

// Parser state
static enum {
    DEFAULT,
    ENUM,
    STRUCT,
} state = DEFAULT;

void errexit(const char *fmt, ...) {
    fprintf(stderr, "%s:%d: ", current_file.data(), line);

    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);

    fprintf(stderr, "\n");

    exit(1);
}

void emit_entry(const TString &name, bool nosave, const char *fmt, ...) {
    char buffer[1024];

    TString expression;

    va_list ap;
    va_start(ap, fmt);
    if (vsnprintf(buffer, sizeof(buffer) - 1, fmt, ap) >= (int)sizeof(buffer) - 1)
        errexit("expression buffer overflow");
    va_end(ap);

    expression = buffer;

    if (!nosave)
        last_expression = expression;

    last_entry_line = line;
    structs[current_struct].push_back(std::make_pair(name, expression));
}

void emit_message(const char *fmt, ...) {
    char buffer[1024];

    va_list ap;
    va_start(ap, fmt);
    if (vsnprintf(buffer, sizeof(buffer) - 1, fmt, ap) >= (int)sizeof(buffer) - 1)
        errexit("expression buffer overflow");
    va_end(ap);

    structs[current_struct].push_back(std::make_pair(TString(), buffer));
}

// Checks token stack to match specific length and token type pattern
bool check_token_stack(int count, ...) {
    int len = token_stack.size();
    if (len < count)
        return false;

    va_list ap;
    va_start(ap, count);

    for (int i = 0; i < count; i++) {
        if (!token_stack[i+len-count].check(va_arg(ap, int))) {
            va_end(ap);
            return false;
        }
    }

    va_end(ap);
    return true;
}

// Test all available patterns when parsing enums
bool check_enum_patterns() {
    int len = token_stack.size();

    /* enumerator: IDENTIFIER = DECIMAL */
    if (check_token_stack(3, TT_IDENTIFIER, TT_EQUALS, TT_DECIMAL)) {
        enums[current_enum].push_back(token_stack[len - 3].string);
        return true;
    }

    /* enumerator: IDENTIFIER = IDENTIFIER [|] IDENTIFIER [[|] IDENTIFIER ...]*/
    if (check_token_stack(4, TT_IDENTIFIER, TT_PIPE, TT_IDENTIFIER, TT_COMMA)) {
        int i = token_stack.size() - 3;
        for (; i > 0; i--)
            if (!token_stack[i].check(TT_IDENTIFIER) && !token_stack[i].check(TT_PIPE))
                break;
        if (i > 0 && token_stack[i].check(TT_EQUALS) && token_stack[i - 1].check(TT_IDENTIFIER)) {
            enums[current_enum].push_back(token_stack[i - 1].string);
            return true;
        }
    }

    return false;
}

// Test all available patterns when parsing structs
bool check_struct_patterns() {
    int len = token_stack.size();

    /* field: IDENTIFIER [IDENTIFIER ...] IDENTIFIER */
    if (check_token_stack(3, TT_IDENTIFIER, TT_IDENTIFIER, TT_SEMICOLON)) {
        emit_entry(token_stack[len - 2].string, false, "&((%s*)0)->%s", current_struct.data(), token_stack[len - 2].string.c_str());
        return true;
    }

    /* bitfield: IDENTIFIER [IDENTIFIER ...] IDENTIFIER : DECIMAL ; */
    if (check_token_stack(5, TT_IDENTIFIER, TT_IDENTIFIER, TT_COLON, TT_DECIMAL, TT_SEMICOLON)) {
        const TString &type = token_stack[len - 5].string;
        const TString &name = token_stack[len - 4].string;
        structs[current_struct].efunctions.push_back(Sprintf("%s Get%sFrom%s(const %s *ptr){ return ptr->%s; }", type.data(), name.data(), current_struct.data(), current_struct.data(), name.data()));
        emit_entry(token_stack[len - 4].string, false, "dump_item((%s_ext_fn_t)Get%sFrom%s, \"%s\")", type.data(), name.data(), current_struct.data(), name.data());
        //errexit("bitfields currently unsupported");
        return true;
    }

    /* array field: IDENTIFIER [IDENTIFIER ...] IDENTIFIER [ DECIMAL ] ; */
    if (check_token_stack(6, TT_IDENTIFIER, TT_IDENTIFIER, TT_OPENSQUARE, TT_DECIMAL, TT_CLOSESQUARE, TT_SEMICOLON)) {
        emit_entry(token_stack[len - 5].string, false, "dump_item(&((%s*)0)->%s[0], %s)", current_struct.data(), token_stack[len - 5].string.c_str(), token_stack[len - 3].string.c_str());
        return true;
    }

    /* array field: char [IDENTIFIER ...] IDENTIFIER [ IDENTIFIER */
    if (check_token_stack(4, TT_IDENTIFIER, TT_IDENTIFIER, TT_OPENSQUARE, TT_IDENTIFIER) && token_stack[len - 4].string == "char") {
        emit_entry(token_stack[len - 3].string, false, "dump_item(&((%s*)0)->%s[0])", current_struct.data(), token_stack[len - 3].string.c_str());
        return true;
    }

    /* simple function: IDENTIFIER [IDENTIFIER ...] IDENTIFIER ( ) const { */
    if (check_token_stack(6, TT_IDENTIFIER, TT_IDENTIFIER, TT_OPENPAREN, TT_CLOSEPAREN, TT_CONST, TT_OPENCURLY)) {
        emit_entry(token_stack[len - 5].string, false, "(%s_fn_t)&%s::%s", token_stack[len-6].string.c_str(), current_struct.data(), token_stack[len - 5].string.c_str());
        return true;
    }

    /* special function: COMPLEXTYPE IDENTIFIER ( ) const { */
    if (check_token_stack(6, TT_COMPLEXTYPE, TT_IDENTIFIER, TT_OPENPAREN, TT_CLOSEPAREN, TT_CONST, TT_OPENCURLY)) {
        emit_entry(token_stack[len - 5].string, false, "(%s_fn_t)&%s::%s", /*token_stack[len-6].string.c_str()*/"str", current_struct.data(), token_stack[len - 5].string.c_str());
        return true;
    }

    /* special function: COMPLEXTYPE IDENTIFIER (TString &buf, COMPLEXTYPE IDENTIFIER) const { */
    if (check_token_stack(12, TT_COMPLEXTYPE, TT_IDENTIFIER, TT_OPENPAREN, TT_IDENTIFIER, TT_OTHER, TT_IDENTIFIER, TT_COMMA, TT_COMPLEXTYPE, TT_IDENTIFIER, TT_CLOSEPAREN, TT_CONST, TT_SEMICOLON)) {
        emit_entry(token_stack[len - 11].string, false, "(%s_fn_t)&%s::%s", /*token_stack[len-12].string.c_str()*/"strbuf_2", current_struct.data(), token_stack[len - 11].string.c_str());
        return true;
    }

    return false;
}

// Process single token
void process_token(int type) {
    // Skip stuff with higher nesting level than we need
    if (wanted_curly_level > 0 && curly_level > wanted_curly_level2)
        return;

    // Add token to stack
    token_stack.push_back(Token(type, tokstart, tokend - tokstart));
    int len = token_stack.size();

    // Match end of token stack with patterns depending on mode
    switch(state) {
    case ENUM:
        if (check_enum_patterns())
            token_stack.clear();
        return;

    case STRUCT:
        if (check_struct_patterns())
            token_stack.clear();
        if (check_token_stack(2, TT_STRUCT, TT_OPENCURLY) || check_token_stack(2, TT_UNION, TT_OPENCURLY))
            wanted_curly_level2 = curly_level + 1;
        return;

    default: // state == DEFAULT
        {
            // enum encountered
            if (check_token_stack(3, TT_ENUM, TT_IDENTIFIER, TT_OPENCURLY)) {
                state = ENUM;
                current_enum = token_stack[len - 2].string;
                current_struct = "";
                wanted_curly_level = wanted_curly_level2 = curly_level + 1;
                token_stack.clear();
                return;
            }

            // struct encountered
            int id_pos = 0, base_pos = 0;
            if (check_token_stack(3, TT_STRUCT, TT_IDENTIFIER, TT_OPENCURLY))
                id_pos = len - 2;
            if (check_token_stack(5, TT_STRUCT, TT_IDENTIFIER, TT_COLON, TT_IDENTIFIER, TT_OPENCURLY))
                id_pos = len - 4, base_pos = len - 2;
            if (check_token_stack(6, TT_STRUCT, TT_IDENTIFIER, TT_COLON, TT_PUBLIC, TT_IDENTIFIER, TT_OPENCURLY))
                id_pos = len - 5, base_pos = len - 2;

            if (id_pos) {
                state = STRUCT;
                current_enum = "";
                current_struct = token_stack[id_pos].string;

                wanted_curly_level = wanted_curly_level2 = curly_level+1;

                if (structs.find(current_struct) != structs.end())
                    errexit("struct %s was already met", current_struct.data());

                if (base_pos) {
                    TString base_struct = token_stack[base_pos].string;

                    // May duplicate dumped enums
                    structs_map::iterator base = structs.find(token_stack[base_pos].string);

                    if (base == structs.end())
                        errexit("for struct %s, base struct %s was not found!\n", current_struct.data(), base_struct.data());

                    // Copy members of base struct
                    structs[current_struct].insert(structs[current_struct].end(), base->second.begin(), base->second.end());
                    structs[current_struct].wanted_enums.insert(base->second.wanted_enums.begin(), base->second.wanted_enums.end());
                }
                emit_message("// members of struct %s...", current_struct.data());
                token_stack.clear();
                return;
            }
            current_enum = current_struct = "";
        }
    }
}

%%{
    machine Scanner;
    write data nofinal;

    # Floating literals.
    fract_const = digit* '.' digit+ | digit+ '.';
    exponent = [eE] [+\-]? digit+;
    float_suffix = [flFL];
    identifier = [a-zA-Z_] [a-zA-Z0-9_]*;
    chrtype = "const" space+ "char" space* "*";

    c_comment :=
        ('\n' %{line++;} | any)* :>> '*/' @{ fgoto main; };

    main := |*

    # high priority single-char tokens
    '{' {
        process_token(TT_OPENCURLY);
        curly_level++;
    };
    '}' {
        curly_level--;
        process_token(TT_CLOSECURLY);
        if (curly_level < wanted_curly_level) {
            state = DEFAULT;
            wanted_curly_level = wanted_curly_level2 = -1;
        }
        if (curly_level < wanted_curly_level2) {
            wanted_curly_level2 = curly_level;
        }
    };
    ',' { process_token(TT_COMMA); };
    '=' { process_token(TT_EQUALS); };
    ';' { process_token(TT_SEMICOLON); };
    ':' { process_token(TT_COLON); };
    '[' { process_token(TT_OPENSQUARE); };
    ']' { process_token(TT_CLOSESQUARE); };
    '(' { process_token(TT_OPENPAREN); };
    ')' { process_token(TT_CLOSEPAREN); };
    '|' { process_token(TT_PIPE); };

    # Single and double literals.
    'L'? "'" ( [^'\\\n] | /\\./ )* "'" {
        process_token(TT_OTHER);
    };
    'L'? '"' ( [^"\\\n] | /\\./ )* '"' {
        process_token(TT_OTHER);
    };

    # Identifiers & reserved words
    "enum" { process_token(TT_ENUM); };
    "struct" { process_token(TT_STRUCT); };
    "union" { process_token(TT_UNION); };
    "const" { process_token(TT_CONST); };
    "public" { process_token(TT_PUBLIC); };
    chrtype { process_token(TT_COMPLEXTYPE); };
    identifier { process_token(TT_IDENTIFIER); };

    # Floating literals.
    fract_const exponent? float_suffix? | digit+ exponent float_suffix? {
        process_token(TT_OTHER);
    };

    # Integer decimal. Leading part buffered by float.
    ( '0' | [1-9] [0-9]* ) [ulUL]{0,3} {
        process_token(TT_DECIMAL);
    };

    # Integer octal. Leading part buffered by float.
    '0' [0-9]+ [ulUL]{0,2} {
        process_token(TT_OTHER);
    };

    # Integer hex. Leading 0 buffered by float.
    '0' ( 'x' [0-9a-fA-F]+ [ulUL]{0,2} ) {
        process_token(TT_OTHER);
    };

    # Only buffer the second item, first buffered by symbol. */
    '::' { process_token(TT_OTHER); };
    '==' { process_token(TT_OTHER); };
    '!=' { process_token(TT_OTHER); };
    '&&' { process_token(TT_OTHER); };
    '||' { process_token(TT_OTHER); };
    '*=' { process_token(TT_OTHER); };
    '/=' { process_token(TT_OTHER); };
    '%=' { process_token(TT_OTHER); };
    '+=' { process_token(TT_OTHER); };
    '-=' { process_token(TT_OTHER); };
    '&=' { process_token(TT_OTHER); };
    '^=' { process_token(TT_OTHER); };
    '|=' { process_token(TT_OTHER); };
    '++' { process_token(TT_OTHER); };
    '--' { process_token(TT_OTHER); };
    '->' { process_token(TT_OTHER); };
    '->*' { process_token(TT_OTHER); };
    '.*' { process_token(TT_OTHER); };

    # Three char compounds, first item already buffered. */
    '...' { process_token(TT_OTHER); };

    # Single char symbols.
    ( punct - [_"'] ) { process_token(TT_OTHER); };

    # Comments with special meaning
    '//' [^\n]* '@nodmp' [^\n]* '\n' {
        if (line != last_entry_line)
            errexit("cannot find which entry @nodmp belongs to (last entry was at line %d)", last_entry_line);

        structs[current_struct].pop_back();
        line++;
    };

    '//' space*
        ( '@dmp'
            >{
                dmp_name_start = dmp_from_start = dmp_to_start =
                dmp_name_end = dmp_from_end = dmp_to_end =
                dmp_mode_start = 0;
            }
        )
        space* ( ( 'bitmask' | 'val' ) > { dmp_mode_start = p; } ) space*
        ( identifier
            >{ dmp_name_start = p; }
            %{ dmp_name_end = p-1; }
        )
        space* 'eff_name' space* 's/^'
        ( [a-zA-Z0-9_]*
            >{ dmp_from_start = p; }
            %{ dmp_from_end = p-1; }
        )
        '/'
        ( [a-zA-Z0-9_]*
            >{ dmp_to_start = p; }
            %{ dmp_to_end = p-1; }
        )
        '/' [^\n]* '\n'
    {
        if (line != last_entry_line)
            errexit("cannot find which entry @dmp belongs to (last entry was at line %d)", last_entry_line);

        // @dmp processor
        TString enum_name = "";
        TString prefix_del = "";
        TString prefix_add = "";

        // Put enum name for current item to TString
        if (dmp_name_start && dmp_name_end && dmp_name_start < dmp_name_end)
            enum_name = TString(dmp_name_start, dmp_name_end-dmp_name_start+1);

        // Put prefix for current item to TString
        if (dmp_from_start && dmp_from_end && dmp_from_start < dmp_from_end)
            prefix_del = TString(dmp_from_start, dmp_from_end-dmp_from_start+1);

        // Put replace prefix for current item to TString
        if (dmp_to_start && dmp_to_end && dmp_to_start < dmp_to_end)
            prefix_add = TString(dmp_to_start, dmp_to_end-dmp_to_start+1);

        // Find corresponding enum
        enums_map::iterator en = enums.find(enum_name);
        if (en == enums.end())
            errexit("bad enum name: %s", enum_name.c_str());

        if (prefix_del == "" && prefix_add == "")
            errexit("enum name should be changed, /^// does nothing");

        if (!dmp_mode_start || (*dmp_mode_start != 'b' && *dmp_mode_start != 'v'))
            errexit("unknown enum alias mode");

        emit_message("// enum-aliases for last field: enum=%s, prefix=%s, replace=%s",
            enum_name.c_str(), prefix_del.c_str(), prefix_add.c_str());

        // Make sure we have enum constants dumped
        structs[current_struct].wanted_enums.insert(enum_name);

        // Emit a callback for all enumerators
        for (enumerators_list::iterator i = en->second.begin(); i != en->second.end(); ++i) {
            TString enumerator_name = *i;


            if (!enumerator_name.StartsWith(prefix_del))
                errexit("bad prefix %s for enumerator %s", prefix_del.c_str(), enumerator_name.c_str());

            enumerator_name.replace(0, prefix_del.length(), prefix_add);

            emit_entry(enumerator_name, true, "dump_item(%s, %s, %s)", last_expression.data(), i->data(), *dmp_mode_start == 'b' ? "true" : "false");
        }
        emit_message("// end enum-aliases");

        line++;
    };

    # Comments and whitespace.
    '/*' { fgoto c_comment; };
    '//' [^\n]* '\n' { line++; };

    '\n' { line++; };
    ( any - 33..126 );

    *|;
}%%

int main(int argc, char **argv) {
    TVector<TString> wanted_structures;
    TVector<TString> wanted_enums_v;
    TVector<TString> tmp;

    Opt2 opt(argc, argv, "s:e:ESR:");
    for (auto s : opt.MArg('s', "<name1,name2,...> - structs to process")) {
        Split(s, ",", tmp);
        wanted_structures.insert(wanted_structures.end(), tmp.begin(), tmp.end());
    }
    for (auto s : opt.MArg('e', "<name1,name2,...> - enums to process")) {
        Split(s, ",", tmp);
        wanted_enums_v.insert(wanted_enums_v.end(), tmp.begin(), tmp.end());
    }
    TString srcRoot = opt.Arg('R', "<dir> - project source root (to make includes relative to)", 0);
    bool dump_all_enums = opt.Has('E', " - dump all enums (don't need -e)");
    bool add_stream_out = opt.Has('S', " - generate Out<>() for IOutputStream output (slow)");
    opt.AutoUsageErr("include.h ...");
    if (wanted_structures.empty() && wanted_enums_v.empty() && !dump_all_enums)
        warnx("nothing to dump");

    if (srcRoot && srcRoot.back() != '/')
        srcRoot += '/';
    // Header
    printf("// THIS IS GENERATED FILE, DO NOT EDIT!\n");
    printf("// Generated by struct2fieldcalc with command line:");
    for (int i = 0; i < argc; i++) {
        TStringBuf arg = argv[i];
        if (srcRoot && arg.StartsWith(srcRoot)) {
            arg.Skip(srcRoot.size());
            printf(" ${ARCADIA_ROOT}/%.*s", (int)arg.size(), arg.data());
        } else
            printf(" %.*s", (int)arg.size(), arg.data());
    }
    printf("\n\n");
    printf("#include <library/cpp/fieldcalc/field_calc_int.h>\n\n");

    for (size_t arg = 0; arg < opt.Pos.size(); arg++) {
        current_file = opt.Pos[arg];
        line = 0;
        last_entry_line = 0;

        %% write init;

        // Open input file
        TFILEPtr f(opt.Pos[arg], "rb");
        TVector<char> buf(f.length());
        if (f.read(buf.begin(), 1, buf.size()) != buf.size())
            errexit("short read in input file");
        f.close();

        char *p = buf.begin();
        char *pe = buf.end();

        TStringBuf arc_path = opt.Pos[arg];
        if (srcRoot && arc_path.StartsWith(srcRoot))
            arc_path.Skip(srcRoot.size());

        printf("#include <%.*s>\n", (int)arc_path.size(), arc_path.data());

        line = 1;

        // Parse file, emits functions
        %% write exec;

        // Done
        if (cs == Scanner_error)
            errexit("parse error");
    }
    printf("\n");

    if (wanted_structures.empty())
        for (structs_map::iterator s = structs.begin(); s != structs.end(); ++s)
            wanted_structures.push_back(s->first);

    for (TVector<TString>::iterator ws = wanted_structures.begin(); ws != wanted_structures.end(); ++ws) {
        structs_map::iterator s = structs.find(*ws);
        if (s == structs.end())
            errx(1, "Structure %s not found in input files", ws->data());

        if (!s->second.efunctions.empty()) {
            printf("namespace {\n");
            for (auto f : s->second.efunctions)
                printf("    %s\n", f.data());
            printf("}\n");
        }

        printf("template <> std::pair<const named_dump_item*, size_t> get_named_dump_items<%s>() {\n", s->first.c_str());
        printf("    static named_dump_item items[] = {\n");

        std::set<TString> avail_names;

        // dump structure fields
        for (dump_items_list::iterator i = s->second.begin(); i != s->second.end(); ++i) {
            if (!i->first)
                printf("        %s\n", i->second.data());
            else {
                if (avail_names.find(i->first) != avail_names.end()) {
                    warnx("Identifier %s is already used (while processing struct %s) - NOT DUMPING!", i->first.data(), ws->data());
                    continue;
                }
                avail_names.insert(i->first);
                printf("        { \"%s\", %s },\n", i->first.data(), i->second.data());
            }
        }

        std::set<TString> &wanted_enums = s->second.wanted_enums;
        if (dump_all_enums)
            for (enums_map::iterator en = enums.begin(); en != enums.end(); ++en)
                wanted_enums.insert(en->first);
        else
            wanted_enums.insert(wanted_enums_v.begin(), wanted_enums_v.end());

        // dump enum values for the structure
        for (std::set<TString>::iterator wen = wanted_enums.begin(); wen != wanted_enums.end(); ++wen) {
            enums_map::iterator en = enums.find(*wen);
            if (en == enums.end())
                errx(1, "Unexpected: enum %s not found (while processing struct %s)", wen->data(), ws->data());

            printf("        // members of enum %s\n", en->first.c_str());
            for (enumerators_list::iterator e = en->second.begin(); e != en->second.end(); ++e) {
                if (avail_names.find(*e) != avail_names.end())
                    errx(1, "Identifier %s is already used (while processing enum %s for struct %s)", e->data(), wen->data(), ws->data());
                printf("        { \"%s\", (long)%s },\n", e->data(), e->data());
            }
        }
        printf("    };\n\n");
        printf("    return std::make_pair(items, sizeof(items)/sizeof(*items));\n");
        printf("}\n\n");
        if (add_stream_out) {
            printf("template<>"
                   "void Out<%s>(IOutputStream& os, TTypeTraits<%s>::TFuncParam s) {\n"
                   "    TFieldCalculator<%s>().DumpAll(os, s, \" \");\n"
                   "}\n\n", s->first.data(), s->first.data(), s->first.data());
        }
    }

    return 0;
}
