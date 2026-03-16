/***********************************************************************

          OPTARGPM - a header-only library for decoding
                PROJ.4 style command line options

                   Thomas Knudsen, 2017-09-10

************************************************************************

For PROJ.4 command line programs, we have a somewhat complex option
decoding situation, since we have to navigate in a cocktail of classic
single letter style options, prefixed by "-", GNU style long options
prefixed by "--", transformation specification elements prefixed by "+",
and input file names prefixed by "" (i.e. nothing).

Hence, classic getopt.h style decoding does not cut the mustard, so
this is an attempt to catch up and chop the ketchup.

Since optargpm (for "optarg plus minus") does not belong, in any
obvious way, in any systems development library, it is provided as
a "header only" library.

While this is conventional in C++, it is frowned at in plain C.
But frown away - "header only" has its places, and this is one of
them.

By convention, we expect a command line to consist of the following
elements:

        <operator/program name>
        [short ("-")/long ("--") options}
        [operator ("+") specs]
        [operands/input files]

or less verbose:

        <operator>   [options]   [operator specs]   [operands]

or less abstract:

   proj  -I --output=foo  +proj=utm +zone=32 +ellps=GRS80   bar baz...

Where

Operator is              proj
Options are             -I --output=foo
Operator specs are      +proj=utm +zone=32 +ellps=GRS80
Operands are             bar baz


While neither claiming to save the world, nor to hint at the "shape of
jazz to come", at least optargpm has shown useful in constructing cs2cs
style transformation filters.

Supporting a wide range of option syntax, the getoptpm API is somewhat
quirky, but also compact, consisting of one data type, 3(+2) functions,
and one enumeration:

OPTARGS
        Housekeeping data type. An instance of OPTARGS is conventionally
        called o or opt
opt_parse (opt, argc, argv ...):
        The work horse: Define supported options; Split (argc, argv)
        into groups (options, op specs, operands); Parse option
        arguments.
opt_given (o, option):
        The number of times <option> was given on the command line.
        (i.e. 0 if not given or option unsupported)
opt_arg (o, option):
        A char pointer to the argument for <option>

An additional function "opt_input_loop" implements
a "read all operands sequentially" functionality, eliminating the need to
handle open/close of a sequence of input files:

enum OPTARGS_FILE_MODE:
        indicates whether to read operands in text (0) or binary (1) mode
opt_input_loop (o, mode, &gotError):
        When used as condition in a while loop, traverses all operands,
        giving the impression of reading just a single input file.

Usage is probably easiest understood by a brief textbook style example:

Consider a simple program taking the conventional "-v, -h, -o" options
indicating "verbose output", "help please", and "output file specification",
respectively.

The "-v" and "-h" options are *flags*, taking no arguments, while the
"-o" option is a *key*, taking a *value* argument, representing the
output file name.

The short options have long aliases: "--verbose", "--help" and "--output".
Additionally, the long key "--hello", without any short counterpart, is
supported.

-------------------------------------------------------------------------------


int main(int argc, char **argv) {
    PJ *P;
    OPTARGS *o;
    FILE *out = stdout;
    char *longflags[]  = {"v=verbose", "h=help", 0};
    char *longkeys[]   = {"o=output", "hello", 0};

    o = opt_parse (argc, argv, "hv", "o", longflags, longkeys);
    if (nullptr==o)
        return nullptr;


    if (opt_given (o, "h")) {
        printf ("Usage: %s [-v|--verbose] [-h|--help] [-o|--output <filename>]
[--hello=<name>] infile...", o->progname); exit (0);
    }

    if (opt_given (o, "v"))
        puts ("Feeling chatty today?");

    if (opt_given (o, "hello")) {
        printf ("Hello, %s!\n", opt_arg(o, "hello"));
        exit (0);
    }

    if (opt_given (o, "o"))
        out = fopen (opt_arg (o, "output"), "rt"); // Note: "output" translates
to "o" internally

    // Setup transformation
    P = proj_create_argv (0, o->pargc, o->pargv);

    // Loop over all lines of all input files
    bool gotError = false;
    while (opt_input_loop (o, optargs_file_format_text, &gotError)) {
        char buf[1000];
        int ret = fgets (buf, 1000, o->input);
        if (opt_eof (o)) {
            continue;
        }
        if (nullptr==ret) {
            fprintf (stderr, "Read error in record %d\n", (int)
o->record_index); continue;
        }
        do_what_needs_to_be_done (buf);
    }

    return nullptr;
}


-------------------------------------------------------------------------------

Note how short aliases for longflags and longkeys are defined by prefixing
an "o=", "h=" or "v=", respectively. This also means that it is possible to
have more than one alias for each short option, e.g.

               longkeys = {"o=output", "o=banana", 0}

would define both "--output" and "--banana" to be aliases for "-o".

************************************************************************

Thomas Knudsen, thokn@sdfe.dk, 2016-05-25/2017-09-10

************************************************************************

* Copyright (c) 2016, 2017 Thomas Knudsen
*
* Permission is hereby granted, free of charge, to any person obtaining a
* copy of this software and associated documentation files (the "Software"),
* to deal in the Software without restriction, including without limitation
* the rights to use, copy, modify, merge, publish, distribute, sublicense,
* and/or sell copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included
* in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
* OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
* DEALINGS IN THE SOFTWARE.

***********************************************************************/
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**************************************************************************************************/
struct OPTARGS;
typedef struct OPTARGS OPTARGS;
enum OPTARGS_FILE_FORMAT {
    optargs_file_format_text = 0,
    optargs_file_format_binary = 1
};

char *opt_filename(OPTARGS *opt);
static int opt_eof(OPTARGS *opt);
int opt_record(OPTARGS *opt);
int opt_input_loop(OPTARGS *opt, int binary, bool *gotError);
static int opt_is_flag(OPTARGS *opt, int ordinal);
static int opt_raise_flag(OPTARGS *opt, int ordinal);
static int opt_ordinal(OPTARGS *opt, const char *option);
int opt_given(OPTARGS *opt, const char *option);
char *opt_arg(OPTARGS *opt, const char *option);
const char *opt_strip_path(const char *full_name);
OPTARGS *opt_parse(int argc, char **argv, const char *flags, const char *keys,
                   const char **longflags, const char **longkeys);

/**************************************************************************************************/

#define OPTARG_SIZE 256

struct OPTARGS {
    int argc, margc, pargc, fargc;
    char **argv, **margv, **pargv, **fargv;
    FILE *input;
    int input_index;
    int record_index;
    const char *progname; /* argv[0], stripped from /path/to, if present */
    char flaglevel[21];   /* if flag -f is specified n times, its optarg pointer
                             is   set to flaglevel + n */
    char *optarg[OPTARG_SIZE]; /* optarg[(unsigned char) 'f'] holds a pointer to
                                  the argument of option "-f" */
    char *flags; /* a list of flag style options supported, e.g. "hv" (help and
                    verbose) */
    char *keys;  /* a list of key/value style options supported, e.g. "o"
                    (output) */
    const char *
        *longflags; /* long flags, {"help", "verbose"}, or {"h=help",
                       "v=verbose"}, to indicate homologous short options */
    const char *
        *longkeys; /* e.g. {"output"} or {o=output"} to support
                      --output=/path/to/output-file. In the latter case, */
    /* all operations on "--output" gets redirected to "-o", so user code need
     * handle arguments to "-o" only */
};

/* name of file currently read from */
char *opt_filename(OPTARGS *opt) {
    if (nullptr == opt)
        return nullptr;
    if (0 == opt->fargc)
        return opt->flaglevel;
    return opt->fargv[opt->input_index];
}

static int opt_eof(OPTARGS *opt) {
    if (nullptr == opt)
        return 1;
    return feof(opt->input);
}

/* record number of most recently read record */
int opt_record(OPTARGS *opt) {
    if (nullptr == opt)
        return 0;
    return opt->record_index + 1;
}

/* handle closing/opening of a "stream-of-streams" */
int opt_input_loop(OPTARGS *opt, int binary, bool *gotError) {
    if (gotError)
        *gotError = false;
    if (nullptr == opt)
        return 0;

    /* most common case: increment record index and read on */
    if ((opt->input != nullptr) && !feof(opt->input)) {
        opt->record_index++;
        return 1;
    }

    opt->record_index = 0;

    /* no input files specified - read from stdin */
    if ((0 == opt->fargc) && (nullptr == opt->input)) {
        opt->input = stdin;
        return 1;
    }

    /* if we're here, we have either reached eof on current input file.   */
    /*  or not yet opened a file. If eof on stdin, we're done             */
    if (opt->input == stdin)
        return 0;

    /* end if no more input */
    if (nullptr != opt->input)
        fclose(opt->input);
    if (opt->input_index >= opt->fargc)
        return 0;

    /* otherwise, open next input file */
    const char *filename = opt->fargv[opt->input_index++];
    opt->input = fopen(filename, binary ? "rb" : "rt");
    if (nullptr == opt->input) {
        fprintf(stderr, "Cannot open file %s\n", filename);
        if (gotError)
            *gotError = true;
        return 0;
    }

    return 1;
}

/* return true if option with given ordinal is a flag, false if undefined or
 * key=value */
static int opt_is_flag(OPTARGS *opt, int ordinal) {
    assert(ordinal >= 0 && ordinal < OPTARG_SIZE);
    if (opt->optarg[ordinal] < opt->flaglevel)
        return 0;
    if (opt->optarg[ordinal] > opt->flaglevel + 20)
        return 0;
    return 1;
}

static int opt_raise_flag(OPTARGS *opt, int ordinal) {
    assert(ordinal >= 0 && ordinal < OPTARG_SIZE);
    if (opt->optarg[ordinal] < opt->flaglevel)
        return 1;
    if (opt->optarg[ordinal] > opt->flaglevel + 20)
        return 1;

    /* Max out at 20 */
    if (opt->optarg[ordinal] == opt->flaglevel + 20)
        return 0;
    opt->optarg[ordinal]++;
    return 0;
}

/* Find the ordinal value of any (short or long) option */
static int opt_ordinal(OPTARGS *opt, const char *option) {
    int i;
    if (nullptr == opt)
        return 0;
    if (nullptr == option)
        return 0;
    if (0 == option[0])
        return 0;
    /* An ordinary -o style short option */
    if (strlen(option) == 1) {
        /* Undefined option? */
        if (nullptr == opt->optarg[(unsigned char)option[0]])
            return 0;
        return (unsigned char)option[0];
    }

    /* --longname style long options are slightly harder */
    for (i = 0; i < 64; i++) {
        const char **f = opt->longflags;
        if (nullptr == f)
            break;
        if (nullptr == f[i])
            break;
        if (0 == strcmp(f[i], "END"))
            break;
        if (0 == strcmp(f[i], option))
            return 128 + i;

        /* long alias? - return ordinal for corresponding short */
        if ((strlen(f[i]) > 2) && (f[i][1] == '=') &&
            (0 == strcmp(f[i] + 2, option))) {
            /* Undefined option? */
            if (nullptr == opt->optarg[(unsigned char)f[i][0]])
                return 0;
            return (unsigned char)f[i][0];
        }
    }

    for (i = 0; i < 64; i++) {
        const char **v = opt->longkeys;
        if (nullptr == v)
            return 0;
        if (nullptr == v[i])
            return 0;
        if (0 == strcmp(v[i], "END"))
            return 0;
        if (0 == strcmp(v[i], option))
            return 192 + i;

        /* long alias? - return ordinal for corresponding short */
        if ((strlen(v[i]) > 2) && (v[i][1] == '=') &&
            (0 == strcmp(v[i] + 2, option))) {
            /* Undefined option? */
            if (nullptr == opt->optarg[(unsigned char)v[i][0]])
                return 0;
            return (unsigned char)v[i][0];
        }
    }
    /* kill some potential compiler warnings about unused functions */
    (void)opt_eof(nullptr);
    return 0;
}

/* Returns 0 if option was not given on command line, non-0 otherwise */
int opt_given(OPTARGS *opt, const char *option) {
    int ordinal = opt_ordinal(opt, option);
    if (0 == ordinal)
        return 0;
    /* For flags we return the number of times the flag was specified (mostly
     * for repeated -v(erbose) flags) */
    if (opt_is_flag(opt, ordinal))
        return (int)(opt->optarg[ordinal] - opt->flaglevel);
    return opt->argv[0] != opt->optarg[ordinal];
}

/* Returns the argument to a given option */
char *opt_arg(OPTARGS *opt, const char *option) {
    int ordinal = opt_ordinal(opt, option);
    if (0 == ordinal)
        return nullptr;
    return opt->optarg[ordinal];
}

const char *opt_strip_path(const char *full_name) {
    const char *last_path_delim, *stripped_name = full_name;

    last_path_delim = strrchr(stripped_name, '\\');
    if (last_path_delim > stripped_name)
        stripped_name = last_path_delim + 1;

    last_path_delim = strrchr(stripped_name, '/');
    if (last_path_delim > stripped_name)
        stripped_name = last_path_delim + 1;
    return stripped_name;
}

/* split command line options into options/flags ("-" style), projdefs ("+"
 * style) and input file args */
OPTARGS *opt_parse(int argc, char **argv, const char *flags, const char *keys,
                   const char **longflags, const char **longkeys) {
    int i, j;
    int free_format;
    OPTARGS *o;

    if (argc == 0)
        return nullptr;

    o = (OPTARGS *)calloc(1, sizeof(OPTARGS));
    if (nullptr == o)
        return nullptr;

    o->argc = argc;
    o->argv = argv;
    o->progname = opt_strip_path(argv[0]);

    /* Reset all flags */
    for (i = 0; i < (int)strlen(flags); i++)
        o->optarg[(unsigned char)flags[i]] = o->flaglevel;

    /* Flag args for all argument taking options as "unset" */
    for (i = 0; i < (int)strlen(keys); i++)
        o->optarg[(unsigned char)keys[i]] = argv[0];

    /* Hence, undefined/illegal options have an argument of 0 */

    /* long opts are handled similarly, but are mapped to the high bit character
     * range (above 128) */
    o->longflags = longflags;
    o->longkeys = longkeys;

    /* check aliases, An end user should never experience this, but */
    /* the developer should make sure that aliases are valid */
    for (i = 0; longflags && longflags[i]; i++) {
        /* Go on if it does not look like an alias */
        if (strlen(longflags[i]) < 3)
            continue;
        if ('=' != longflags[i][1])
            continue;
        if (nullptr == strchr(flags, longflags[i][0])) {
            fprintf(stderr,
                    "%s: Invalid alias - '%s'. Valid short flags are '%s'\n",
                    o->progname, longflags[i], flags);
            free(o);
            return nullptr;
        }
    }
    for (i = 0; longkeys && longkeys[i]; i++) {
        /* Go on if it does not look like an alias */
        if (strlen(longkeys[i]) < 3)
            continue;
        if ('=' != longkeys[i][1])
            continue;
        if (nullptr == strchr(keys, longkeys[i][0])) {
            fprintf(stderr,
                    "%s: Invalid alias - '%s'. Valid short flags are '%s'\n",
                    o->progname, longkeys[i], keys);
            free(o);
            return nullptr;
        }
    }

    /* aside from counting the number of times a flag has been specified, we
     * also abuse the */
    /* flaglevel array to provide a pseudo-filename for the case of reading from
     * stdin      */
    strcpy(o->flaglevel, "<stdin>");

    for (i = 128; (longflags != nullptr) && (longflags[i - 128] != nullptr);
         i++) {
        if (i == 192) {
            free(o);
            fprintf(stderr, "Too many flag style long options\n");
            return nullptr;
        }
        o->optarg[i] = o->flaglevel;
    }

    for (i = 192; (longkeys != nullptr) && (longkeys[i - 192] != nullptr);
         i++) {
        if (i == 256) {
            free(o);
            fprintf(stderr, "Too many value style long options\n");
            return nullptr;
        }
        o->optarg[i] = argv[0];
    }

    /* Now, set up the argc/argv pairs, and interpret args */
    o->argc = argc;
    o->argv = argv;

    /* Process all '-' and '--'-style options */
    for (i = 1; i < argc; i++) {
        int arg_group_size = (int)strlen(argv[i]);

        if ('-' != argv[i][0])
            break;

        if (nullptr == o->margv)
            o->margv = argv + i;
        o->margc++;

        for (j = 1; j < arg_group_size; j++) {
            int c = (unsigned char)(argv[i][j]);
            char cstring[2], *crepr = cstring;
            cstring[0] = argv[i][j];
            cstring[1] = 0;

            /* Long style flags and options (--long_opt_name, --long_opt_namr
             * arg,
             * --long_opt_name=arg) */
            if (c == (int)'-') {
                char *equals;
                crepr = argv[i] + 2;

                /* We need to manipulate a bit to support gnu style --foo=bar
                 * syntax. */
                /* NOTE: This will segfault for read-only (const char * style)
                 * storage,
                 */
                /* but since the canonical use case, int main (int argc, char
                 * **argv),
                 */
                /* is non-const, we ignore this for now */
                equals = strchr(crepr, '=');
                if (equals)
                    *equals = 0;
                c = opt_ordinal(o, crepr);
                if (0 == c) {
                    fprintf(stderr, "Invalid option \"%s\"\n", crepr);
                    free(o);
                    return nullptr;
                }

                /* inline (gnu) --foo=bar style arg */
                if (equals) {
                    *equals = '=';
                    if (opt_is_flag(o, c)) {
                        fprintf(stderr, "Option \"%s\" takes no arguments\n",
                                crepr);
                        free(o);
                        return nullptr;
                    }
                    o->optarg[c] = equals + 1;
                    break;
                }

                /* "outline" --foo bar style arg */
                if (!opt_is_flag(o, c)) {
                    if ((argc == i + 1) || ('+' == argv[i + 1][0]) ||
                        ('-' == argv[i + 1][0])) {
                        fprintf(stderr, "Missing argument for option \"%s\"\n",
                                crepr);
                        free(o);
                        return nullptr;
                    }
                    o->optarg[c] = argv[i + 1];
                    i++; /* eat the arg */
                    break;
                }

                if (!opt_is_flag(o, c)) {
                    fprintf(stderr,
                            "Expected flag style long option here, but got "
                            "\"%s\"\n",
                            crepr);
                    free(o);
                    return nullptr;
                }

                /* Flag style option, i.e. taking no arguments */
                opt_raise_flag(o, c);
                break;
            }

            /* classic short options */
            if (nullptr == o->optarg[c]) {
                fprintf(stderr, "Invalid option \"%s\"\n", crepr);
                free(o);
                return nullptr;
            }

            /* Flag style option, i.e. taking no arguments */
            if (opt_is_flag(o, c)) {
                opt_raise_flag(o, c);
                continue;
            }

            /* options taking arguments */

            /* argument separate (i.e. "-i 10") */
            if (j + 1 == arg_group_size) {
                if ((argc == i + 1) || ('+' == argv[i + 1][0]) ||
                    ('-' == argv[i + 1][0])) {
                    fprintf(stderr, "Bad or missing arg for option \"%s\"\n",
                            crepr);
                    free(o);
                    return nullptr;
                }
                o->optarg[c] = argv[i + 1];
                i++;
                break;
            }

            /* Option arg inline (i.e. "-i10") */
            o->optarg[c] = argv[i] + j + 1;
            break;
        }
    }

    /* Process all '+'-style options, starting from where '-'-style processing
     * ended */
    o->pargv = argv + i;

    /* Is free format in use, instead of plus-style? */
    free_format = 0;
    for (j = 1; j < argc; j++) {
        if (0 == strcmp("--", argv[j])) {
            free_format = j;
            break;
        }
    }

    if (free_format) {
        o->pargc = free_format - (o->margc + 1);
        o->fargc = argc - (free_format + 1);
        if (0 != o->fargc)
            o->fargv = argv + free_format + 1;
        return o;
    }

    for (/* empty */; i < argc; i++) {
        if ('-' == argv[i][0]) {
            free(o);
            fprintf(stderr,
                    "Minus options must come first, then the plus options\n");
            return nullptr;
        }

        if ('+' != argv[i][0])
            break;
        o->pargc++;
    }

    /* Handle input file names */
    o->fargc = argc - i;
    if (0 != o->fargc)
        o->fargv = argv + i;

    return o;
}
