
/* Parser-tokenizer link implementation */

#include "../Include/pgenheaders.h"
#include "tokenizer.h"
#include "../Include/node.h"
#include "../Include/grammar.h"
#include "parser.h"
#include "../Include/parsetok.h"
#include "../Include/errcode.h"
#include "../Include/graminit.h"

int Ta27_TabcheckFlag;


/* Forward */
static node *parsetok(struct tok_state *, grammar *, int, perrdetail *, int *);
static void initerr(perrdetail *err_ret, const char* filename);
static int initerr_object(perrdetail *err_ret, PyObject *filename);

/* Parse input coming from a string.  Return error code, print some errors. */
node *
Ta27Parser_ParseString(const char *s, grammar *g, int start, perrdetail *err_ret)
{
    return Ta27Parser_ParseStringFlagsFilename(s, NULL, g, start, err_ret, 0);
}

node *
Ta27Parser_ParseStringFlags(const char *s, grammar *g, int start,
                          perrdetail *err_ret, int flags)
{
    return Ta27Parser_ParseStringFlagsFilename(s, NULL,
                                             g, start, err_ret, flags);
}

node *
Ta27Parser_ParseStringFlagsFilename(const char *s, const char *filename,
                          grammar *g, int start,
                          perrdetail *err_ret, int flags)
{
    int iflags = flags;
    return Ta27Parser_ParseStringFlagsFilenameEx(s, filename, g, start,
                                               err_ret, &iflags);
}

node *
Ta27Parser_ParseStringFlagsFilenameEx(const char *s, const char *filename,
                          grammar *g, int start,
                          perrdetail *err_ret, int *flags)
{
    struct tok_state *tok;

    initerr(err_ret, filename);

    if ((tok = Ta27Tokenizer_FromString(s, start == file_input)) == NULL) {
        err_ret->error = PyErr_Occurred() ? E_DECODE : E_NOMEM;
        return NULL;
    }

    tok->filename = filename ? filename : "<string>";
    if (Ta27_TabcheckFlag || Py_VerboseFlag) {
        tok->altwarning = (tok->filename != NULL);
        if (Ta27_TabcheckFlag >= 2)
            tok->alterror++;
    }

    return parsetok(tok, g, start, err_ret, flags);
}

node *
Ta27Parser_ParseStringObject(const char *s, PyObject *filename,
                           grammar *g, int start,
                           perrdetail *err_ret, int *flags)
{
    struct tok_state *tok;
    int exec_input = start == file_input;

    initerr_object(err_ret, filename);

    if (*flags & PyPARSE_IGNORE_COOKIE)
        tok = Ta27Tokenizer_FromUTF8(s, exec_input);
    else
        tok = Ta27Tokenizer_FromString(s, exec_input);

    if (tok == NULL) {
        err_ret->error = PyErr_Occurred() ? E_DECODE : E_NOMEM;
        return NULL;
    }

#ifndef PGEN
    Py_INCREF(err_ret->filename);
    tok->filename = PyUnicode_AsUTF8(err_ret->filename);
#endif
    return parsetok(tok, g, start, err_ret, flags);
}

/* Parse input coming from a file.  Return error code, print some errors. */

node *
Ta27Parser_ParseFile(FILE *fp, const char *filename, grammar *g, int start,
                   char *ps1, char *ps2, perrdetail *err_ret)
{
    return Ta27Parser_ParseFileFlags(fp, filename, g, start, ps1, ps2,
                                   err_ret, 0);
}

node *
Ta27Parser_ParseFileFlags(FILE *fp, const char *filename, grammar *g, int start,
                        char *ps1, char *ps2, perrdetail *err_ret, int flags)
{
    int iflags = flags;
    return Ta27Parser_ParseFileFlagsEx(fp, filename, g, start, ps1, ps2, err_ret, &iflags);
}

node *
Ta27Parser_ParseFileFlagsEx(FILE *fp, const char *filename, grammar *g, int start,
                          char *ps1, char *ps2, perrdetail *err_ret, int *flags)
{
    struct tok_state *tok;

    initerr(err_ret, filename);

    if ((tok = Ta27Tokenizer_FromFile(fp, ps1, ps2)) == NULL) {
        err_ret->error = E_NOMEM;
        return NULL;
    }
    tok->filename = filename;
    if (Ta27_TabcheckFlag || Py_VerboseFlag) {
        tok->altwarning = (filename != NULL);
        if (Ta27_TabcheckFlag >= 2)
            tok->alterror++;
    }

    return parsetok(tok, g, start, err_ret, flags);
}

#if 0
static char with_msg[] =
"%s:%d: Warning: 'with' will become a reserved keyword in Python 2.6\n";

static char as_msg[] =
"%s:%d: Warning: 'as' will become a reserved keyword in Python 2.6\n";

static void
warn(const char *msg, const char *filename, int lineno)
{
    if (filename == NULL)
        filename = "<string>";
    PySys_WriteStderr(msg, filename, lineno);
}
#endif


typedef struct {
    struct {
        int lineno;
        char *comment;
    } *items;
    size_t size;
    size_t num_items;
} growable_comment_array;

static int
growable_comment_array_init(growable_comment_array *arr, size_t initial_size) {
    assert(initial_size > 0);
    arr->items = malloc(initial_size * sizeof(*arr->items));
    arr->size = initial_size;
    arr->num_items = 0;

    return arr->items != NULL;
}

static int
growable_comment_array_add(growable_comment_array *arr, int lineno, char *comment) {
    if (arr->num_items >= arr->size) {
        arr->size *= 2;
        arr->items = realloc(arr->items, arr->size * sizeof(*arr->items));
        if (!arr->items) {
            return 0;
        }
    }

    arr->items[arr->num_items].lineno = lineno;
    arr->items[arr->num_items].comment = comment;
    arr->num_items++;
    return 1;
}

static void
growable_comment_array_deallocate(growable_comment_array *arr) {
    unsigned i;
    for (i = 0; i < arr->num_items; i++) {
        PyObject_FREE(arr->items[i].comment);
    }
    free(arr->items);
}


/* Parse input coming from the given tokenizer structure.
   Return error code. */

static node *
parsetok(struct tok_state *tok, grammar *g, int start, perrdetail *err_ret,
         int *flags)
{
    parser_state *ps;
    node *n;
    int started = 0;

    growable_comment_array type_ignores;
    if (!growable_comment_array_init(&type_ignores, 10)) {
        err_ret->error = E_NOMEM;
        Ta27Tokenizer_Free(tok);
        return NULL;
    }

    if ((ps = Ta27Parser_New(g, start)) == NULL) {
        fprintf(stderr, "no mem for new parser\n");
        err_ret->error = E_NOMEM;
        Ta27Tokenizer_Free(tok);
        return NULL;
    }
#ifdef PY_PARSER_REQUIRES_FUTURE_KEYWORD
    if (*flags & PyPARSE_PRINT_IS_FUNCTION) {
        ps->p_flags |= CO_FUTURE_PRINT_FUNCTION;
    }
    if (*flags & PyPARSE_UNICODE_LITERALS) {
        ps->p_flags |= CO_FUTURE_UNICODE_LITERALS;
    }

#endif

    for (;;) {
        char *a, *b;
        int type;
        size_t len;
        char *str;
        int col_offset;

        type = Ta27Tokenizer_Get(tok, &a, &b);
        if (type == ERRORTOKEN) {
            err_ret->error = tok->done;
            break;
        }
        if (type == ENDMARKER && started) {
            type = NEWLINE; /* Add an extra newline */
            started = 0;
            /* Add the right number of dedent tokens,
               except if a certain flag is given --
               codeop.py uses this. */
            if (tok->indent &&
                !(*flags & PyPARSE_DONT_IMPLY_DEDENT))
            {
                tok->pendin = -tok->indent;
                tok->indent = 0;
            }
        }
        else
            started = 1;
        len = b - a; /* XXX this may compute NULL - NULL */
        str = (char *) PyObject_MALLOC(len + 1);
        if (str == NULL) {
            fprintf(stderr, "no mem for next token\n");
            err_ret->error = E_NOMEM;
            break;
        }
        if (len > 0)
            strncpy(str, a, len);
        str[len] = '\0';

#ifdef PY_PARSER_REQUIRES_FUTURE_KEYWORD
#endif
        if (a >= tok->line_start)
            col_offset = a - tok->line_start;
        else
            col_offset = -1;

        if (type == TYPE_IGNORE) {
            if (!growable_comment_array_add(&type_ignores, tok->lineno, str)) {
                err_ret->error = E_NOMEM;
                break;
            }
            continue;
        }

        if ((err_ret->error =
             Ta27Parser_AddToken(ps, (int)type, str, tok->lineno, col_offset,
                               &(err_ret->expected))) != E_OK) {
            if (err_ret->error != E_DONE) {
                PyObject_FREE(str);
                err_ret->token = type;
            }
            break;
        }
    }

    if (err_ret->error == E_DONE) {
        n = ps->p_tree;
        ps->p_tree = NULL;

        if (n->n_type == file_input) {
            /* Put type_ignore nodes in the ENDMARKER of file_input. */
            int num;
            node *ch;
            size_t i;

            num = NCH(n);
            ch = CHILD(n, num - 1);
            REQ(ch, ENDMARKER);

            for (i = 0; i < type_ignores.num_items; i++) {
                int res = Ta27Node_AddChild(ch, TYPE_IGNORE, type_ignores.items[i].comment,
                                            type_ignores.items[i].lineno, 0);
                if (res != 0) {
                    err_ret->error = res;
                    Ta27Node_Free(n);
                    n = NULL;
                    break;
                }
                type_ignores.items[i].comment = NULL;
            }
        }
    }
    else
        n = NULL;

    growable_comment_array_deallocate(&type_ignores);

#ifdef PY_PARSER_REQUIRES_FUTURE_KEYWORD
    *flags = ps->p_flags;
#endif
    Ta27Parser_Delete(ps);

    if (n == NULL) {
        if (tok->lineno <= 1 && tok->done == E_EOF)
            err_ret->error = E_EOF;
        err_ret->lineno = tok->lineno;
        if (tok->buf != NULL) {
            char *text = NULL;
            size_t len;
            assert(tok->cur - tok->buf < INT_MAX);
            err_ret->offset = (int)(tok->cur - tok->buf);
            len = tok->inp - tok->buf;
#ifdef Py_USING_UNICODE
            text = Ta27Tokenizer_RestoreEncoding(tok, len, &err_ret->offset);

#endif
            if (text == NULL) {
                text = (char *) PyObject_MALLOC(len + 1);
                if (text != NULL) {
                    if (len > 0)
                        strncpy(text, tok->buf, len);
                    text[len] = '\0';
                }
            }
            err_ret->text = text;
        }
    } else if (tok->encoding != NULL) {
        /* 'nodes->n_str' uses PyObject_*, while 'tok->encoding' was
         * allocated using PyMem_
         */
        node* r = Ta27Node_New(encoding_decl);
        if (r)
            r->n_str = PyObject_MALLOC(strlen(tok->encoding)+1);
        if (!r || !r->n_str) {
            err_ret->error = E_NOMEM;
            if (r)
                PyObject_FREE(r);
            n = NULL;
            goto done;
        }
        strcpy(r->n_str, tok->encoding);
        PyMem_FREE(tok->encoding);
        tok->encoding = NULL;
        r->n_nchildren = 1;
        r->n_child = n;
        n = r;
    }

done:
    Ta27Tokenizer_Free(tok);

    return n;
}

static void
initerr(perrdetail *err_ret, const char *filename)
{
  initerr_object(err_ret, PyUnicode_FromString(filename));
}

static int
initerr_object(perrdetail *err_ret, PyObject *filename)
{
    err_ret->error = E_OK;
    err_ret->lineno = 0;
    err_ret->offset = 0;
    err_ret->text = NULL;
    err_ret->token = -1;
    err_ret->expected = -1;
#ifndef PGEN
    if (filename) {
        Py_INCREF(filename);
        err_ret->filename = filename;
    }
    else {
        err_ret->filename = PyUnicode_FromString("<string>");
        if (err_ret->filename == NULL) {
            err_ret->error = E_ERROR;
            return -1;
        }
    }
#endif
    return 0;
}
