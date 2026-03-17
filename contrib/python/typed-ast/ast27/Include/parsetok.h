
/* Parser-tokenizer link interface */

#ifndef Ta27_PARSETOK_H
#define Ta27_PARSETOK_H
#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    int error;
    PyObject *filename;
    int lineno;
    int offset;
    char *text;
    int token;
    int expected;
} perrdetail;

#if 0
#define PyPARSE_YIELD_IS_KEYWORD	0x0001
#endif

#define PyPARSE_DONT_IMPLY_DEDENT	0x0002

#if 0
#define PyPARSE_WITH_IS_KEYWORD		0x0003
#endif

#define PyPARSE_PRINT_IS_FUNCTION       0x0004
#define PyPARSE_UNICODE_LITERALS        0x0008

#define PyPARSE_IGNORE_COOKIE 0x0010


node *Ta27Parser_ParseString(const char *, grammar *, int,
                             perrdetail *);
node *Ta27Parser_ParseFile (FILE *, const char *, grammar *, int,
                            char *, char *, perrdetail *);

node *Ta27Parser_ParseStringFlags(const char *, grammar *, int,
                                  perrdetail *, int);
node *Ta27Parser_ParseFileFlags(FILE *, const char *, grammar *,
       			 int, char *, char *,
       			 perrdetail *, int);
node *Ta27Parser_ParseFileFlagsEx(FILE *, const char *, grammar *,
       			 int, char *, char *,
       			 perrdetail *, int *);

node *Ta27Parser_ParseStringFlagsFilename(const char *,
       		      const char *,
       		      grammar *, int,
                perrdetail *, int);
node *Ta27Parser_ParseStringFlagsFilenameEx(const char *,
					      const char *,
					      grammar *, int,
                                              perrdetail *, int *);

node *Ta27Parser_ParseStringObject(
    const char *s,
    PyObject *filename,
    grammar *g,
    int start,
    perrdetail *err_ret,
    int *flags);

/* Note that he following function is defined in pythonrun.c not parsetok.c. */
PyAPI_FUNC(void) PyParser_SetError(perrdetail *);

#ifdef __cplusplus
}
#endif
#endif /* !Ta27_PARSETOK_H */
