/* Copyright 2018, UCAR/Unidata.
   See the COPYRIGHT file for more information.
*/

/*
TODO: make utf8 safe
*/

/*
WARNING:
If you modify this file,
then you need to go to
the include/ directory
and do the command:
    make netcdf_json.h
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif /*HAVE_CONFIG_H*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "ncjson.h"

#undef NCJCATCH
#undef NCJDEBUG
#undef NCJTRACE

#ifdef NCJCATCH
/* Warning: do not evaluate err more than once */
#define NCJTHROW(err) ncjbreakpoint(err)
static int ncjbreakpoint(int err) {return err;}
#else /*!NCJCATCH*/
#define NCJTHROW(err) (err)
#endif /*NCJCATCH*/

/**************************************************/

#define NCJ_EOF -2

#define NCJ_LBRACKET '['
#define NCJ_RBRACKET ']'
#define NCJ_LBRACE '{'
#define NCJ_RBRACE '}'
#define NCJ_COLON ':'
#define NCJ_COMMA ','
#define NCJ_QUOTE '"'
#define NCJ_ESCAPE '\\'
#define NCJ_TAG_TRUE "true"
#define NCJ_TAG_FALSE "false"
#define NCJ_TAG_NULL "null"

/* JSON_WORD Subsumes Number also */
#define JSON_WORD "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$+-."

#define NCJ_DEFAULTALLOC 16

/**************************************************/
typedef struct NCJparser {
    char* text;
    char* pos;
    size_t yylen; /* |yytext| */
    char* yytext; /* string or word */
    long long num;
    int tf;
    int status; /* NCJ_ERR|NCJ_OK */
    unsigned flags;
#     define NCJ_TRACE 1
} NCJparser;

/* This is used only by the unparser */
typedef struct NCJbuf {
    size_t len; /* |text|; does not include nul terminator */
    char* text; /* NULL || nul terminated */
} NCJbuf;

/**************************************************/

#if defined(_WIN32) && !defined(__MINGW32__)
#define strdup _strdup
#define strcasecmp _stricmp
#else /*!WIN32 || __MINGW32*/
#include <strings.h>
#endif /*defined(_WIN32) && !defined(__MINGW32__)*/

#ifndef nullfree
#define nullfree(x) {if(x)free(x);}
#endif /*nullfree*/
#ifndef nulldup
#define nulldup(x) ((x)?strdup(x):(x))
#endif /*nulldup*/

#if defined NCJDEBUG || defined NCJTRACE
static char* tokenname(int token);
#endif /*defined NCJDEBUG || defined NCJTRACE*/

/**************************************************/
/* Forward */
static int NCJparseR(NCJparser* parser, NCjson**);
static int NCJparseArray(NCJparser* parser, struct NCjlist* array);
static int NCJparseDict(NCJparser* parser, struct NCjlist* dict);
static int testbool(const char* word);
static int testint(const char* word);
static int testdouble(const char* word);
static int testnull(const char* word);
static int NCJlex(NCJparser* parser);
static int NCJyytext(NCJparser*, char* start, size_t pdlen);
static void NCJreclaimArray(struct NCjlist*);
static void NCJreclaimDict(struct NCjlist*);
static int NCJunescape(NCJparser* parser);
static char unescape1(char c);

static int listappend(struct NCjlist* list, NCjson* element);
static int listsetalloc(struct NCjlist* list, size_t sz);
static int listlookup(const struct NCjlist* list, const char* key, size_t* indexp);

static int NCJcloneArray(const NCjson* array, NCjson** clonep);
static int NCJcloneDict(const NCjson* dict, NCjson** clonep);

/* These are used only by the unparser */
static int NCJunparseR(const NCjson* json, NCJbuf* buf, unsigned flags);
static int bytesappendquoted(NCJbuf* buf, const char* s);
static int bytesappend(NCJbuf* buf, const char* s);
static int bytesappendc(NCJbuf* bufp, char c);

/* Hide everything for plugins */
#ifdef NETCDF_JSON_H
#define OPTSTATIC static
#else /*!NETCDF_JSON_H*/
#define OPTSTATIC
#endif /*NETCDF_JSON_H*/

/* List legal nan and infinity names (lower case); keep in strcasecmp sorted order */
static const char* NANINF[] = {"-infinity","infinity","-infinityf","infinityf","nan","nanf"};
static const size_t NNANINF = 6;

/**************************************************/

OPTSTATIC int
NCJparse(const char* text, unsigned flags, NCjson** jsonp)
{
    size_t textlen = strlen(text);
    return NCJparsen(textlen,text,flags,jsonp);
}

OPTSTATIC int
NCJparsen(size_t len, const char* text, unsigned flags, NCjson** jsonp)
{
    int stat = NCJ_OK;
    NCJparser* parser = NULL;
    NCjson* json = NULL;

    parser = calloc(1,sizeof(NCJparser));
    if(parser == NULL)
	{stat = NCJTHROW(NCJ_ERR); goto done;}
    parser->flags = flags;
    parser->text = (char*)malloc(len+1+1);
    if(parser->text == NULL)
	{stat = NCJTHROW(NCJ_ERR); goto done;}
    memcpy(parser->text,text,len);
    /* trim trailing whitespace */
    if(len > 0) {
	char* p;
        for(p=parser->text+(len-1);p >= parser->text;p--) {
	   if(*p > ' ') break;
	}
	len = (size_t)((p - parser->text) + 1);
    }
    if(len == 0) 
	{stat = NCJTHROW(NCJ_ERR); goto done;}
    parser->text[len] = '\0';
    parser->text[len+1] = '\0';
    parser->pos = &parser->text[0];
    parser->status = NCJ_OK;
#ifdef NCJDEBUG
fprintf(stderr,"json: |%s|\n",parser->text);
#endif /*NCJDEBUG*/
    if((stat=NCJparseR(parser,&json))==NCJ_ERR) goto done;
    /* Must consume all of the input */
    if(parser->pos != (parser->text+len)) {stat = NCJ_ERR; goto done;}
    *jsonp = json;
    json = NULL;

done:
    if(parser != NULL) {
	nullfree(parser->text);
	nullfree(parser->yytext);
	free(parser);
    }
    (void)NCJreclaim(json);
    return NCJTHROW(stat);
}

/*
Simple recursive descent
intertwined with dict and list parsers.

Invariants:
1. The json argument is provided by caller and filled in by NCJparseR.
2. Each call pushed back last unconsumed token
*/

static int
NCJparseR(NCJparser* parser, NCjson** jsonp)
{
    int stat = NCJ_OK;
    int token = NCJ_UNDEF;
    NCjson* json = NULL;

    if(jsonp == NULL)
	{stat = NCJTHROW(NCJ_ERR); goto done;}
    if((token = NCJlex(parser)) == NCJ_UNDEF)
	{stat = NCJTHROW(NCJ_ERR); goto done;}
    switch (token) {
    case NCJ_EOF:
	break;
    case NCJ_NULL:
        if((stat = NCJnew(NCJ_NULL,&json))==NCJ_ERR) goto done;
	break;
    case NCJ_BOOLEAN:
        if((stat = NCJnew(NCJ_BOOLEAN,&json))==NCJ_ERR) goto done;
	json->string = strdup(parser->yytext);
	break;
    case NCJ_INT:
        if((stat = NCJnew(NCJ_INT,&json))==NCJ_ERR) goto done;
	json->string = strdup(parser->yytext);
	break;
    case NCJ_DOUBLE:
        if((stat = NCJnew(NCJ_DOUBLE,&json))==NCJ_ERR) goto done;
	json->string = strdup(parser->yytext);
	break;
    case NCJ_STRING:
        if((stat = NCJnew(NCJ_STRING,&json))==NCJ_ERR) goto done;
	json->string = strdup(parser->yytext);
	break;
    case NCJ_LBRACE:
        if((stat = NCJnew(NCJ_DICT,&json))==NCJ_ERR) goto done;
	if((stat = NCJparseDict(parser, &json->list))==NCJ_ERR) goto done;
	break;
    case NCJ_LBRACKET:
        if((stat = NCJnew(NCJ_ARRAY,&json))==NCJ_ERR) goto done;
	if((stat = NCJparseArray(parser, &json->list))==NCJ_ERR) goto done;
	break;
    case NCJ_RBRACE: /* We hit end of the dict we are parsing */
	parser->pos--; /* pushback so NCJparseArray will catch */
	json = NULL;
	break;
    case NCJ_RBRACKET:
	parser->pos--; /* pushback so NCJparseDict will catch */
	json = NULL;
	break;
    default:
	stat = NCJTHROW(NCJ_ERR);
	break;
    }
    if(jsonp && json) {*jsonp = json; json = NULL;}

done:
    NCJreclaim(json);
    return NCJTHROW(stat);
}

static int
NCJparseArray(NCJparser* parser, struct NCjlist* arrayp)
{
    int stat = NCJ_OK;
    int token = NCJ_UNDEF;
    NCjson* element = NULL;
    int stop = 0;

    /* [ ^e1,e2, ...en] */

    while(!stop) {
	/* Recurse to get the value ei (might be null) */
	if((stat = NCJparseR(parser,&element))==NCJ_ERR) goto done;
	token = NCJlex(parser); /* Get next token */
	/* Next token should be comma or rbracket */
	switch(token) {
	case NCJ_RBRACKET:
	    if(element != NULL) listappend(arrayp,element);
	    element = NULL;
	    stop = 1;
	    break;
	case NCJ_COMMA:
	    /* Append the ei to the list */
	    if(element == NULL) {stat = NCJTHROW(NCJ_ERR); goto done;} /* error */
	    listappend(arrayp,element);
	    element = NULL;
	    break;
	case NCJ_EOF:
	case NCJ_UNDEF:
	default:
	    stat = NCJTHROW(NCJ_ERR);
	    goto done;
	}	
    }	

done:
    if(element != NULL)
	NCJreclaim(element);
    return NCJTHROW(stat);
}

static int
NCJparseDict(NCJparser* parser, struct NCjlist* dictp)
{
    int stat = NCJ_OK;
    int token = NCJ_UNDEF;
    NCjson* value = NULL;
    NCjson* key = NULL;
    int stop = 0;

    /* { ^k1:v1,k2:v2, ...kn:vn] */

    while(!stop) {
	/* Get the key, which must be a word of some sort */
	token = NCJlex(parser);
	switch(token) {
	case NCJ_STRING:
	case NCJ_BOOLEAN:
	case NCJ_INT: case NCJ_DOUBLE: {
 	    if((stat=NCJnewstring(token,parser->yytext,&key))==NCJ_ERR) goto done;
	    } break;
	case NCJ_RBRACE: /* End of containing Dict */
	    stop = 1;
	    continue; /* leave loop */
	case NCJ_EOF: case NCJ_UNDEF:
	default:
	    stat = NCJTHROW(NCJ_ERR);
	    goto done;
	}
	/* Next token must be colon*/
   	switch((token = NCJlex(parser))) {
	case NCJ_COLON: break;
	case NCJ_UNDEF: case NCJ_EOF:
	default: stat = NCJTHROW(NCJ_ERR); goto done;
	}    
	/* Get the value */
	if((stat = NCJparseR(parser,&value))==NCJ_ERR) goto done;
        /* Next token must be comma or RBRACE */
	switch((token = NCJlex(parser))) {
	case NCJ_RBRACE:
	    stop = 1;
	    /* fall thru */
	case NCJ_COMMA:
	    /* Insert key value into dict: key first, then value */
	    listappend(dictp,key);
	    key = NULL;
	    listappend(dictp,value);
	    value = NULL;
	    break;
	case NCJ_EOF:
	case NCJ_UNDEF:
	default:
	    stat = NCJTHROW(NCJ_ERR);
	    goto done;
	}	
    }	

done:
    if(key != NULL)
	NCJreclaim(key);
    if(value != NULL)
	NCJreclaim(value);
    return NCJTHROW(stat);
}

static int
NCJlex(NCJparser* parser)
{
    int token = NCJ_UNDEF;
    char* start;
    size_t count;

    while(token == 0) { /* avoid need to goto when retrying */
	char c = *parser->pos;
	if(c == '\0') {
	    token = NCJ_EOF;
	} else if(c <= ' ' || c == '\177') {/* ignore whitespace */
	    parser->pos++;
	    continue;
	} else if(c == NCJ_ESCAPE) {
	    parser->pos++;
	    c = *parser->pos;
	    *parser->pos = (char)unescape1(c);
	    continue;
	} else if(strchr(JSON_WORD, c) != NULL) {
	    start = parser->pos;
	    for(;;) {
		c = *parser->pos++;
		if(c == '\0' || strchr(JSON_WORD,c) == NULL) break; /* end of word */
	    }
	    /* Pushback c */
	    parser->pos--;
	    count = (size_t)((parser->pos) - start);
	    if(NCJyytext(parser,start,count)) goto done;
	    /* Discriminate the word string to get the proper sort */
	    if(testbool(parser->yytext))
		token = NCJ_BOOLEAN;
	    /* do int test first since double subsumes int */
	    else if(testint(parser->yytext))
		token = NCJ_INT;
	    else if(testdouble(parser->yytext))
		token = NCJ_DOUBLE;
	    else if(testnull(parser->yytext))
		token = NCJ_NULL;
	    else
		token = NCJ_STRING;
	} else if(c == NCJ_QUOTE) {
	    parser->pos++;
	    start = parser->pos;
	    for(;;) {
		c = *parser->pos++;
		if(c == NCJ_ESCAPE) parser->pos++;
		else if(c == NCJ_QUOTE || c == '\0') break;
	    }
	    if(c == '\0') {
		parser->status = NCJ_ERR;
		token = NCJ_UNDEF;
		goto done;
	    }
	    count = (size_t)((parser->pos) - start) - 1; /* -1 for trailing quote */
	    if(NCJyytext(parser,start,count)==NCJ_ERR) goto done;
	    if(NCJunescape(parser)==NCJ_ERR) goto done;
	    token = NCJ_STRING;
	} else { /* single char token */
	    if(NCJyytext(parser,parser->pos,1)==NCJ_ERR) goto done;
	    token = *parser->pos++;
	}
#ifdef NCJDEBUG
fprintf(stderr,"%s(%d): |%s|\n",tokenname(token),token,parser->yytext);
#endif /*NCJDEBUG*/
    } /*for(;;)*/
done:
    if(parser->status == NCJ_ERR)
        token = NCJ_UNDEF;
#ifdef NCJTRACE
    if(parser->flags & NCJ_TRACE) {
	const char* txt = NULL;
	switch(token) {
	case NCJ_STRING: case NCJ_INT: case NCJ_DOUBLE: case NCJ_BOOLEAN: txt = parser->yytext; break;
        default: break;
	}
        fprintf(stderr,">>>> token=%s:'%s'\n",tokenname(token),(txt?txt:""));
    }
#endif /*NCJTRACE*/
    return token;
}

static int
testnull(const char* word)
{
    if(strcasecmp(word,NCJ_TAG_NULL)==0) return 1;
    return 0;
}

static int
testbool(const char* word)
{
    if(strcasecmp(word,NCJ_TAG_TRUE)==0
       || strcasecmp(word,NCJ_TAG_FALSE)==0)
	return 1;
    return 0;
}

static int
testint(const char* word)
{
    int ncvt;
    long long i;
    int count = 0;
    /* Try to convert to number */
    ncvt = sscanf(word,"%lld%n",&i,&count);
    return (ncvt == 1 && strlen(word)==((size_t)count) ? 1 : 0);
}

static int
nancmp(const void* keyp, const void* membpp)
{
    int cmp;    
    const char* key = (const char*)keyp;
    const char** membp = (const char**)membpp;
    cmp = strcasecmp(key,*membp);
    return cmp;
}

static int
testdouble(const char* word)
{
    int ncvt;
    double d;
    int count = 0;
    void* pos = NULL;

    /* Check for Nan and Infinity */
    pos = bsearch(word, NANINF, NNANINF, sizeof(char*), nancmp);
    if(pos != NULL) return 1;
    /* Try to convert to number */
    ncvt = sscanf(word,"%lg%n",&d,&count); 
    return (ncvt == 1 && strlen(word)==((size_t)count) ? 1 : 0);
}

static int
NCJyytext(NCJparser* parser, char* start, size_t pdlen)
{
    size_t len = (size_t)pdlen;
    if(parser->yytext == NULL) {
	parser->yytext = (char*)malloc(len+1);
	parser->yylen = len;
    } else if(parser->yylen <= len) {
	parser->yytext = (char*) realloc(parser->yytext,len+1);
	parser->yylen = len;
    }
    if(parser->yytext == NULL) return NCJTHROW(NCJ_ERR);
    memcpy(parser->yytext,start,len);
    parser->yytext[len] = '\0';
    return NCJTHROW(NCJ_OK);
}

/**************************************************/

OPTSTATIC void
NCJreclaim(NCjson* json)
{
    if(json == NULL) return;
    switch(json->sort) {
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
    case NCJ_STRING: 
	nullfree(json->string);
	break;
    case NCJ_DICT:
	NCJreclaimDict(&json->list);
	break;
    case NCJ_ARRAY:
	NCJreclaimArray(&json->list);
	break;
    default: break; /* nothing to reclaim */
    }
    free(json);
}

static void
NCJreclaimArray(struct NCjlist* array)
{
    size_t i;
    for(i=0;i<array->len;i++) {
	NCJreclaim(array->contents[i]);
    }
    nullfree(array->contents);
    array->contents = NULL;
    array->len = 0;
}

static void
NCJreclaimDict(struct NCjlist* dict)
{
   NCJreclaimArray(dict);
}

/**************************************************/
/* Build Functions */

OPTSTATIC int
NCJnew(int sort, NCjson** objectp)
{
    int stat = NCJ_OK;
    NCjson* object = NULL;

    if((object = (NCjson*)calloc(1,sizeof(NCjson))) == NULL)
	{stat = NCJTHROW(NCJ_ERR); goto done;}
    NCJsetsort(object,sort);
    switch (sort) {
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
    case NCJ_STRING:
    case NCJ_NULL:
	break;
    case NCJ_DICT:
    case NCJ_ARRAY:
	break;
    default: 
	stat = NCJTHROW(NCJ_ERR);
	goto done;
    }
    if(objectp) {*objectp = object; object = NULL;}

done:
    if(stat) NCJreclaim(object);
    return NCJTHROW(stat);
}

OPTSTATIC int
NCJnewstring(int sort, const char* value, NCjson** jsonp)
{
    return NCJTHROW(NCJnewstringn(sort,strlen(value),value,jsonp));
}

OPTSTATIC int
NCJnewstringn(int sort, size_t len, const char* value, NCjson** jsonp)
{
    int stat = NCJ_OK;
    NCjson* json = NULL;

    if(jsonp) *jsonp = NULL;
    if(value == NULL)
        {stat = NCJTHROW(NCJ_ERR); goto done;}
    if((stat = NCJnew(sort,&json))==NCJ_ERR) goto done;
    if((json->string = (char*)malloc(len+1))==NULL)
        {stat = NCJTHROW(NCJ_ERR); goto done;}
    memcpy(json->string,value,len);
    json->string[len] = '\0';
    if(jsonp) *jsonp = json;
    json = NULL; /* avoid memory errors */
done:
    NCJreclaim(json);
    return NCJTHROW(stat);
}

OPTSTATIC int
NCJdictget(const NCjson* dict, const char* key, const NCjson** jvaluep)
{
    int stat = NCJ_OK;
    size_t i;

    if(dict == NULL || dict->sort != NCJ_DICT)
        {stat = NCJTHROW(NCJ_ERR); goto done;}
    if(jvaluep) {*jvaluep = NULL;}
    for(i=0;i<NCJdictlength(dict);i++) {
	NCjson* jkey = NCJdictkey(dict,i);
	NCjson* jvalue  = NCJdictvalue(dict,i);
	if(jkey->string != NULL && strcmp(jkey->string,key)==0) {
	    if(jvaluep) {*jvaluep = jvalue;}
	    break;
	}	    
    }

done:
    return NCJTHROW(stat);
}

/* Functional version of NCJdictget */
OPTSTATIC NCjson*
NCJdictlookup(const NCjson* dict, const char* key)
{
    int stat;
    const NCjson* jvalue = NULL;
    stat = NCJdictget(dict,key,&jvalue);
    if(stat != NCJ_OK) jvalue = NULL;
    return jvalue;
}

/* Unescape the text in parser->yytext; can
   do in place because unescaped string will
   always be shorter */
static int
NCJunescape(NCJparser* parser)
{
    char* p = parser->yytext;
    char* q = p;
    char c;
    for(;(c=*p++);) {
	if(c == NCJ_ESCAPE) {
	    c = *p++;
	    switch (c) {
	    case 'b': c = '\b'; break;
	    case 'f': c = '\f'; break;
	    case 'n': c = '\n'; break;
	    case 'r': c = '\r'; break;
	    case 't': c = '\t'; break;
	    case NCJ_QUOTE: break;
	    case NCJ_ESCAPE: break;
	    default: break;/* technically not Json conformant */
	    }
	}
	*q++ = (char)c;
    }
    *q = '\0';
    return NCJTHROW(NCJ_OK);    
}

/* Unescape a single character */
static char
unescape1(char c)
{
    switch (c) {
    case 'b': c = '\b'; break;
    case 'f': c = '\f'; break;
    case 'n': c = '\n'; break;
    case 'r': c = '\r'; break;
    case 't': c = '\t'; break;
    default: break;/* technically not Json conformant */
    }
    return c;
}

#if defined NCJDEBUG || defined NCJTRACE
static char*
tokenname(int token)
{
    switch (token) {
    case NCJ_STRING: return ("NCJ_STRING");
    case NCJ_INT: return ("NCJ_INT");
    case NCJ_DOUBLE: return ("NCJ_DOUBLE");
    case NCJ_BOOLEAN: return ("NCJ_BOOLEAN");
    case NCJ_DICT: return ("NCJ_DICT");
    case NCJ_ARRAY: return ("NCJ_ARRAY");
    case NCJ_NULL: return ("NCJ_NULL");
    default:
	if(token > ' ' && token <= 127) {
	    static char s[4];
	    s[0] = '\'';
	    s[1] = (char)token;
	    s[2] = '\'';
	    s[3] = '\0';
	    return (s);
	} else
	    break;
    }
    return ("NCJ_UNDEF");
}
#endif /*defined NCJDEBUG || defined NCJTRACE*/

/* Convert a JSON value to an equivalent value of a specified sort */
OPTSTATIC int
NCJcvt(const NCjson* jvalue, int outsort, struct NCJconst* output)
{
    int stat = NCJ_OK;

    if(output == NULL) goto done;

#undef CASE
#define CASE(t1,t2) ((t1)<<4 | (t2)) /* the shift constant must be larger than log2(NCJ_NSORTS) */
    switch (CASE(jvalue->sort,outsort)) {

    case CASE(NCJ_BOOLEAN,NCJ_BOOLEAN):
	if(strcasecmp(jvalue->string,NCJ_TAG_FALSE)==0) output->bval = 0; else output->bval = 1;
	break;
    case CASE(NCJ_BOOLEAN,NCJ_INT):
	if(strcasecmp(jvalue->string,NCJ_TAG_FALSE)==0) output->ival = 0; else output->ival = 1;
	break;	
    case CASE(NCJ_BOOLEAN,NCJ_DOUBLE):
	if(strcasecmp(jvalue->string,NCJ_TAG_FALSE)==0) output->dval = 0.0; else output->dval = 1.0;
	break;	
    case CASE(NCJ_BOOLEAN,NCJ_STRING):
        output->sval = nulldup(jvalue->string);
	break;	

    case CASE(NCJ_INT,NCJ_BOOLEAN):
	sscanf(jvalue->string,"%lld",&output->ival);
	output->bval = (output->ival?1:0);
	break;	
    case CASE(NCJ_INT,NCJ_INT):
	sscanf(jvalue->string,"%lld",&output->ival);
	break;	
    case CASE(NCJ_INT,NCJ_DOUBLE):
	sscanf(jvalue->string,"%lld",&output->ival);
	output->dval = (double)output->ival;
	break;	
    case CASE(NCJ_INT,NCJ_STRING):
        output->sval = nulldup(jvalue->string);
	break;	

    case CASE(NCJ_DOUBLE,NCJ_BOOLEAN):
	sscanf(jvalue->string,"%lf",&output->dval);
	output->bval = (output->dval == 0?0:1);
	break;	
    case CASE(NCJ_DOUBLE,NCJ_INT):
	sscanf(jvalue->string,"%lf",&output->dval);
	output->ival = (long long)output->dval;
	break;	
    case CASE(NCJ_DOUBLE,NCJ_DOUBLE):
	sscanf(jvalue->string,"%lf",&output->dval);
	break;	
    case CASE(NCJ_DOUBLE,NCJ_STRING):
        output->sval = nulldup(jvalue->string);
	break;	

    case CASE(NCJ_STRING,NCJ_BOOLEAN):
	if(strcasecmp(jvalue->string,NCJ_TAG_FALSE)==0) output->bval = 0; else output->bval = 1;
	break;
    case CASE(NCJ_STRING,NCJ_INT):
	sscanf(jvalue->string,"%lld",&output->ival);
	break;
    case CASE(NCJ_STRING,NCJ_DOUBLE):
	sscanf(jvalue->string,"%lf",&output->dval);
	break;
    case CASE(NCJ_STRING,NCJ_STRING):
        output->sval = nulldup(jvalue->string);
	break;	

    default:
        stat = NCJTHROW(NCJ_ERR);
	break;
    }

done:
    return NCJTHROW(stat);
}

static int
listappend(struct NCjlist* list, NCjson* json)
{
    int stat = NCJ_OK;

    assert(list->len == 0 || list->contents != NULL);
    if(json == NULL) {stat = NCJTHROW(NCJ_ERR); goto done;}
    /* Make space for two new elements; better than one, but still probably not optimal */
    if((stat = listsetalloc(list,list->len + 2))<0) goto done;
    /* Append the new item */
    list->contents[list->len++] = json;

done:
    return NCJTHROW(stat);
}

/* Locate the index of a key in a list of (key,value) pairs.
@param list pointer to the list
@param key  for which to search
@param indexp store index of match here
@return NCJ_OK if key found, NCJ_EOF if not found, NCJ_ERR if error
*/
static int
listlookup(const struct NCjlist* list, const char* key, size_t* indexp)
{
    int stat = NCJ_OK;
    int i,len,match = -1;

    if(list == NULL || key == NULL || strlen(key) == 0 || list->len %2 == 1)
	{stat = NCJTHROW(NCJ_ERR); goto done;}
    len = (int)list->len; /* => |list| < 2 billion or do */
    for(i=0;i<len;i+=2) {
	const NCjson* jkey = list->contents[i];
	if(jkey != NULL && jkey->string != NULL && strcmp(jkey->string,key)==0) {match = i;break;}	    
    }
    if(match < 0) {stat = NCJ_EOF;}  else {if(indexp) *indexp = (size_t)match;}
done:
    return NCJTHROW(stat);
}

/* Increase the space available to dict/array.
   Even if alloc is zero, ensure that the object's list alloc is >= 1.
@param list pointer to the list
@param alloc increase allocation to this size
@return NCJ_ERR|NCJ_OK
*/
static int
listsetalloc(struct NCjlist* list, size_t alloc)
{
    int stat = NCJ_OK;
    NCjson** newcontents = NULL;

    if(list == NULL) {stat = NCJTHROW(NCJ_ERR); goto done;}
    assert(list->alloc == 0 || list->contents != NULL);
    if(alloc == 0) alloc = 1; /* Guarantee that the list->content is not NULL */
    if(list->alloc >= alloc) goto done;
    /* Since alloc > list->alloc > 0, we need to allocate space */
    if((newcontents=(NCjson**)calloc(alloc,sizeof(NCjson*))) == NULL) {stat = NCJTHROW(NCJ_ERR); goto done;}
    list->alloc = alloc;
    if(list->contents != NULL && list->len > 0) {
	/* Preserve any existing contents */
	memcpy((void*)newcontents,
		(void*)list->contents,
		sizeof(NCjson*)*list->len);
    }
    free(list->contents);
    list->contents = newcontents; newcontents = NULL;
    assert(list->alloc > 0 && list->contents != NULL);
done:
    if(newcontents != NULL) free(newcontents);
    return NCJTHROW(stat);
}

/**************************************************/

OPTSTATIC int
NCJclone(const NCjson* json, NCjson** clonep)
{
    int stat = NCJ_OK;
    NCjson* clone = NULL;

    if(clonep) *clonep = NULL;
    if(json == NULL) goto done;
    switch(NCJsort(json)) {
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
    case NCJ_STRING:
	if((stat=NCJnew(NCJsort(json),&clone))==NCJ_ERR) goto done;
	NCJsetstring(clone,strdup(NCJstring(json)));
	if(NCJstring(clone)==NULL)
	    {stat = NCJTHROW(NCJ_ERR); goto done;}
	break;
    case NCJ_NULL:
	if((stat=NCJnew(NCJsort(json),&clone))==NCJ_ERR) goto done;
	break;
    case NCJ_DICT:
	if((stat=NCJcloneDict(json,&clone))==NCJ_ERR) goto done;
	break;
    case NCJ_ARRAY:
	if((stat=NCJcloneArray(json,&clone))==NCJ_ERR) goto done;
	break;
    default: break; /* nothing to clone */
    }
done:
    if(stat == NCJ_OK && clonep) {*clonep = clone; clone = NULL;}
    NCJreclaim(clone);    
    return NCJTHROW(stat);
}

static int
NCJcloneArray(const NCjson* array, NCjson** clonep)
{
    int stat=NCJ_OK;
    size_t i;
    NCjson* clone = NULL;
    if((stat=NCJnew(NCJ_ARRAY,&clone))==NCJ_ERR) goto done;
    if((stat=listsetalloc(&clone->list,array->list.len))<0) goto done;
    for(i=0;i<NCJarraylength(array);i++) {
	NCjson* elem = NCJith(array,i);
	NCjson* elemclone = NULL;
	if((stat=NCJclone(elem,&elemclone))==NCJ_ERR) goto done;
	NCJappend(clone,elemclone);
    }
done:
    if(stat == NCJ_OK && clonep) {*clonep = clone; clone = NULL;}
    NCJreclaim(clone);    
    return stat;
}

static int
NCJcloneDict(const NCjson* dict, NCjson** clonep)
{
    int stat=NCJ_OK;
    size_t i;
    NCjson* clone = NULL;
    if((stat=NCJnew(NCJ_DICT,&clone))==NCJ_ERR) goto done;
    if((stat=listsetalloc(&clone->list,dict->list.len))<0) goto done;
    for(i=0;i<NCJarraylength(dict);i++) {
	NCjson* elem = NCJith(dict,i);
	NCjson* elemclone = NULL;
	if((stat=NCJclone(elem,&elemclone))==NCJ_ERR) goto done;
	NCJappend(clone,elemclone);
    }
done:
    if(stat == NCJ_OK && clonep) {*clonep = clone; clone = NULL;}
    NCJreclaim(clone);    
    return NCJTHROW(stat);
}

OPTSTATIC int
NCJaddstring(NCjson* json, int sort, const char* s)
{
    int stat = NCJ_OK;
    NCjson* jtmp = NULL;

    if(NCJsort(json) != NCJ_DICT && NCJsort(json) != NCJ_ARRAY)
        {stat = NCJTHROW(NCJ_ERR); goto done;}
    if((stat = NCJnewstring(sort, s, &jtmp))==NCJ_ERR) goto done;
    if((stat = NCJappend(json,jtmp))==NCJ_ERR) goto done;
    jtmp = NULL;
    
done:
    NCJreclaim(jtmp);
    return NCJTHROW(stat);
}

/* Insert key-value pair into a dict object. key will be strdup'd, jvalue will be used without copying.
   If key already exists, then it's value is overwritten
*/
OPTSTATIC int
NCJinsert(NCjson* jdict, const char* key, NCjson* jvalue)
{
    int stat = NCJ_OK;
    size_t i;
    NCjson* jkey = NULL;
    NCjson* jprev = NULL;
    int found;

    if(jdict == NULL
	|| NCJsort(jdict) != NCJ_DICT
	|| key == NULL
	|| jvalue == NULL) {stat = NCJTHROW(NCJ_ERR); goto done;}
    for(found=(-1),i=0;i < NCJdictlength(jdict); i++) {
	jkey = NCJdictkey(jdict,i);
	if (jkey != NULL && strcmp(NCJstring(jkey), key) == 0) {
	    found = (int)i;
	    break;
	}
    }
    if(found >= 0) {
	jprev = NCJdictvalue(jdict,found);
	// replace existing values for new key
	NCJreclaim(jprev); // free old value
	NCJdictvalue(jdict,found) = jvalue; jvalue = NULL;
	jkey = NULL; /* avoid reclamation */
    } else { /* not found */
        if((stat=listsetalloc(&jdict->list,jdict->list.len+2))<0) goto done;
	NCJcheck(NCJnewstring(NCJ_STRING, key, (NCjson**)&jkey));
	NCJcheck(NCJappend(jdict,jkey)); jkey = NULL;
	NCJcheck(NCJappend(jdict,jvalue)); jvalue = NULL;
    }
done:
    NCJreclaim(jkey);
    NCJreclaim(jvalue);
    return NCJTHROW(stat);
}

/* Insert key-value pair into a dict object. key will be strdup'd */
OPTSTATIC int
NCJinsertstring(NCjson* object, const char* key, const char* value)
{
    int stat = NCJ_OK;
    NCjson* jkey = NULL;
    NCjson* jvalue = NULL;
    if(key == NULL || value == NULL)
	{stat = NCJTHROW(NCJ_ERR); goto done;}
    if((stat = NCJnewstring(NCJ_STRING,key,&jkey))==NCJ_ERR) goto done;
    if((stat = NCJnewstring(NCJ_STRING,value,&jvalue))==NCJ_ERR) goto done;
    if((stat = NCJappend(object,jkey))==NCJ_ERR) goto done;
    if((stat = NCJappend(object,jvalue))==NCJ_ERR) goto done;
done:
    return NCJTHROW(stat);
}

/* Insert key-value pair into a dict object. key will be strdup'd */
OPTSTATIC int
NCJinsertint(NCjson* object, const char* key, long long value)
{
    int stat = NCJ_OK;
    NCjson* jkey = NULL;
    NCjson* jvalue = NULL;
    char digits[64];
 
    if(key == NULL)
	{stat = NCJTHROW(NCJ_ERR); goto done;}
    if((stat = NCJnewstring(NCJ_STRING,key,&jkey))==NCJ_ERR) goto done;
    snprintf(digits,sizeof(digits),"%lld",value);
    if((stat = NCJnewstring(NCJ_INT,digits,&jvalue))==NCJ_ERR) goto done;
    listsetalloc(&object->list,object->list.len + 2);
    if((stat = NCJappend(object,jkey))==NCJ_ERR) goto done;
    if((stat = NCJappend(object,jvalue))==NCJ_ERR) goto done;
done:
    return NCJTHROW(stat);
}

/* Append value to an array or dict object. */
OPTSTATIC int
NCJappend(NCjson* object, NCjson* value)
{
    if(object == NULL || value == NULL)
	return NCJTHROW(NCJ_ERR);
    switch (object->sort) {
    case NCJ_ARRAY:
    case NCJ_DICT:
	listappend(&object->list,value);
	break;
    default:
	return NCJTHROW(NCJ_ERR);
    }
    return NCJTHROW(NCJ_OK);
}

/* Append string value to an array or dict object. */
OPTSTATIC int
NCJappendstring(NCjson* object, int sort, const char* s)
{
    NCjson* js = NULL;    
    if(object == NULL || s == NULL)
	return NCJTHROW(NCJ_ERR);
    NCJnewstring(sort,s,&js);
    switch (object->sort) {
    case NCJ_ARRAY:
    case NCJ_DICT:
	listappend(&object->list,js);
	break;
    default:
	return NCJTHROW(NCJ_ERR);
    }
    return NCJTHROW(NCJ_OK);
}

/* Append int value into an array/dict object. */
OPTSTATIC int
NCJappendint(NCjson* object, long long value)
{
    int stat = NCJ_OK;
    NCjson* jvalue = NULL;
    char digits[64];
 
    snprintf(digits,sizeof(digits),"%lld",value);
    NCJcheck(NCJnewstring(NCJ_INT,digits,&jvalue));
    NCJcheck(NCJappend(object,jvalue)); jvalue = NULL;
done:
    NCJreclaim(jvalue);
    return NCJTHROW(stat);
}

/* Overwrite key-value pair in a dict object.
   If key does not exist, then act like NCJinsert().
*/
OPTSTATIC int
NCJoverwrite(NCjson* dict, const char* key, NCjson* jvalue)
{
    int stat = NCJ_OK;
    size_t index;
    NCjson* jkey = NULL;
    NCjson* oldvalue = NULL;

    if(dict == NULL
	|| dict->sort != NCJ_DICT
	|| key == NULL
	|| jvalue == NULL) {stat = NCJTHROW(NCJ_ERR); goto done;}
    /* See if key already exists */
    switch(stat=listlookup(&dict->list,key,&index)) {
    case NCJ_OK:
	/* Overwrite value part */
	oldvalue = dict->list.contents[index];
	dict->list.contents[index] = jvalue;
	NCJreclaim(oldvalue);
	break;
    case NCJ_EOF: /* Not found */
	if((stat=listsetalloc(&dict->list,dict->list.len+2))<0) goto done;
	if((stat = NCJappend(dict,jkey))==NCJ_ERR) goto done;
	if((stat = NCJappend(dict,jvalue))==NCJ_ERR) goto done;
	break;
    case NCJ_ERR:
    default: goto done;
    }
done:
    return NCJTHROW(stat);
}

/**************************************************/
/* Unparser to convert NCjson object to text in buffer */

OPTSTATIC int
NCJunparse(const NCjson* json, unsigned flags, char** textp)
{
    int stat = NCJ_OK;
    NCJbuf buf = {0,NULL};
    if((stat = NCJunparseR(json,&buf,flags))==NCJ_ERR)
	goto done;
    if(textp) {*textp = buf.text; buf.text = NULL; buf.len = 0;}
done:
    nullfree(buf.text);
    return NCJTHROW(stat);
}

static int
NCJunparseR(const NCjson* json, NCJbuf* buf, unsigned flags)
{
    int stat = NCJ_OK;
    size_t i;

    switch (NCJsort(json)) {
    case NCJ_STRING:
	bytesappendquoted(buf,json->string);
	break;
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
	bytesappend(buf,json->string);
	break;
    case NCJ_DICT:
	bytesappendc(buf,NCJ_LBRACE);
	if(json->list.len > 0 && json->list.contents != NULL) {
	    int shortlist = 0;
	    for(i=0;!shortlist && i < json->list.len;i+=2) {
		if(i > 0) {bytesappendc(buf,NCJ_COMMA);bytesappendc(buf,' ');};
		NCJunparseR(json->list.contents[i],buf,flags); /* key */
		bytesappendc(buf,NCJ_COLON);
		bytesappendc(buf,' ');
		/* Allow for the possibility of a short dict entry */
		if(json->list.contents[i+1] == NULL) { /* short */
	   	    bytesappendc(buf,'?');		
		    shortlist = 1;
		} else {
		    NCJunparseR(json->list.contents[i+1],buf,flags);
		}
	    }
	}
	bytesappendc(buf,NCJ_RBRACE);
	break;
    case NCJ_ARRAY:
	bytesappendc(buf,NCJ_LBRACKET);
	if(json->list.len > 0 && json->list.contents != NULL) {
	    for(i=0;i < json->list.len;i++) {
	        if(i > 0) bytesappendc(buf,NCJ_COMMA);
	        NCJunparseR(json->list.contents[i],buf,flags);
	    }
	}
	bytesappendc(buf,NCJ_RBRACKET);
	break;
    case NCJ_NULL:
	bytesappend(buf,"null");
	break;
    default:
	stat = NCJTHROW(NCJ_ERR); goto done;
    }
done:
    return NCJTHROW(stat);
}

/* Escape a string and append to buf */
static int
escape(const char* text, NCJbuf* buf)
{
    const char* p = text;
    char c;
    for(;(c=*p++);) {
        char replace = 0;
        switch (c) {
	case '\b': replace = 'b'; break;
	case '\f': replace = 'f'; break;
	case '\n': replace = 'n'; break;
	case '\r': replace = 'r'; break;
	case '\t': replace = 't'; break;
	case NCJ_QUOTE: replace = '\"'; break;
	case NCJ_ESCAPE: replace = '\\'; break;
	default: break;
	}
	if(replace) {
	    bytesappendc(buf,NCJ_ESCAPE);
	    bytesappendc(buf,replace);
	} else
	    bytesappendc(buf,(char)c);
    }
    return NCJTHROW(NCJ_OK);    
}

static int
bytesappendquoted(NCJbuf* buf, const char* s)
{
    bytesappend(buf,"\"");
    escape(s,buf);
    bytesappend(buf,"\"");
    return NCJTHROW(NCJ_OK);
}

static int
bytesappend(NCJbuf* buf, const char* s)
{
    int stat = NCJ_OK;
    char* newtext = NULL;
    if(buf == NULL)
        {stat = NCJTHROW(NCJ_ERR); goto done;}
    if(s == NULL) s = "";
    if(buf->len == 0) {
	assert(buf->text == NULL);
	buf->text = strdup(s);
	if(buf->text == NULL)
	    {stat = NCJTHROW(NCJ_ERR); goto done;}
	buf->len = strlen(s);
    } else {
	size_t slen = strlen(s);
	size_t newlen = buf->len + slen + 1;
        if((newtext = (char*)malloc(newlen))==NULL)
            {stat = NCJTHROW(NCJ_ERR); goto done;}
        strcpy(newtext,buf->text);
	strcat(newtext,s);	
	free(buf->text); buf->text = NULL;
	buf->text = newtext; newtext = NULL;
	buf->len = newlen;
    }

done:
    nullfree(newtext);
    return NCJTHROW(stat);
}

static int
bytesappendc(NCJbuf* bufp, const char c)
{
    char s[2];
    s[0] = c;
    s[1] = '\0';
    return bytesappend(bufp,s);
}

OPTSTATIC const char*
NCJtotext(const NCjson* json, unsigned flags)
{
    static char outtext[4096];
    char* text = NULL;
    NC_UNUSED(flags);
    if(json == NULL) {strcpy(outtext,"<null>"); goto done;}
    (void)NCJunparse(json,0,&text);
    strncpy(outtext,text,sizeof(outtext));
    nullfree(text);
done:
    return outtext;
}

OPTSTATIC void
NCJdump(const NCjson* json, unsigned flags, FILE* out)
{
    const char* text = NCJtotext(json,flags);
    if(out == NULL) out = stderr;
    fprintf(out,"%s\n",text);
    fflush(out);
}

static int
pairsort(const void* a, const void* b)
{
    const NCjson** j1 = (const NCjson**)a;
    const NCjson** j2 = (const NCjson**)b;
    return strcmp(NCJstring(*j1),NCJstring(*j2));
}

OPTSTATIC void
NCJdictsort(NCjson* jdict)
{
    assert(NCJsort(jdict) == NCJ_DICT);
    qsort((void*)NCJcontents(jdict),NCJdictlength(jdict),2*sizeof(NCjson*),pairsort);
}

/* Hack to avoid static unused warning */
static void
netcdf_supresswarnings(void)
{
    void* ignore = NULL;
    ignore = (void*)netcdf_supresswarnings;
    ignore = (void*)NCJparse;
    ignore = (void*)NCJparsen;
    ignore = (void*)NCJreclaim;
    ignore = (void*)NCJnew;
    ignore = (void*)NCJnewstring;
    ignore = (void*)NCJnewstringn;
    ignore = (void*)NCJdictget;
    ignore = (void*)NCJdictlookup;
    ignore = (void*)NCJcvt;
    ignore = (void*)NCJaddstring;
    ignore = (void*)NCJappend;
    ignore = (void*)NCJappendstring;
    ignore = (void*)NCJappendint;
    ignore = (void*)NCJinsert;
    ignore = (void*)NCJinsertstring;
    ignore = (void*)NCJinsertint;
    ignore = (void*)NCJoverwrite;
    ignore = (void*)NCJdictsort;
    ignore = (void*)NCJdump;
    (void)ignore;
}
