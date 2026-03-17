/* Copyright 2018, UCAR/Unidata.
   See the COPYRIGHT file for more information.
*/

#ifndef NCJSON_H
#define NCJSON_H 1

#ifndef OPTEXPORT
#ifdef NETCDF_JSON_H
#define OPTEXPORT static
#else /*!NETCDF_JSON_H*/
#ifdef _WIN32
#define OPTEXPORT __declspec(dllexport)
#else /*!WIN32*/
#define OPTEXPORT extern
#endif /*WIN32*/
#endif /*NETCDF_JSON_H*/
#endif /*OPTEXPORT*/

/**************************************************/

/* Return codes */
#define NCJ_OK 0 /* must equal NC_NOERR in netcdf.h */
#define NCJ_ERR (-1) /* must equal NC_ERROR in netcdf.h */

/* Json object sorts (note use of term sort rather than e.g. type or discriminant) */
#define NCJ_UNDEF    0
#define NCJ_STRING   1
#define NCJ_INT      2
#define NCJ_DOUBLE   3
#define NCJ_BOOLEAN  4
#define NCJ_DICT     5
#define NCJ_ARRAY    6
#define NCJ_NULL     7

#define NCJ_NSORTS   8

/* Dump/text/unparse flags */
#define NCJFLAG_NONE	    0
#define NCJFLAG_INDENTED    1

/* Define a struct to store primitive values as unquoted
   strings. The sort will provide more info.  Do not bother with
   a union since the amount of saved space is minimal.
*/

typedef struct NCjson {
    int sort;     /* of this object */
    char* string; /* sort != DICT|ARRAY */
    struct NCjlist {
	size_t alloc;
	size_t len;
	struct NCjson** contents;
    } list; /* sort == DICT|ARRAY */
} NCjson;

/* Structure to hold result of convertinf one json sort to  value of another type;
   don't use union so we can know when to reclaim sval
*/
struct NCJconst {int bval; long long ival; double dval; char* sval;};

/**************************************************/
/* Extended API */

/* Return NCJ_OK if ok else NCJ_ERR */

#if defined(__cplusplus)
extern "C" {
#endif /*__cplusplus*/

/* Parse a string to NCjson*/
OPTEXPORT int NCJparse(const char* text, unsigned flags, NCjson** jsonp);

/* Parse a counted string to NCjson*/
OPTEXPORT int NCJparsen(size_t len, const char* text, unsigned flags, NCjson** jsonp);

/* Reclaim a JSON tree */
OPTEXPORT void NCJreclaim(NCjson* json);

/* Create a new JSON node of a given sort */
OPTEXPORT int NCJnew(int sort, NCjson** objectp);

/* Create new json object with given string content */
OPTEXPORT int NCJnewstring(int sort, const char* value, NCjson** jsonp);

/* Create new json object with given counted string content */
OPTEXPORT int NCJnewstringn(int sort, size_t len, const char* value, NCjson** jsonp);

/* Get dict key value by name */
OPTEXPORT int NCJdictget(const NCjson* dict, const char* key, const NCjson** valuep);

/* Functional version of NCJdictget */
OPTEXPORT NCjson* NCJdictlookup(const NCjson* dict, const char* key);

/* Convert one json sort to  value of another type; don't use union so we can know when to reclaim sval */
OPTEXPORT int NCJcvt(const NCjson* value, int outsort, struct NCJconst* output);

/* Append an atomic value to an array or dict object. */
OPTEXPORT int NCJaddstring(NCjson* json, int sort, const char* s);

/* Append value to an array or dict object. */
OPTEXPORT int NCJappend(NCjson* object, NCjson* value);

/* Append string value to an array or dict object. */
OPTEXPORT int NCJappendstring(NCjson* object, int sort, const char* s);

/* Append int value to an array or dict object. */
OPTEXPORT int NCJappendint(NCjson* object, long long n);

/* Insert (string)key-(NCjson*)value pair into a dict object. key will be copied; jvalue will not */
OPTEXPORT int NCJinsert(NCjson* object, const char* key, NCjson* jvalue);

/* Insert key-value pair into a dict object. key and value will be copied */
OPTEXPORT int NCJinsertstring(NCjson* object, const char* key, const char* value);

/* Overwrite key-value pair in a dict object. Act like NCJinsert if key not found */
OPTEXPORT int NCJoverwrite(NCjson* object, const char* key, NCjson* value);

/* Insert key-value pair into a dict object. key and value will be copied */
OPTEXPORT int NCJinsertint(NCjson* object, const char* key, long long n);

/* Unparser to convert NCjson object to text in buffer */
OPTEXPORT int NCJunparse(const NCjson* json, unsigned flags, char** textp);

/* Deep clone a json object */
OPTEXPORT int NCJclone(const NCjson* json, NCjson** clonep);

#ifndef NETCDF_JSON_H

/* dump NCjson* object to output file */
OPTEXPORT void NCJdump(const NCjson* json, unsigned flags, FILE*);

/* convert NCjson* object to output string */
OPTEXPORT const char* NCJtotext(const NCjson* json, unsigned flags);

/* Sort a dictionary by key */
OPTEXPORT void NCJdictsort(NCjson* jdict);

#endif /*NETCDF_JSON_H*/

#if defined(__cplusplus)
}
#endif /*__cplusplus*/

/* Getters */
#define NCJsort(x) ((x)==NULL?NCJ_UNDEF:(x)->sort)
#define NCJstring(x) ((x)==NULL?(char*)(x):(x)->string)
#define NCJarraylength(x) ((x)==NULL ? 0 : (x)->list.len)
#define NCJdictlength(x) ((x)==NULL ? 0 : ((x)->list.len) / 2)
#define NCJcontents(x) ((x)==NULL?(struct NCjson**)(x):(x)->list.contents)
#define NCJith(x,i) ((x)->list.contents[i])
#define NCJdictkey(x,i) ((x)->list.contents[(i)*2])
#define NCJdictvalue(x,i) ((x)->list.contents[((i)*2)+1])

/* Setters */
#define NCJsetsort(x,s) (x)->sort=(s)
#define NCJsetstring(x,y) (x)->string=(y)
#define NCJsetcontents(x,c) (x)->list.contents=(c)
#define NCJsetarraylength(x,l) (x)->list.len=(l)
#define NCJsetdictlength(x,l) (x)->list.len=((l)*2)

/* Misc */
#define NCJisatomic(j) ((j)->sort != NCJ_ARRAY && (j)->sort != NCJ_DICT && (j)->sort != NCJ_NULL && (j)->sort != NCJ_UNDEF)

/**************************************************/
/* Error detection helper */
#undef NCJDEBUG
#ifdef NCJDEBUG
static int
NCJBREAKPOINT(int err)
{
    (void)NCJBREAKPOINT;
    return err;
}
#else
#define NCJBREAKPOINT(err) (err)
#endif /*NCJDEBUG*/
#define NCJcheck(expr) do{if((expr) < 0) {stat = NCJBREAKPOINT(NCJ_ERR); goto done;}}while(0)

/**************************************************/

#endif /*!NCJSON_H*/ /* Leave the ! as a tag for sed */
