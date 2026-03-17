#include "jsmn.h"
#include "../../readstat.h"
#include "../extract_metadata.h"

#ifndef __JSON_METADATA_H_
#define __JSON_METADATA_H_

typedef struct json_metadata {
    char* js;
    jsmntok_t* tok;
} json_metadata;

struct json_metadata* get_json_metadata(const char* filename);
extract_metadata_type_t column_type(struct json_metadata* md, const char* varname, int output_format);
extract_metadata_format_t column_format(struct json_metadata* md, const char* varname);
void free_json_metadata(struct json_metadata*);

int get_decimals(struct json_metadata* md, const char* varname);

unsigned char get_separator(struct json_metadata* md);
int missing_double_idx(struct json_metadata* md, const char* varname, double v);
int missing_string_idx(struct json_metadata* md, const char* varname, char* v);
char* copy_variable_property(struct json_metadata* md, const char* varname, const char* property, char* dest, size_t maxsize);
jsmntok_t* find_variable_property(const char *js, jsmntok_t *t, const char* varname, const char* property);
int slurp_object(jsmntok_t *t);
jsmntok_t* find_object_property(const char *js, jsmntok_t *t, const char* propname);
char* get_object_property(const char *js, jsmntok_t *t, const char* propname, char* dest, size_t size);
int match_token(const char *js, jsmntok_t *tok, const char* name);

double get_double_from_token(const char *js, jsmntok_t* token);

#endif /* __JSON_METADATA_H_ */

