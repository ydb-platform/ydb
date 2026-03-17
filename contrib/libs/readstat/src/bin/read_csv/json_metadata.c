#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "jsmn.h"
#include "json_metadata.h"
#include "../../readstat.h"
#include "../util/file_format.h"

/* Function realloc_it() is a wrapper function for standart realloc()
 * with one difference - it frees old memory pointer in case of realloc
 * failure. Thus, DO NOT use old data pointer in anyway after call to
 * realloc_it(). If your code has some kind of fallback algorithm if
 * memory can't be re-allocated - use standart realloc() instead.
 */
static inline void *realloc_it(void *ptrmem, size_t size) {
	void *p = realloc(ptrmem, size);
	if (!p)  {
		free (ptrmem);
		fprintf(stderr, "realloc(): errno=%d\n", errno);
	}
	return p;
}

int slurp_object(jsmntok_t *t) {
    int res = 1;
    for (int i=0; i<t->size; i++) {
        res+= slurp_object((t+res));
    }
    return res;
}

int match_token(const char *js, jsmntok_t *tok, const char* name) {
    unsigned int len = tok->end - tok->start;
    return (tok->type == JSMN_STRING) && (len == strlen(name)) && (0==strncmp(js+tok->start, name, len));
}

jsmntok_t* find_object_property(const char *js, jsmntok_t *t, const char* propname) {
    int j = 0;
    for (int i = 0; i < t->size; i++) {
        jsmntok_t* tok = t+1+j;
        if (match_token(js, tok, propname)) {
            return tok+1;
        }
        j+= slurp_object(tok);
    }
    return 0;
}

char* get_object_property(const char *js, jsmntok_t *t, const char* propname, char* dest, size_t size) {
	jsmntok_t* tok = find_object_property(js, t, propname);
	if (!tok) {
		return NULL;
	}
	snprintf(dest, size, "%.*s", tok->end-tok->start, js+tok->start);
	return dest;
}

unsigned char get_separator(struct json_metadata* md) {
	jsmntok_t* token = find_object_property(md->js, md->tok, "separator");
	if (!token) {
		return ',';
	} else {
		int len = token->end - token->start;
		const char *tokenstr = md->js + token->start;
		if (len == 1) {
			return tokenstr[0];
		} else if (len == 2 && tokenstr[0] == '\\' && tokenstr[1]=='t') {
			return '\t';
		} else {
			return ',';
		}
	}
}

jsmntok_t* find_variable_property(const char *js, jsmntok_t *t, const char* varname, const char* property) {
    if (t->type != JSMN_OBJECT) {
        fprintf(stderr, "expected root token to be OBJECT\n");
        return 0;
    }

    jsmntok_t* variables = find_object_property(js, t, "variables");
    if (!variables) {
        fprintf(stderr, "Could not find variables property\n");
        return 0;
    }
    int j = 0;
    for (int i=0; i<variables->size; i++) {
        jsmntok_t* variable = variables+1+j;
        jsmntok_t* name = find_object_property(js, variable, "name");
        if (name && match_token(js, name, varname)) {
            return find_object_property(js, variable, property);
        } else if (name == 0) {
            fprintf(stderr, "name property not found\n");
        }
        j += slurp_object(variable);
    }
    return 0;
}

char* copy_variable_property(struct json_metadata* md, const char* varname, const char* property, char* dest, size_t maxsize) {
	jsmntok_t* tok = find_variable_property(md->js, md->tok, varname, property);
	if (tok == NULL) {
		return NULL;
	}

	int len = tok->end - tok->start;
	if (len == 0) {
		return NULL;
	}
	snprintf(dest, maxsize, "%.*s", len, md->js+tok->start);

	return dest;
}

int missing_string_idx(struct json_metadata* md, const char* varname, char* v) {
	jsmntok_t* missing = find_variable_property(md->js, md->tok, varname, "missing");
	if (!missing) {
		return 0;
	}

	jsmntok_t* values = find_object_property(md->js, missing, "values");
	if (!values) {
		return 0;
	}

	int j = 1;
	for (int i=0; i<values->size; i++) {
		jsmntok_t* value = values+j;
		int len = value->end - value->start;
		if (len == strlen(v)) {
			if (0 == strncmp(v, md->js + value->start, len)) {
				return i+1;
			}
		}
		j+= slurp_object(value);
	}
	return 0;
}

int missing_double_idx(struct json_metadata* md, const char* varname, double v) {
	jsmntok_t* missing = find_variable_property(md->js, md->tok, varname, "missing");
	if (!missing) {
		return 0;
	}

	jsmntok_t* values = find_object_property(md->js, missing, "values");
	if (!values) {
		return 0;
	}

	int j = 1;
	for (int i=0; i<values->size; i++) {
		jsmntok_t* value = values+j;
		int len = value->end - value->start;
		char tmp[1024];
		snprintf(tmp, sizeof(tmp), "%.*s", len, md->js + value->start);
		char *dest;
		double vv = strtod(tmp, &dest);
		if (dest == tmp) {
			fprintf(stderr, "Expected a number: %s\n", tmp);
			exit(EXIT_FAILURE);
		}
		if (vv == v) {
			return i+1;
		}
		j+= slurp_object(value);
	}
	return 0;
}

int get_decimals(struct json_metadata* md, const char* varname) {
	jsmntok_t* decimals_tok = find_variable_property(md->js, md->tok, varname, "decimals");
	if (!decimals_tok) {
		return 0;
	} else {
		char *dest;
		char *buf = md->js + decimals_tok->start;
		long int decimals = strtol(buf, &dest, 10);
		if (dest == buf) {
			fprintf(stderr, "%s:%d not a number: %.*s\n", __FILE__, __LINE__, decimals_tok->end-decimals_tok->start, buf);
			exit(EXIT_FAILURE);
		}
		return decimals;
	}
}

extract_metadata_type_t column_type(struct json_metadata* md, const char* varname, int output_format) {
	jsmntok_t* typ = find_variable_property(md->js, md->tok, varname, "type");
	if (!typ) {
		fprintf(stderr, "Could not find type of variable %s in metadata\n", varname);
		exit(EXIT_FAILURE);
	}

	if (match_token(md->js, typ, "NUMERIC")) {
		return EXTRACT_METADATA_TYPE_NUMERIC;
	} else if (match_token(md->js, typ, "STRING")) {
		return EXTRACT_METADATA_TYPE_STRING;
	} else {
		fprintf(stderr, "%s: %d: Unknown metadata type for variable %s\n", __FILE__, __LINE__, varname);
		exit(EXIT_FAILURE);
	}
}

extract_metadata_format_t column_format(struct json_metadata* md, const char* varname) {
	jsmntok_t* typ = find_variable_property(md->js, md->tok, varname, "format");
	if (!typ) {
		return EXTRACT_METADATA_FORMAT_UNSPECIFIED;
	}

	if (match_token(md->js, typ, "NUMBER")) {
		return EXTRACT_METADATA_FORMAT_NUMBER;
	} else if (match_token(md->js, typ, "PERCENT")) {
		return EXTRACT_METADATA_FORMAT_PERCENT;
	} else if (match_token(md->js, typ, "CURRENCY")) {
		return EXTRACT_METADATA_FORMAT_CURRENCY;
	} else if (match_token(md->js, typ, "DATE")) {
		return EXTRACT_METADATA_FORMAT_DATE;
	} else if (match_token(md->js, typ, "TIME")) {
		return EXTRACT_METADATA_FORMAT_TIME;
	} else if (match_token(md->js, typ, "DATE_TIME")) {
		return EXTRACT_METADATA_FORMAT_DATE_TIME;
	}
	return EXTRACT_METADATA_FORMAT_UNSPECIFIED;
}

double get_double_from_token(const char *js, jsmntok_t* token) {
	char buf[255];
    char *dest;
    int len = token->end - token->start;
    snprintf(buf, sizeof(buf), "%.*s", len, js + token->start);
    double val = strtod(buf, &dest);
    if (buf == dest) {
        fprintf(stderr, "%s:%d failed to parse double: %s\n", __FILE__, __LINE__, buf);
        exit(EXIT_FAILURE);
    }
    return val;
}

struct json_metadata* get_json_metadata(const char* filename) {
    struct json_metadata* result = malloc(sizeof(struct json_metadata));
    if (result == NULL) {
        fprintf(stderr, "%s: %d: malloc failed: %s\n", __FILE__, __LINE__, strerror(errno));
        return 0;
    }
    int r;
	int eof_expected = 0;
	char *js = NULL;
	size_t jslen = 0;
	char buf[BUFSIZ];
    FILE* fd = NULL;

	jsmn_parser p;
	jsmntok_t *tok = NULL;
	size_t tokcount = 10;

	/* Prepare parser */
	jsmn_init(&p);

	/* Allocate some tokens as a start */
	tok = malloc(sizeof(*tok) * tokcount);
	if (tok == NULL) {
		fprintf(stderr, "malloc(): error:%s\n", strerror(errno));
		goto errexit;
	}

	fd = fopen(filename, "rb");
	if (fd == NULL) {
		fprintf(stderr, "Could not open %s: %s\n", filename, strerror(errno));
		goto errexit;
	}
	for (;;) {
		/* Read another chunk */
		r = fread(buf, 1, sizeof(buf), fd);
		if (r < 0) {
			fprintf(stderr, "fread(): %s\n", strerror(errno));
			goto errexit;
		}
		if (r == 0) {
			if (eof_expected != 0) {
                break;
			} else {
				fprintf(stderr, "fread(): unexpected EOF\n");
				goto errexit;
			}
		}

		js = realloc_it(js, jslen + r + 1);
		if (js == NULL) {
			goto errexit;
		}
		strncpy(js + jslen, buf, r);
		jslen = jslen + r;

again:
		r = jsmn_parse(&p, js, jslen, tok, tokcount);
		if (r < 0) {
			if (r == JSMN_ERROR_NOMEM) {
				tokcount = tokcount * 2;
				tok = realloc_it(tok, sizeof(*tok) * tokcount);
				if (tok == NULL) {
					goto errexit;
				}
				goto again;
			}
		} else {
			eof_expected = 1;
		}
	}
    fclose(fd);
	result->tok = tok;
	result->js = js;
    return result;

	errexit:
	fprintf(stderr, "error during json metadata parsing\n");
	if (fd) {
		fclose(fd);
		fd = NULL;
	}
	if (tok) {
		free(tok);
		tok = NULL;
	}
	if (js) {
		free(js);
		js = NULL;
	}
	if (result) {
		free(result);
		result = NULL;
	}
	return NULL;
}

void free_json_metadata(struct json_metadata* md) {
	free(md->tok);
	free(md->js);
	free(md);
}
