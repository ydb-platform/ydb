#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

char *uwsgi_str_split_nget(char *str, size_t len, char what, size_t pos, size_t *rlen) {
	size_t i;
	size_t current = 0;
	char *choosen = str;
	size_t choosen_len = 0;
	*rlen = 0;
	for(i=0;i<len;i++) {
		if (!choosen) choosen = str + i;
		if (str[i] == what) {
			if (current == pos) {
				if (choosen_len == 0) return NULL;
				*rlen = choosen_len;
				return choosen;
			}
			current++;
			choosen = NULL;
			choosen_len = 0;
		}
		else {
			choosen_len ++;
		}
	}

	if (current == pos) {
		if (choosen_len == 0) return NULL;
		*rlen = choosen_len;
		return choosen;
	}

	return NULL;
}

size_t uwsgi_str_occurence(char *str, size_t len, char what) {
	size_t count = 0;
	size_t i;
	for(i=0;i<len;i++) {
		if (str[i] == what) count++;
	}
	return count;
}

// check if a string_list contains an item
struct uwsgi_string_list *uwsgi_string_list_has_item(struct uwsgi_string_list *list, char *key, size_t keylen) {
        struct uwsgi_string_list *usl = list;
        while (usl) {
                if (keylen == usl->len) {
                        if (!memcmp(key, usl->value, keylen)) {
                                return usl;
                        }
                }
                usl = usl->next;
        }
        return NULL;
}

// lower a string
char *uwsgi_lower(char *str, size_t size) {
        size_t i;
        for (i = 0; i < size; i++) {
                str[i] = tolower((int) str[i]);
        }

        return str;
}

// check if a string is contained in another one
char *uwsgi_str_contains(char *str, int slen, char what) {

        int i;
        for (i = 0; i < slen; i++) {
                if (str[i] == what) {
                        return str + i;
                }
        }
        return NULL;
}

int uwsgi_contains_n(char *s1, size_t s1_len, char *s2, size_t s2_len) {
        size_t i;
        char *ptr = s2;
        for (i = 0; i < s1_len; i++) {
                if (s1[i] == *ptr) {
                        ptr++;
                        if (ptr == s2 + s2_len) {
                                return 1;
                        }
                }
                else {
                        ptr = s2;
                }
        }
        return 0;
}

// fast compare 2 sized strings
int uwsgi_strncmp(char *src, int slen, char *dst, int dlen) {

        if (slen != dlen)
                return 1;

        return memcmp(src, dst, dlen);

}

// fast compare 2 sized strings (case insensitive)
int uwsgi_strnicmp(char *src, int slen, char *dst, int dlen) {

        if (slen != dlen)
                return 1;

        return strncasecmp(src, dst, dlen);

}

// fast sized check of initial part of a string
int uwsgi_starts_with(char *src, int slen, char *dst, int dlen) {

        if (slen < dlen)
                return -1;

        return memcmp(src, dst, dlen);
}


// unsized check
int uwsgi_startswith(char *src, char *what, int wlen) {

        int i;

        for (i = 0; i < wlen; i++) {
                if (src[i] != what[i])
                        return -1;
        }

        return 0;
}

// concatenate strings
char *uwsgi_concatn(int c, ...) {

        va_list s;
        char *item;
        int j = c;
        char *buf;
        size_t len = 1;
        size_t tlen = 1;

        va_start(s, c);
        while (j > 0) {
                item = va_arg(s, char *);
                if (item == NULL) {
                        break;
                }
                len += va_arg(s, int);
                j--;
        }
        va_end(s);


        buf = uwsgi_malloc(len);
        memset(buf, 0, len);

        j = c;

        len = 0;

        va_start(s, c);
        while (j > 0) {
                item = va_arg(s, char *);
                if (item == NULL) {
                        break;
                }
                tlen = va_arg(s, int);
                memcpy(buf + len, item, tlen);
                len += tlen;
                j--;
        }
        va_end(s);


        return buf;

}

char *uwsgi_concat2(char *one, char *two) {

        char *buf;
        size_t len = strlen(one) + strlen(two) + 1;


        buf = uwsgi_malloc(len);
        buf[len - 1] = 0;

        memcpy(buf, one, strlen(one));
        memcpy(buf + strlen(one), two, strlen(two));

        return buf;

}

char *uwsgi_concat4(char *one, char *two, char *three, char *four) {

        char *buf;
        size_t len = strlen(one) + strlen(two) + strlen(three) + strlen(four) + 1;


        buf = uwsgi_malloc(len);
        buf[len - 1] = 0;

        memcpy(buf, one, strlen(one));
        memcpy(buf + strlen(one), two, strlen(two));
        memcpy(buf + strlen(one) + strlen(two), three, strlen(three));
        memcpy(buf + strlen(one) + strlen(two) + strlen(three), four, strlen(four));

        return buf;

}


char *uwsgi_concat3(char *one, char *two, char *three) {

        char *buf;
        size_t len = strlen(one) + strlen(two) + strlen(three) + 1;


        buf = uwsgi_malloc(len);
        buf[len - 1] = 0;

        memcpy(buf, one, strlen(one));
        memcpy(buf + strlen(one), two, strlen(two));
        memcpy(buf + strlen(one) + strlen(two), three, strlen(three));

        return buf;

}

char *uwsgi_concat2n(char *one, int s1, char *two, int s2) {

        char *buf;
        size_t len = s1 + s2 + 1;


        buf = uwsgi_malloc(len);
        buf[len - 1] = 0;

        memcpy(buf, one, s1);
        memcpy(buf + s1, two, s2);

        return buf;

}

char *uwsgi_concat2nn(char *one, int s1, char *two, int s2, int *len) {

        char *buf;
        *len = s1 + s2 + 1;


        buf = uwsgi_malloc(*len);
        buf[*len - 1] = 0;

        memcpy(buf, one, s1);
        memcpy(buf + s1, two, s2);

        return buf;

}


char *uwsgi_concat3n(char *one, int s1, char *two, int s2, char *three, int s3) {

        char *buf;
        size_t len = s1 + s2 + s3 + 1;


        buf = uwsgi_malloc(len);
        buf[len - 1] = 0;

        memcpy(buf, one, s1);
        memcpy(buf + s1, two, s2);
        memcpy(buf + s1 + s2, three, s3);

        return buf;

}

char *uwsgi_concat4n(char *one, int s1, char *two, int s2, char *three, int s3, char *four, int s4) {

        char *buf;
        size_t len = s1 + s2 + s3 + s4 + 1;


        buf = uwsgi_malloc(len);
        buf[len - 1] = 0;

        memcpy(buf, one, s1);
        memcpy(buf + s1, two, s2);
        memcpy(buf + s1 + s2, three, s3);
        memcpy(buf + s1 + s2 + s3, four, s4);

        return buf;

}

// concat unsized strings
char *uwsgi_concat(int c, ...) {

        va_list s;
        char *item;
        size_t len = 1;
        int j = c;
        char *buf;

        va_start(s, c);
        while (j > 0) {
                item = va_arg(s, char *);
                if (item == NULL) {
                        break;
                }
                len += (int) strlen(item);
                j--;
        }
        va_end(s);


        buf = uwsgi_malloc(len);
        memset(buf, 0, len);

        j = c;

        len = 0;

        va_start(s, c);
        while (j > 0) {
                item = va_arg(s, char *);
                if (item == NULL) {
                        break;
                }
                memcpy(buf + len, item, strlen(item));
                len += strlen(item);
                j--;
        }
        va_end(s);


        return buf;

}

char *uwsgi_strncopy(char *s, int len) {

        char *buf;

        buf = uwsgi_malloc(len + 1);
        buf[len] = 0;

        memcpy(buf, s, len);

        return buf;

}

// this move a string back of one char and put a 0 at the end (used in uwsgi parsers for buffer reusing)
char *uwsgi_cheap_string(char *buf, int len) {

        int i;
        char *cheap_buf = buf - 1;


        for (i = 0; i < len; i++) {
                *cheap_buf++ = buf[i];
        }


        buf[len - 1] = 0;

        return buf - 1;
}

/*
	status 0: append mode
	status 1: single quote found
	status 2: double quote found
	status 3: backslash found (on 0)
	status 4: backslash found (on 1)
	status 5: backslash found (on 2)
	
*/
char ** uwsgi_split_quoted(char *what, size_t what_len, char *sep, size_t *rlen) {
	size_t i;
	int status = 0;
	char *base = uwsgi_concat2n(what, what_len, "", 0);
	char *item = NULL;
	char *ptr = NULL;
	*rlen = 0;
	char **ret = (char **) uwsgi_malloc(sizeof(char *) * (what_len+1));

	for(i=0;i<what_len;i++) {
		if (!item) {
			item = uwsgi_malloc((what_len - i)+1);
			ptr = item;
		}

		if (status == 0) {
			if (base[i] == '\\') {
				status = 3;
			}	
			else if (base[i] == '"') {
				status = 2;
			}
			else if (base[i] == '\'') {
				status = 1;
			}
			else if (strchr(sep, base[i])) {
				*ptr++= 0;
				ret[(*rlen)] = item; (*rlen)++;
				item = NULL;
			}
			else {
				*ptr++= base[i];
			}
			continue;
		}

		// backslash
		if (status == 3) {
			*ptr++= base[i];
			status = 0;
			continue;
		}

		// single quote
		if (status == 1) {
			if (base[i] == '\\') {
				status = 4;
			}
			else if (base[i] == '\'') {
				status = 0;
			}
			else {
				*ptr++= base[i];
			}
			continue;
		}

		// double quote
		if (status == 2) {
			if (base[i] == '\\') {
                                status = 5;
                        }
                        else if (base[i] == '"') {
                        	status = 0;
                        }
                        else {
                                *ptr++= base[i];
                        }
			continue;
		}

		// backslash single quote
                if (status == 4) {
                        *ptr++= base[i];
                        status = 1;
			continue;
                }

		// backslash double quote
                if (status == 5) {
                        *ptr++= base[i];
                        status = 2;
			continue;
                }
	}

	if (item) {
		*ptr++= 0;
		ret[(*rlen)] = item; (*rlen)++;
	}

	free(base);

	return ret;
}
