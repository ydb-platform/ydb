#include <contrib/python/uWSGI/py2/config.h>
#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

void uwsgi_opt_pcre_jit(char *opt, char *value, void *foobar) {
#if defined(PCRE_STUDY_JIT_COMPILE) && defined(PCRE_CONFIG_JIT)
	int has_jit = 0, ret;
	ret = pcre_config(PCRE_CONFIG_JIT, &has_jit);
	if (ret != 0 || has_jit != 1)
		return;
	uwsgi.pcre_jit = PCRE_STUDY_JIT_COMPILE;
#elif defined(PCRE2_CONFIG_JIT)
	int has_jit = 0, ret;
	ret = pcre2_config(PCRE2_CONFIG_JIT, &has_jit);
	if (ret != 0)
		return;
	uwsgi.pcre_jit = has_jit;
#endif
}

int uwsgi_regexp_build(char *re, uwsgi_pcre ** pattern) {

#ifdef UWSGI_PCRE2
	int errnbr;
	size_t erroff;

	*pattern = pcre2_compile((const unsigned char *) re, PCRE2_ZERO_TERMINATED, 0, &errnbr, &erroff, NULL);
#else
	const char *errstr;
	int erroff;

	*pattern = uwsgi_malloc(sizeof(uwsgi_pcre));
	(*pattern)->p = pcre_compile((const char *) re, 0, &errstr, &erroff, NULL);
#endif
#ifdef UWSGI_PCRE2
	if (!(*pattern)) {
		uwsgi_log("pcre error: code %d at offset %d\n", errnbr, erroff);
#else
	if (!((*pattern)->p)) {
		uwsgi_log("pcre error: %s at offset %d\n", errstr, erroff);
#endif
		return -1;
	}

#ifdef UWSGI_PCRE2
	if (uwsgi.pcre_jit) {
		errnbr = pcre2_jit_compile(*pattern, PCRE2_JIT_COMPLETE);
		if (errnbr) {
			pcre2_code_free(*pattern);
			uwsgi_log("pcre JIT compile error code %d\n", errnbr);
			return -1;
		}
#else
	int opt = uwsgi.pcre_jit;

	(*pattern)->extra = (pcre_extra *) pcre_study((const pcre *) (*pattern)->p, opt, &errstr);
	if ((*pattern)->extra == NULL && errstr != NULL) {
		pcre_free((*pattern)->p);
		free(*pattern);
		uwsgi_log("pcre (study) error: %s\n", errstr);
		return -1;
#endif
	}

	return 0;

}

int uwsgi_regexp_match(uwsgi_pcre *pattern, const char *subject, int length) {
#ifdef UWSGI_PCRE2
	return uwsgi_regexp_match_ovec(pattern, subject, length, NULL, 0);
#else
	return pcre_exec((const pcre *) pattern->p, (const pcre_extra *) pattern->extra, subject, length, 0, 0, NULL, 0);
#endif
}

int uwsgi_regexp_match_ovec(uwsgi_pcre *pattern, const char *subject, int length, int *ovec, int n) {

#ifdef UWSGI_PCRE2
	int rc;
	int i;
	pcre2_match_data *match_data;
	size_t *pcre2_ovec;

	match_data = pcre2_match_data_create_from_pattern(pattern, NULL);
	rc = pcre2_match(pattern, (const unsigned char *)subject, length, 0, 0, match_data, NULL);

	/*
	 * Quoting PCRE{,2} spec, "The first pair of integers, ovector[0]
	 * and ovector[1], identify the portion of the subject string matched
	 * by the entire pattern. The next pair is used for the first capturing
	 * subpattern, and so on." Therefore, the ovector size is the number of
	 * capturing subpatterns (INFO_CAPTURECOUNT), from uwsgi_regexp_ovector(),
	 * as matching pairs, plus room for the first pair.
	 */
	if (n > 0) {
		// copy pcre2 output vector to uwsgi output vector
		pcre2_ovec = pcre2_get_ovector_pointer(match_data);
		for (i=0;i<(n+1)*2;i++) {
			ovec[i] = pcre2_ovec[i];
		}
#else
	if (n > 0) {
		return pcre_exec((const pcre *) pattern->p, (const pcre_extra *) pattern->extra, subject, length, 0, 0, ovec, PCRE_OVECTOR_BYTESIZE(n));
#endif
	}

#ifdef UWSGI_PCRE2
	pcre2_match_data_free(match_data);

	return rc;
#else
	return pcre_exec((const pcre *) pattern->p, (const pcre_extra *) pattern->extra, subject, length, 0, 0, NULL, 0);
#endif
}

int uwsgi_regexp_ovector(const uwsgi_pcre *pattern) {

	int n;
#ifdef UWSGI_PCRE2
	if (pcre2_pattern_info(pattern, PCRE2_INFO_CAPTURECOUNT, &n))
#else
	if (pcre_fullinfo((const pcre *) pattern->p, (const pcre_extra *) pattern->extra, PCRE_INFO_CAPTURECOUNT, &n))
#endif
		return 0;

	return n;
}

char *uwsgi_regexp_apply_ovec(char *src, int src_n, char *dst, int dst_n, int *ovector, int n) {

	int i;
	int dollar = 0;

	size_t dollars = n;

	for(i=0;i<dst_n;i++) {
		if (dst[i] == '$') {
			dollars++;
		}
	}

	char *res = uwsgi_malloc(dst_n + (src_n * dollars) + 1);
	char *ptr = res;

	for (i = 0; i < dst_n; i++) {
		if (dollar) {
			if (isdigit((int) dst[i])) {
				int pos = (dst[i] - 48);
				if (pos <= n) {
					pos = pos * 2;
					memcpy(ptr, src + ovector[pos], ovector[pos + 1] - ovector[pos]);
					ptr += ovector[pos + 1] - ovector[pos];
				}
			}
			else {
				*ptr++ = '$';
				*ptr++ = dst[i];
			}
			dollar = 0;
		}
		else {
			if (dst[i] == '$') {
				dollar = 1;
			}
			else {
				*ptr++ = dst[i];
			}
		}
	}

	*ptr++ = 0;

	return res;
}

#endif
