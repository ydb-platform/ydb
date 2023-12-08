/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/json.h>
#include <aws/common/logging.h>
#include <aws/common/string.h>
#include <aws/sdkutils/private/endpoints_util.h>
#include <aws/sdkutils/sdkutils.h>

#include <inttypes.h>

#ifdef _MSC_VER /* Disable sscanf warnings on windows. */
#    pragma warning(disable : 4204)
#    pragma warning(disable : 4706)
#    pragma warning(disable : 4996)
#endif

/* 4 octets of 3 chars max + 3 separators + null terminator */
#define AWS_IPV4_STR_LEN 16
#define IP_CHAR_FMT "%03" SCNu16

/* arbitrary max length of a region. curent longest region name is 16 chars */
#define AWS_REGION_LEN 50

bool aws_is_ipv4(struct aws_byte_cursor host) {
    if (host.len > AWS_IPV4_STR_LEN - 1) {
        return false;
    }

    char copy[AWS_IPV4_STR_LEN] = {0};
    memcpy(copy, host.ptr, host.len);

    uint16_t octet[4] = {0};
    char remainder[2] = {0};
    if (4 != sscanf(
                 copy,
                 IP_CHAR_FMT "." IP_CHAR_FMT "." IP_CHAR_FMT "." IP_CHAR_FMT "%1s",
                 &octet[0],
                 &octet[1],
                 &octet[2],
                 &octet[3],
                 remainder)) {
        return false;
    }

    for (size_t i = 0; i < 4; ++i) {
        if (octet[i] > 255) {
            return false;
        }
    }

    return true;
}

static bool s_starts_with(struct aws_byte_cursor cur, uint8_t ch) {
    return cur.len > 0 && cur.ptr[0] == ch;
}

static bool s_ends_with(struct aws_byte_cursor cur, uint8_t ch) {
    return cur.len > 0 && cur.ptr[cur.len - 1] == ch;
}

static bool s_is_ipv6_char(uint8_t value) {
    return aws_isxdigit(value) || value == ':';
}

/* actual encoding is %25, but % is omitted for simplicity, since split removes it */
static struct aws_byte_cursor s_percent_uri_enc = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("25");
/*
 * IPv6 format:
 * 8 groups of 4 hex chars separated by colons (:)
 * leading 0s in each group can be skipped
 * 2 or more consecutive zero groups can be replaced by double colon (::),
 *     but only once.
 * ipv6 literal can be scoped by to zone by appending % followed by zone name
 * ( does not look like there is length reqs on zone name length. this
 * implementation enforces that its > 1 )
 * ipv6 can be embedded in url, in which case it must be wrapped inside []
 * and % be uri encoded as %25.
 * Implementation is fairly trivial and just iterates through the string
 * keeping track of the spec above.
 */
bool aws_is_ipv6(struct aws_byte_cursor host, bool is_uri_encoded) {
    if (host.len == 0) {
        return false;
    }

    if (is_uri_encoded) {
        if (!s_starts_with(host, '[') || !s_ends_with(host, ']')) {
            return false;
        }
        aws_byte_cursor_advance(&host, 1);
        --host.len;
    }

    struct aws_byte_cursor substr = {0};
    /* first split is required ipv6 part */
    bool is_split = aws_byte_cursor_next_split(&host, '%', &substr);
    AWS_ASSERT(is_split); /* function is guaranteed to return at least one split */

    if (!is_split || substr.len == 0 || (s_starts_with(substr, ':') || s_ends_with(substr, ':')) ||
        !aws_byte_cursor_satisfies_pred(&substr, s_is_ipv6_char)) {
        return false;
    }

    uint8_t group_count = 0;
    bool has_double_colon = false;
    struct aws_byte_cursor group = {0};
    while (aws_byte_cursor_next_split(&substr, ':', &group)) {
        ++group_count;

        if (group_count > 8 ||                      /* too many groups */
            group.len > 4 ||                        /* too many chars in group */
            (has_double_colon && group.len == 0)) { /* only one double colon allowed */
            return false;
        }

        has_double_colon = has_double_colon || group.len == 0;
    }

    /* second split is optional zone part */
    if (aws_byte_cursor_next_split(&host, '%', &substr)) {
        if ((is_uri_encoded &&
             (substr.len < 3 ||
              !aws_byte_cursor_starts_with(&substr, &s_percent_uri_enc))) || /* encoding for % + 1 extra char */
            (!is_uri_encoded && substr.len == 0) ||                          /* at least 1 char */
            !aws_byte_cursor_satisfies_pred(&substr, aws_isalnum)) {
            return false;
        }
    }

    return has_double_colon ? group_count < 7 : group_count == 8;
}

static char s_known_countries[][3] = {{"us"}, {"eu"}, {"ap"}, {"sa"}, {"ca"}, {"me"}, {"af"}};

struct aws_byte_cursor aws_map_region_to_partition(struct aws_byte_cursor region) {
    if (region.len > AWS_REGION_LEN - 1) {
        return aws_byte_cursor_from_c_str("");
    }

    char copy[AWS_REGION_LEN] = {0};
    memcpy(copy, region.ptr, region.len);

    char country[3] = {0};
    char location[31] = {0};
    uint8_t num = 0;

    if (3 == sscanf(copy, "%2[^-]-%30[^-]-%03" SCNu8, country, location, &num)) {
        if (location[0] != 0 && num > 0) {
            for (size_t i = 0; i < sizeof(s_known_countries) / sizeof(s_known_countries[0]); ++i) {
                if (0 == strncmp(s_known_countries[i], country, 3)) {
                    return aws_byte_cursor_from_c_str("aws");
                }
            }

            if (0 == strncmp("cn", country, 3)) {
                return aws_byte_cursor_from_c_str("aws-cn");
            }
        }
    }

    if (2 == sscanf(copy, "us-gov-%30[^-]-%03" SCNu8, location, &num)) {
        if (location[0] != 0 && num > 0) {
            return aws_byte_cursor_from_c_str("aws-us-gov");
        }
    }

    if (2 == sscanf(copy, "us-iso-%30[^-]-%03" SCNu8, location, &num)) {
        if (location[0] != 0 && num > 0) {
            return aws_byte_cursor_from_c_str("aws-iso");
        }
    }

    if (2 == sscanf(copy, "us-isob-%30[^-]-%03" SCNu8, location, &num)) {
        if (location[0] != 0 && num > 0) {
            return aws_byte_cursor_from_c_str("aws-iso-b");
        }
    }

    return aws_byte_cursor_from_c_str("");
}

bool aws_is_valid_host_label(struct aws_byte_cursor label, bool allow_subdomains) {
    bool next_must_be_alnum = true;
    size_t subdomain_count = 0;

    for (size_t i = 0; i < label.len; ++i) {
        if (label.ptr[i] == '.') {
            if (!allow_subdomains || subdomain_count == 0) {
                return false;
            }

            if (!aws_isalnum(label.ptr[i - 1])) {
                return false;
            }

            next_must_be_alnum = true;
            subdomain_count = 0;
            continue;
        }

        if (next_must_be_alnum && !aws_isalnum(label.ptr[i])) {
            return false;
        } else if (label.ptr[i] != '-' && !aws_isalnum(label.ptr[i])) {
            return false;
        }

        next_must_be_alnum = false;
        ++subdomain_count;

        if (subdomain_count > 63) {
            return false;
        }
    }

    return aws_isalnum(label.ptr[label.len - 1]);
}

struct aws_byte_cursor s_path_slash = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/");

int aws_byte_buf_init_from_normalized_uri_path(
    struct aws_allocator *allocator,
    struct aws_byte_cursor path,
    struct aws_byte_buf *out_normalized_path) {
    /* Normalized path is just regular path that ensures that path starts and ends with slash */

    if (aws_byte_buf_init(out_normalized_path, allocator, path.len + 2)) {
        AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE, "Failed init buffer for parseUrl return.");
        goto on_error;
    }

    if (path.len == 0) {
        if (aws_byte_buf_append(out_normalized_path, &s_path_slash)) {
            AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE, "Failed to add path to object.");
            goto on_error;
        }
        return AWS_OP_SUCCESS;
    }

    if (path.ptr[0] != '/') {
        if (aws_byte_buf_append_dynamic(out_normalized_path, &s_path_slash)) {
            AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE, "Failed to append slash to normalized path.");
            goto on_error;
        }
    }

    if (aws_byte_buf_append_dynamic(out_normalized_path, &path)) {
        AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE, "Failed to append path to normalized path.");
        goto on_error;
    }

    if (out_normalized_path->buffer[out_normalized_path->len - 1] != '/') {
        if (aws_byte_buf_append_dynamic(out_normalized_path, &s_path_slash)) {
            AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE, "Failed to append slash to normalized path.");
            goto on_error;
        }
    }

    return AWS_OP_SUCCESS;

on_error:
    aws_byte_buf_clean_up(out_normalized_path);
    return AWS_ERROR_SDKUTILS_ENDPOINTS_RESOLVE_FAILED;
}

struct aws_string *aws_string_new_from_json(struct aws_allocator *allocator, const struct aws_json_value *value) {
    struct aws_byte_buf json_blob;
    if (aws_byte_buf_init(&json_blob, allocator, 0)) {
        AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE, "Failed to init buffer for json conversion.");
        goto on_error;
    }

    if (aws_byte_buf_append_json_string(value, &json_blob)) {
        AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE, "Failed to convert json to string.");
        goto on_error;
    }

    struct aws_string *ret = aws_string_new_from_buf(allocator, &json_blob);
    aws_byte_buf_clean_up(&json_blob);
    return ret;

on_error:
    aws_byte_buf_clean_up(&json_blob);
    aws_raise_error(AWS_ERROR_SDKUTILS_ENDPOINTS_RESOLVE_FAILED);
    return NULL;
}

bool aws_endpoints_byte_cursor_eq(const void *a, const void *b) {
    const struct aws_byte_cursor *a_cur = a;
    const struct aws_byte_cursor *b_cur = b;
    return aws_byte_cursor_eq(a_cur, b_cur);
}

void aws_array_list_deep_clean_up(struct aws_array_list *array, aws_array_callback_clean_up_fn on_clean_up_element) {
    for (size_t idx = 0; idx < aws_array_list_length(array); ++idx) {
        void *element = NULL;

        aws_array_list_get_at_ptr(array, &element, idx);
        AWS_ASSERT(element);
        on_clean_up_element(element);
    }

    aws_array_list_clean_up(array);
}

/* TODO: this can be moved into common */
static bool s_split_on_first_delim(
    struct aws_byte_cursor input,
    char split_on,
    struct aws_byte_cursor *out_split,
    struct aws_byte_cursor *out_rest) {
    AWS_PRECONDITION(aws_byte_cursor_is_valid(&input));

    uint8_t *delim = memchr(input.ptr, split_on, input.len);
    if (delim != NULL) {
        out_split->ptr = input.ptr;
        out_split->len = delim - input.ptr;

        out_rest->ptr = delim;
        out_rest->len = input.len - (delim - input.ptr);
        return true;
    }

    *out_split = input;
    out_rest->ptr = NULL;
    out_rest->len = 0;
    return false;
}

static int s_buf_append_and_update_quote_count(
    struct aws_byte_buf *buf,
    struct aws_byte_cursor to_append,
    size_t *quote_count,
    bool is_json) {

    /* Dont count quotes if its not json. escaped quotes will be replaced with
    regular quotes when ruleset json is parsed, which will lead to incorrect
    results for when templates should be resolved in regular strings.
    Note: in json blobs escaped quotes are preserved and bellow approach works. */
    if (is_json) {
        for (size_t idx = 0; idx < to_append.len; ++idx) {
            if (to_append.ptr[idx] == '"' && !(idx > 0 && to_append.ptr[idx - 1] == '\\')) {
                ++*quote_count;
            }
        }
    }
    return aws_byte_buf_append_dynamic(buf, &to_append);
}

static struct aws_byte_cursor escaped_closing_curly = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("}}");
static struct aws_byte_cursor escaped_opening_curly = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("{{");

/*
 * Small helper to deal with escapes correctly in strings that occur before
 * template opening curly. General flow for resolving is to look for opening and
 * then closing curly. This function correctly appends any escaped closing
 * curlies and errors out if closing is not escaped (i.e. its unmatched).
 */
int s_append_template_prefix_to_buffer(
    struct aws_byte_buf *out_buf,
    struct aws_byte_cursor prefix,
    size_t *quote_count,
    bool is_json) {

    struct aws_byte_cursor split = {0};
    struct aws_byte_cursor rest = {0};

    while (s_split_on_first_delim(prefix, '}', &split, &rest)) {
        if (s_buf_append_and_update_quote_count(out_buf, split, quote_count, is_json)) {
            AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to append to resolved template buffer.");
            goto on_error;
        }

        if (*quote_count % 2 == 0) {
            if (aws_byte_buf_append_byte_dynamic(out_buf, '}')) {
                AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to append to resolved template buffer.");
                goto on_error;
            }
            aws_byte_cursor_advance(&rest, 1);
            prefix = rest;
            continue;
        }

        if (aws_byte_cursor_starts_with(&rest, &escaped_closing_curly)) {
            if (aws_byte_buf_append_byte_dynamic(out_buf, '}')) {
                AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to append to resolved template buffer.");
                goto on_error;
            }
            aws_byte_cursor_advance(&rest, 2);
        } else {
            AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Unmatched or unescaped closing curly.");
            goto on_error;
        }

        prefix = rest;
    }

    if (s_buf_append_and_update_quote_count(out_buf, split, quote_count, is_json)) {
        AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to append to resolved template buffer.");
        goto on_error;
    }

    return AWS_OP_SUCCESS;

on_error:
    return aws_raise_error(AWS_ERROR_SDKUTILS_ENDPOINTS_RESOLVE_FAILED);
}

int aws_byte_buf_init_from_resolved_templated_string(
    struct aws_allocator *allocator,
    struct aws_byte_buf *out_buf,
    struct aws_byte_cursor string,
    aws_endpoints_template_resolve_fn resolve_callback,
    void *user_data,
    bool is_json) {
    AWS_PRECONDITION(allocator);

    struct aws_owning_cursor resolved_template;
    AWS_ZERO_STRUCT(resolved_template);

    if (aws_byte_buf_init(out_buf, allocator, string.len)) {
        return aws_raise_error(AWS_ERROR_SDKUTILS_ENDPOINTS_RESOLVE_FAILED);
    }

    size_t quote_count = is_json ? 0 : 1;
    struct aws_byte_cursor split = {0};
    struct aws_byte_cursor rest = {0};
    while (s_split_on_first_delim(string, '{', &split, &rest)) {
        if (s_append_template_prefix_to_buffer(out_buf, split, &quote_count, is_json)) {
            AWS_LOGF_ERROR(
                AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to append to buffer while evaluating templated sting.");
            goto on_error;
        }

        if (quote_count % 2 == 0) {
            if (aws_byte_buf_append_byte_dynamic(out_buf, '{')) {
                AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to append to resolved template buffer.");
                goto on_error;
            }
            aws_byte_cursor_advance(&rest, 1);
            string = rest;
            continue;
        }

        if (aws_byte_cursor_starts_with(&rest, &escaped_opening_curly)) {
            if (aws_byte_buf_append_byte_dynamic(out_buf, '{')) {
                AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to append to resolved template buffer.");
                goto on_error;
            }
            aws_byte_cursor_advance(&rest, 2);
            string = rest;
            continue;
        }

        aws_byte_cursor_advance(&rest, 1);

        struct aws_byte_cursor after_closing = {0};
        if (!s_split_on_first_delim(rest, '}', &split, &after_closing)) {
            AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Unmatched closing curly.");
            goto on_error;
        }
        aws_byte_cursor_advance(&after_closing, 1);
        string = after_closing;

        if (resolve_callback(split, user_data, &resolved_template)) {
            AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to resolve template.");
            goto on_error;
        }

        if (s_buf_append_and_update_quote_count(out_buf, resolved_template.cur, &quote_count, is_json)) {
            AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to append resolved value.");
            goto on_error;
        }

        aws_owning_cursor_clean_up(&resolved_template);
    }

    if (s_buf_append_and_update_quote_count(out_buf, split, &quote_count, is_json)) {
        AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_GENERAL, "Failed to append to resolved template buffer.");
        goto on_error;
    }

    return AWS_OP_SUCCESS;

on_error:
    aws_byte_buf_clean_up(out_buf);
    aws_owning_cursor_clean_up(&resolved_template);
    return aws_raise_error(AWS_ERROR_SDKUTILS_ENDPOINTS_RESOLVE_FAILED);
}

int aws_path_through_json(
    struct aws_allocator *allocator,
    const struct aws_json_value *root,
    struct aws_byte_cursor path,
    const struct aws_json_value **out_value) {

    struct aws_array_list path_segments;
    if (aws_array_list_init_dynamic(&path_segments, allocator, 10, sizeof(struct aws_byte_cursor)) ||
        aws_byte_cursor_split_on_char(&path, '.', &path_segments)) {
        goto on_error;
    }

    *out_value = root;
    for (size_t idx = 0; idx < aws_array_list_length(&path_segments); ++idx) {
        struct aws_byte_cursor path_el_cur;
        if (aws_array_list_get_at(&path_segments, &path_el_cur, idx)) {
            AWS_LOGF_ERROR(AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE, "Failed to get path element");
            goto on_error;
        }

        struct aws_byte_cursor element_cur = {0};
        aws_byte_cursor_next_split(&path_el_cur, '[', &element_cur);

        struct aws_byte_cursor index_cur = {0};
        bool has_index = aws_byte_cursor_next_split(&path_el_cur, '[', &index_cur) &&
                         aws_byte_cursor_next_split(&path_el_cur, ']', &index_cur);

        if (element_cur.len > 0) {
            *out_value = aws_json_value_get_from_object(*out_value, element_cur);
            if (NULL == *out_value) {
                AWS_LOGF_ERROR(
                    AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE, "Invalid path. " PRInSTR ".", AWS_BYTE_CURSOR_PRI(element_cur));
                goto on_error;
            }
        }

        if (has_index) {
            uint64_t index;
            if (aws_byte_cursor_utf8_parse_u64(index_cur, &index)) {
                AWS_LOGF_ERROR(
                    AWS_LS_SDKUTILS_ENDPOINTS_RESOLVE,
                    "Failed to parse index: " PRInSTR,
                    AWS_BYTE_CURSOR_PRI(index_cur));
                goto on_error;
            }
            *out_value = aws_json_get_array_element(*out_value, (size_t)index);
            if (NULL == *out_value) {
                aws_reset_error();
                goto on_success;
            }
        }
    }

on_success:
    aws_array_list_clean_up(&path_segments);
    return AWS_OP_SUCCESS;

on_error:
    aws_array_list_clean_up(&path_segments);
    *out_value = NULL;
    return aws_raise_error(AWS_ERROR_SDKUTILS_ENDPOINTS_RESOLVE_FAILED);
}

struct aws_owning_cursor aws_endpoints_owning_cursor_create(
    struct aws_allocator *allocator,
    const struct aws_string *str) {
    struct aws_string *clone = aws_string_clone_or_reuse(allocator, str);
    struct aws_owning_cursor ret = {.string = clone, .cur = aws_byte_cursor_from_string(clone)};
    return ret;
}

struct aws_owning_cursor aws_endpoints_owning_cursor_from_string(struct aws_string *str) {
    struct aws_owning_cursor ret = {.string = str, .cur = aws_byte_cursor_from_string(str)};
    return ret;
}

struct aws_owning_cursor aws_endpoints_owning_cursor_from_cursor(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor cur) {
    struct aws_string *clone = aws_string_new_from_cursor(allocator, &cur);
    struct aws_owning_cursor ret = {.string = clone, .cur = aws_byte_cursor_from_string(clone)};
    return ret;
}

struct aws_owning_cursor aws_endpoints_non_owning_cursor_create(struct aws_byte_cursor cur) {
    struct aws_owning_cursor ret = {.string = NULL, .cur = cur};
    return ret;
}

void aws_owning_cursor_clean_up(struct aws_owning_cursor *cursor) {
    aws_string_destroy(cursor->string);
    cursor->string = NULL;
    cursor->cur.ptr = NULL;
    cursor->cur.len = 0;
}
