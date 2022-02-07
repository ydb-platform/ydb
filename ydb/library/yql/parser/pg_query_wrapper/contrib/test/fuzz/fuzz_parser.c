#include <pg_query.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
	char *new_str = (char *)malloc(size+1);
	if (new_str == NULL){
        return 0;
	}
	memcpy(new_str, data, size);
	new_str[size] = '\0';

	PgQueryParseResult result = pg_query_parse(new_str);
	pg_query_free_parse_result(result);

	free(new_str);
	return 0;
}
