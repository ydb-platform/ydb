// Ensure we have asprintf's definition on glibc-based platforms to avoid compiler warnings
#define _GNU_SOURCE
#include <stdio.h>

#include <pg_query.h>

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "split_tests.c"

int main()
{
	size_t i;
	bool ret_code = EXIT_SUCCESS;

	for (i = 0; i < testsLength; i += 2)
	{
		PgQuerySplitResult result = pg_query_split_with_scanner(tests[i]);

		if (result.error)
		{
			ret_code = EXIT_FAILURE;
			printf("%s\n", result.error->message);
			pg_query_free_split_result(result);
			continue;
		}

		char *buf = strdup("");
		for (int i = 0; i < result.n_stmts; i++)
		{
			char *newbuf;
			int nbytes = asprintf(&newbuf, "%sloc=%d,len=%d;", buf, result.stmts[i]->stmt_location, result.stmts[i]->stmt_len);
			if (nbytes == -1)
			{
				printf("FAILED TO ALLOCATE MEMORY\n");
				return EXIT_FAILURE;
			}
			free(buf);
			buf = newbuf;
		}
		// Drop trailing ;
		if (strlen(buf) > 0 && buf[strlen(buf) - 1] == ';')
			buf[strlen(buf) - 1] = '\0';

		if (strcmp(buf, tests[i + 1]) != 0)
		{
			ret_code = EXIT_FAILURE;
			printf("INVALID scanner split result for \"%s\"\nexpected: %s\n  actual: %s\n", tests[i], tests[i + 1], buf);
		}
		else
		{
			printf(".");
		}

		free(buf);
		pg_query_free_split_result(result);

		// Now the same again with the parser splitter
		result = pg_query_split_with_parser(tests[i]);

		if (result.error)
		{
			ret_code = EXIT_FAILURE;
			printf("%s\n", result.error->message);
			pg_query_free_split_result(result);
			continue;
		}

		buf = strdup("");
		for (int i = 0; i < result.n_stmts; i++)
		{
			char *newbuf;
			int nbytes = asprintf(&newbuf, "%sloc=%d,len=%d;", buf, result.stmts[i]->stmt_location, result.stmts[i]->stmt_len);
			if (nbytes == -1)
			{
				printf("FAILED TO ALLOCATE MEMORY\n");
				return EXIT_FAILURE;
			}
			free(buf);
			buf = newbuf;
		}
		// Drop trailing ;
		if (strlen(buf) > 0 && buf[strlen(buf) - 1] == ';')
			buf[strlen(buf) - 1] = '\0';

		if (strcmp(buf, tests[i + 1]) != 0)
		{
			ret_code = EXIT_FAILURE;
			printf("INVALID parser split result for \"%s\"\nexpected: %s\n  actual: %s\n", tests[i], tests[i + 1], buf);
		}
		else
		{
			printf(".");
		}

		free(buf);
		pg_query_free_split_result(result);
	}

	printf("\n");

	pg_query_exit();

	return ret_code;
}
