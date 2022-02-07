#include <pg_query.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>

#include "parse_tests.c"

#ifdef USE_VALGRIND
#define THREAD_COUNT 50
#else
#define THREAD_COUNT 500
#endif

void* test_runner(void*);

int main() {
  size_t i;
  int ret;
  pthread_t threads[THREAD_COUNT];

  for (i = 0; i < THREAD_COUNT; i += 1) {
    ret = pthread_create(&threads[i], NULL, test_runner, NULL);
    if (ret) {
      perror("ERROR creating pthread");
      return 1;
    }
  }

  for (i = 0; i < THREAD_COUNT; i += 1) {
    pthread_join(threads[i], NULL);
  }

  printf("\n");

  return 0;
}

void* test_runner(void* ptr) {
  size_t i;

  for (i = 0; i < testsLength; i += 2) {
    PgQueryParseResult result = pg_query_parse(tests[i]);

		if (result.error) {
			printf("%s\n", result.error->message);
		} else if (strcmp(result.parse_tree, tests[i + 1]) == 0) {
      printf(".");
    } else {
      printf("INVALID result for \"%s\"\nexpected: %s\nactual: %s\n", tests[i], tests[i + 1], result.parse_tree);
    }

    pg_query_free_parse_result(result);
  }

  return NULL;
}
