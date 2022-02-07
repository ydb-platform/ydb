#include <pg_query.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "scan_tests.c"

#include "protobuf/pg_query.pb-c.h"

int main() {
  size_t i;
  size_t j;
  bool ret_code = 0;
  PgQuery__ScanResult *scan_result;
  PgQuery__ScanToken *scan_token;
  const ProtobufCEnumValue *token_kind;
  const ProtobufCEnumValue *keyword_kind;
  PgQueryScanResult result;

  for (i = 0; i < testsCount * 2; i += 2) {
    char buffer[1024];
    buffer[0] = '\0';

    result = pg_query_scan(tests[i]);

    if (result.error) {
      ret_code = -1;
      printf("%s\n", result.error->message);
    } else {
      scan_result = pg_query__scan_result__unpack(NULL, result.pbuf.len, (void*) result.pbuf.data);

      for (j = 0; j < scan_result->n_tokens; j++) {
        char buffer2[1024];
        scan_token = scan_result->tokens[j];
        token_kind = protobuf_c_enum_descriptor_get_value(&pg_query__token__descriptor, scan_token->token);
        keyword_kind = protobuf_c_enum_descriptor_get_value(&pg_query__keyword_kind__descriptor, scan_token->keyword_kind);
        sprintf(buffer2, "%.*s = %s, %s\n", scan_token->end - scan_token->start, &(tests[i][scan_token->start]), token_kind->name, keyword_kind->name);
        strcat(buffer, buffer2);
      }

      pg_query__scan_result__free_unpacked(scan_result, NULL);

      if (strcmp(buffer, tests[i + 1]) == 0) {
        printf(".");
      } else {
        ret_code = -1;
        printf("INVALID result for \"%s\"\nexpected:\n%s\nactual:\n%s\n", tests[i], tests[i + 1], buffer);
      }
    }

    pg_query_free_scan_result(result);
  }

  printf("\n");

  pg_query_exit();

  return ret_code;
}
