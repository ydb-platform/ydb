#include <stdlib.h>

#include "../readstat.h"
#include "readstat_schema.h"
#include "readstat_copy.h"

void readstat_schema_free(readstat_schema_t *schema) {
    if (schema) {
        free(schema->entries);
        free(schema);
    }
}

readstat_schema_entry_t *readstat_schema_find_or_create_entry(readstat_schema_t *dct, const char *var_name) {
    readstat_schema_entry_t *entry = NULL;
    int i;
    /* linear search. this is shitty, but whatever */
    for (i=0; i<dct->entry_count; i++) {
        if (strcmp(dct->entries[i].variable.name, var_name) == 0) {
            entry = &dct->entries[i];
            break;
        }
    }
    if (!entry) {
        dct->entries = realloc(dct->entries, sizeof(readstat_schema_entry_t) * (dct->entry_count + 1));
        entry = &dct->entries[dct->entry_count];
        memset(entry, 0, sizeof(readstat_schema_entry_t));
        
        readstat_copy(entry->variable.name, sizeof(entry->variable.name), var_name, strlen(var_name));
        entry->decimal_separator = '.';
        entry->variable.index = dct->entry_count++;
    }
    return entry;
}

