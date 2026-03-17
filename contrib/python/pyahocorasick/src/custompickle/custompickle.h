#pragma once

#include "../Automaton.h"

typedef struct AutomatonData {
    AutomatonKind   kind;
    KeysStore       store;
    KeyType         key_type;
    size_t          words_count;
    int             longest_word;
} AutomatonData;


typedef struct CustompickleHeader {
    char            magick[16]; // CUSTOMPICKLE_MAGICK
    AutomatonData   data;
} CustompickleHeader;


typedef struct CustompickleFooter {
    size_t          nodes_count;
    char            magick[16]; // CUSTOMPICKLE_MAGICK
} CustompickleFooter;


void custompickle_initialize_header(CustompickleHeader* header, Automaton* automaton);
void custompickle_initialize_footer(CustompickleFooter* footer, size_t nodescount);
int custompickle_validate_header(CustompickleHeader* header);
int custompickle_validate_footer(CustompickleFooter* footer);
