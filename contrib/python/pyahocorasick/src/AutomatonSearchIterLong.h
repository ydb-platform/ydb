/*
    This is part of pyahocorasick Python module.
    
    AutomatonSearchIterLong const, struct & methods declarations.
    This class implements iterator walk over Aho-Corasick
    automaton. Object of this class is returnd by 'iter' method
    of Automaton class.

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    License   : 3-clauses BSD (see LICENSE)
*/
#ifndef ahocorasick_AutomatonSearchIterLong_h_included
#define ahocorasick_AutomatonSearchIterLong_h_included

#include "common.h"
#include "Automaton.h"

typedef struct AutomatonSearchIterLong {
    PyObject_HEAD

    Automaton*  automaton;
    int         version;    ///< automaton version
    PyObject*   object;     ///< unicode or buffer
    struct Input input;     ///< input string
    TrieNode*   state;      ///< current state of automaton
    TrieNode*   last_node;  ///< last node on trie path
    int         last_index;
    
    int         index;      ///< current index in data
    int         shift;      ///< shift + index => output index
    int         end;        ///< end index
} AutomatonSearchIterLong;


static PyObject*
automaton_search_iter_long_new(
    Automaton* automaton,
    PyObject* object,
    int start,
    int end
);

#endif
