/*
    This is part of pyahocorasick Python module.

    AutomatonSearchIter const, struct & methods declarations.
    This class implements iterator walk over Aho-Corasick
    automaton. Object of this class is returned by 'iter' method
    of Automaton class.

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : BSD-3-Clause (see LICENSE)
*/
#ifndef ahocorasick_AutomatonSearchIter_h_included
#define ahocorasick_AutomatonSearchIter_h_included

#include "common.h"
#include "Automaton.h"

#ifdef VARIABLE_LEN_CHARCODES
typedef enum {
    pyaho_UCS2_Any,
    pyaho_UCS2_LowSurrogate
} UCS2ExpectedChar;
#endif

typedef struct AutomatonSearchIter {
    PyObject_HEAD

    Automaton*  automaton;
    int         version;    ///< automaton version
    struct Input input;     ///< input string
    TrieNode*   state;      ///< current state of automaton
    TrieNode*   output;     ///< current node, i.e. yielded value

    Py_ssize_t  index;      ///< current index in data
    Py_ssize_t  shift;      ///< shift + index => output index
    Py_ssize_t  end;        ///< end index
    bool        ignore_white_space; ///< ignore input string white spaces using iswspace() function
#ifdef VARIABLE_LEN_CHARCODES
    int         position;       ///< position in string
    UCS2ExpectedChar expected;
#endif
} AutomatonSearchIter;


static PyObject*
automaton_search_iter_new(
    Automaton* automaton,
    PyObject* object,
    int start,
    int end,
    bool ignore_white_space
);

#endif
