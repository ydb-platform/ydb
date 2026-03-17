#include <stan/lang/grammars/term_grammar_def.hpp>
#include <stan/lang/grammars/iterator_typedefs.hpp>

namespace stan {
  namespace lang {
    template struct term_grammar<pos_iterator_t>;
  }
}
