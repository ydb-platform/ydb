#include <stan/lang/grammars/whitespace_grammar_def.hpp>
#include <stan/lang/grammars/iterator_typedefs.hpp>

namespace stan {
  namespace lang {
    template struct whitespace_grammar<pos_iterator_t>;
  }
}
