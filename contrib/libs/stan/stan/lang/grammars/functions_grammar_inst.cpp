#include <stan/lang/grammars/functions_grammar_def.hpp>
#include <stan/lang/grammars/iterator_typedefs.hpp>

namespace stan {
  namespace lang {
    template struct functions_grammar<pos_iterator_t>;
  }
}
