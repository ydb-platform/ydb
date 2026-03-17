#include <stan/lang/grammars/program_grammar_def.hpp>
#include <stan/lang/grammars/iterator_typedefs.hpp>

namespace stan {
  namespace lang {
    template struct program_grammar<pos_iterator_t>;
  }
}
