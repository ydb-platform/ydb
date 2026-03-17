#ifndef STAN_LANG_GENERATOR_VISGEN_HPP
#define STAN_LANG_GENERATOR_VISGEN_HPP

#include <stan/lang/ast.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Base class for variant type visitor that generates output by
     * writing to an output stream.
     */
    struct visgen {
      /**
       * Result type for visitor pattern; always void for generators.
       */
      typedef void result_type;

      /**
       * Construct a varint type visitor for generation to the
       * specified output stream, with indentation level zero.  The
       * specified output stream must remain in scope in order to use
       * this object.
       *
       * @param[in,out] o output stream to store by reference for generation
       */
      explicit visgen(std::ostream& o) : indent_(0), o_(o) { }

      /**
       * Construct a varint type visitor for generation to the
       * specified output stream at the specified indentation level.
       * The specified output stream must remain in scope in order to
       * use this object.
       *
       * @param[in] indent indentation level
       * @param[in,out] o output stream to store by reference for generation
       */
      explicit visgen(int indent, std::ostream& o) : indent_(indent), o_(o) { }

      /**
       * Base destructor does nothing.  Specialize in subclasses.
       */
      virtual ~visgen() { }

      /**
       * Indentation level.
       */
      int indent_;

      /**
       * Reference to output stream for generation.
       */
      std::ostream& o_;
    };



  }
}
#endif
