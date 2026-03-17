#ifndef STAN_IO_JSON_JSON_HANDLER_HPP
#define STAN_IO_JSON_JSON_HANDLER_HPP

#include <string>

namespace stan {

  namespace json {

    /**
     * Abstract base class for JSON handlers.  More efficient to just
     * implement directly and pass in as a template, but this version
     * is available for convenience.
     */
    class json_handler {
    public:
      json_handler() { }

      ~json_handler() { }

      /**
       * Handle the the start of the text.
       */
      virtual void start_text() { }

      /**
       * Handle the the end of the text.
       */
      virtual void end_text() { }

      /**
       * Handle the start of an array.
       */
      virtual void start_array() { }

      /**
       * Handle the end of an array.
       */
      virtual void end_array() { }

      /**
       * Handle the start of an object.
       */
      virtual void start_object() { }

      /**
       * Handle the end of an object.
       */
      virtual void end_object() { }

      /**
       * Handle the null literal value.
       */
      virtual void null() { }

      /**
       * Handle the boolean literal value of the specified polarity.
       *
       * @param p polarity of boolean
       */
      virtual void boolean(bool p) { }

      /**
       * Handle a the specified double-precision floating point
       * value.
       *
       * @param x Value to handle.
       */
      virtual void number_double(double x) { }

      /**
       * Handle a the specified long integer value.
       *
       * @param n Value to handle.
       */
      // NOLINTNEXTLINE(runtime/int)
      virtual void number_long(long n) { }

      /**
       * Handle a the specified unsigned long integer value.
       *
       * @param n Value to handle.
       */
      // NOLINTNEXTLINE(runtime/int)
      virtual void number_unsigned_long(unsigned long n) { }

      /**
       * Handle the specified string value.
       *
       * @param s String value to handle.
       */
      virtual void string(const std::string& s) { }

      /**
       * Handle the specified object key.
       *
       * @param s String object key to handle.
       */
      virtual void key(const std::string& s) { }
    };

  }
}

#endif
