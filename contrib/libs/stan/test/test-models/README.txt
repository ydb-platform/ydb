good: parsed and code generated without a main, *included* (not
      linked) in some other tests, so putting a model here will
      not generate object code.
      All of these are parsed, code generated without a main,
      and compiled with a dummy mail with -fsyntax-only (so no
      object code).

bad: not parsed; put ill-formed models here to test error messages.

good-standalone-functions: same as good, but only for files to be 
      compiled as standalone functions.

included: Files used to test Stan's "#include" feature. These are
      almost entirely snippets that are meant to be included by other
      Stan files.

include_path_test: Files used to test Stanc's "--include_paths"
      option. By design, a file in this directory includes a file that
      is in a different directory, e.g. "included".
