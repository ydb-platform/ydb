# Changelog

## [Unreleased]

## [2.5.0] - 2025-08-13
### Added
- Added new module Engine.
- Added benchmarks.
- Added LEXBOR_BUILD_WITH_MSAN definition for building with msan.
- CSS::Syntax: added UNICODE-RANGE token.
- Added link to external bindings for Julia.

### Fixed
- Core: Fix undefined behavior in function lexbor_str_append().
- Core: Fix undefined behavior in function lexbor_in_node_make().
- Core: fixed slow realloc for large strings.
- HTML: fixed error report for whitespace characters reference.
- HTML: fixed error report on self-closing tag parsing.
- HTML: fixed attribute cloning for bad HTML.
- HTML: fixed duplicate attributes in svg namespace.
- CSS::Syntax: fixed consume a numeric token.
- Style: fixed use-after-poison when the element is destroyed.
- URL: fixed hostname setter if port is specified.
- Selectors: fixed selector :has(). It didn't always work properly.

### Changed
- HTML: allowed `<hr>` to be used inside `<select>`.
- HTML: changing the serialization of attributes. The `<` and `>` characters will now be escaped in attributes.
- Unicode: significant reduction tables size.
- Encoding: size reduction and performance up of multibyte encodings.
- CSS::Syntax: code refactoring.
- Selectors: code refactoring. The approach to searching for nodes by selectors has been rewritten.

Special thanks for patches, fixes and hints: Michael Hatherly @MichaelHatherly,
Alex Peattie @alexpeattie, Niels Dossche @nielsdos, Máté Kocsis @kocsismate,
Sebastian Pipping @hartwork.

## [2.4.0] - 2024-11-12
### Added
- Core: improve performance using SWAR.
- URL: added APIs to modify the URL object.
- URL: support for cloning urls.
- URL: removing newlines and tabs before parsing.
- Test: added performance test for HTML.
- Test:KV: added escape special character \x.

### Fixed
- Core: fixed build perf without LEXBOR_WITH_PERF define.
- HTML: fixed fragment parsing for tags not in data state.
- HTML: fixed use-of-uninitialized-value in encoding function.
- HTML: NULL dereference in lxb_html_document_parse_fragment_chunk_begin().
- CSS: fixed use-of-uninitialized-value in parsing function.
- CSS: fixed parse '|=' attribute value matching for selectors.
- Selectors: fixed potential NULL pointer dereference.
- Selectors: fixed matching of anb-of selectors.
- URL: fixed buffer overflow for host parsing with base URL.
- URL: fixed remove leading and trailing spaces for broken UTF-8.
- URL: fixed memory leak if a URL contained \n\r\t.
- URL: fixed memory leak after destroy parser.
- Unicode: fixed incorrect order of verification in IDNA.
- Unicode: fixed a potential memory leak.
- Punycode: fixed potential memory leak.
- PunyCode: fixed use-of-uninitialized-value for large decode data.
- Encoding: fixed incorrect code point check for ISO 2022 JP.

### Changed
- Encoding: updated GB18030-2022 index.
- Encoding: reduce the size for static data.
- URL: performance improvement.

Special thanks for patches, fixes and hints: Niels Dossche @nielsdos,
Máté Kocsis @kocsismate, Sergey Fedorov @barracuda156, Peter Kokot @petk.

## [2.3.0] - 2023-08-17
### Added
- Added new module Unicode.
- Added new module Punycode.
- Added new module URL.
- Added Unicode IDNA processing.
- Added new tests.
- Core: new functions for data conversations.
- CSS: added initial properties.
- CSS: added more than 70 new properties for parsing.
- Encoding: added decode function for valid UTF-8.
- Grammar: added new grammars for testing CSS properties.
- Test: added fuzzer for CSS StyleSheet.

### Fixed
- Core: fixed test failure in Hash on 32-bit architectures. Thanks @nmeum.
- CSS: fixed a couple of crashes related to lack of variable validation.
- CSS: fixed use-after-poison for declarations.
- CSS: fixed offset for token End-Of-File.
- CSS: fixed Qualified Rule prelude offset.
- Various Cppcheck report fixes.

### Changed
- CSS: renamed LXB_CSS_SYNTAX_TOKEN__TERMINATED to LXB_CSS_SYNTAX_TOKEN__END.
- Removed deprecated function 'sprintf' for macOS.

## [2.2.0] - 2023-04-06
### Added
- Added clone functions for DOM/HTML nodes.
- CMake: fixed build for Windows.
- Support overriding default memory functions. (thanks @zyc9012)
- Parsing CSS StyleSheet. Styles, declarations, properties.
- HTML: added events (insert, remove, destroy) for elements.
- Added styles parsing inside the style tag.
- Added style recalculating for an element when it changes.
- Added Grammar for generate test for CSS Property.
- Added examples for Styles, CSS StyleSheet parsing.

### Fixed
- HTML: fixed text node serialization without parent.
- HTML: fixed finding/getting title tag for HTML namespace.
- HTML: fixed adding attributes for foreign elements.
- Fixed memory leak in examples and tests.
- Fixed memory leak for qualified name set.

### Changed
- Minimal CMake version 2.8.12.
- Completely changed approach to parsing CSS (selectors, properties, styles).
- Removed XCode project files.

## [2.1.0] - 2021-08-05
### Added
- CSS: parsing selectors.
- Selectors for find DOM/HTML nodes.
- Build: clang fuzzer support.

### Fixed
- Core: fixed includes in "core.h".
- DOM: fixed skip child nodes in simple walker.
- HTML: fixed the incorrect state of the switch for "pre", "listing", "textarea".
- HTML: fixed heap-buffer-overflow in active/open elements.

### Changed
- HTML: refactoring module for better performance.
- CSS: parsing api and token retrieval changed.

## [1.0.0] - 2020-03-13
### Added
- Core: added hash table implementation.
- Created public header file for all modules.

### Fixed
- HTML: memory leak of repeated parsing of document.
- NULL pointer use in lxb_dom_attr_compare().
- Symbols visibility for Windows.

### Changed
- DOM, HTML, Tag, NS: breaking API changes.
- DOM: node tag_id to local_name.
- DOM: attribute name now is uintptr_t. Reference to global unique naming.

## [0.4.0] - 2019-11-18
### Added
- Encoding module.
- Utils module.
- CMake option for build all modules separately.
- Examples for html tokenizer.
- HTML: prescan the byte stream to determine encoding function.
- Aliases for inline functions for use ABI of library.
- Support ASAN for memory pool.
- Core: added dup function for mraw.
- More statuses.
- Converting functions for string to number.

### Fixed
- Full path for cmake test command.
- HTML: fixed parse '<![[CDATA[' chunks.
- Use after free document element in fragment parse.
- HTML: fixed memory leak in tokenizer.
- HTML: fixed pointer offset for lxb_dom_node_text_content() function.
- HTML: fixed use-after-free after clearing a document.

### Changed
- Core: changed lexbor_str_length_set() function.

## [0.2.0] - 2019-03-12
### Added
- CSS:Syntax parser.
- Core: added convertation floating-point numbers from/to string.
- DOM: general implementation of the functional.

### Fixed
- HTML: fixed problem with serialize U+00A0 character. #22
- Fixed build with C++. #20

## [0.1.0] - 2018-11-30
### Added
- The Lexbor project.
- HTML Parser.
- HTML/DOM interfaces.
- Basic functions for DOM and HTML interfaces.
- Examples for HTML and DOM.
- Tests for Core module.
- Tests for HTML tokenizator and tree builder.
- Python scripts for generating static structures.
