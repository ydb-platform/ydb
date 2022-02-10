root_dir := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
word-dot = $(word $2,$(subst ., ,$1))

TARGET = pg_query
ARLIB = lib$(TARGET).a
SOLIB = lib$(TARGET).so
PGDIR = $(root_dir)/tmp/postgres
PGDIRBZ2 = $(root_dir)/tmp/postgres.tar.bz2

PG_VERSION = 13.3
PG_VERSION_MAJOR = $(call word-dot,$(PG_VERSION),1)
PROTOC_VERSION = 3.14.0

VERSION = 2.1.0
VERSION_MAJOR = $(call word-dot,$(VERSION),1)
VERSION_MINOR = $(call word-dot,$(VERSION),2)
VERSION_PATCH = $(call word-dot,$(VERSION),3)

SONAME = $(SOLIB).$(shell printf '%02d%02d' $(PG_VERSION_MAJOR) $(VERSION_MAJOR)).$(VERSION_MINOR)
SOLIBVER = $(SONAME).$(VERSION_PATCH)

SRC_FILES := $(wildcard src/*.c src/postgres/*.c) vendor/protobuf-c/protobuf-c.c vendor/xxhash/xxhash.c protobuf/pg_query.pb-c.c
NOT_OBJ_FILES := src/pg_query_enum_defs.o src/pg_query_fingerprint_defs.o src/pg_query_fingerprint_conds.o src/pg_query_outfuncs_defs.o src/pg_query_outfuncs_conds.o src/pg_query_readfuncs_defs.o src/pg_query_readfuncs_conds.o src/postgres/guc-file.o src/postgres/scan.o src/pg_query_json_helper.o
OBJ_FILES := $(filter-out $(NOT_OBJ_FILES), $(SRC_FILES:.c=.o))

override CFLAGS += -g -I. -I./vendor -I./src/postgres/include -Wall -Wno-unused-function -Wno-unused-value -Wno-unused-variable -fno-strict-aliasing -fwrapv -fPIC

override PG_CONFIGURE_FLAGS += -q --without-readline --without-zlib

override TEST_CFLAGS += -I. -I./vendor -g
override TEST_LDFLAGS += -pthread

CFLAGS_OPT_LEVEL = -O3
ifeq ($(DEBUG),1)
	CFLAGS_OPT_LEVEL = -O0
endif
ifeq ($(VALGRIND),1)
	CFLAGS_OPT_LEVEL = -O0
endif
override CFLAGS += $(CFLAGS_OPT_LEVEL)

ifeq ($(DEBUG),1)
	# We always add -g, so this only has to enable assertion checking
	override CFLAGS += -D USE_ASSERT_CHECKING
endif
ifeq ($(VALGRIND),1)
	override CFLAGS += -DUSE_VALGRIND
	override TEST_CFLAGS += -DUSE_VALGRIND
endif

CLEANLIBS = $(ARLIB)
CLEANOBJS = $(OBJ_FILES)
CLEANFILES = $(PGDIRBZ2)

AR = ar rs
INSTALL = install
LN_S = ln -s
RM = rm -f
ECHO = echo

VALGRIND_MEMCHECK = valgrind --leak-check=full --gen-suppressions=all \
  --suppressions=test/valgrind.supp --time-stamp=yes \
  --error-markers=VALGRINDERROR-BEGIN,VALGRINDERROR-END \
  --log-file=test/valgrind.log --trace-children=yes --show-leak-kinds=all \
  --error-exitcode=1 --errors-for-leak-kinds=all

CC ?= cc

# Experimental use of Protobuf C++ library, primarily used to validate JSON output matches Protobuf JSON mapping
CXX_SRC_FILES := src/pg_query_outfuncs_protobuf_cpp.cc protobuf/pg_query.pb.cc
ifeq ($(USE_PROTOBUF_CPP),1)
	override CXXFLAGS += `pkg-config --cflags protobuf` -I. -I./src/postgres/include -DHAVE_PTHREAD -std=c++11 -Wall -Wno-unused-function -Wno-zero-length-array -Wno-c99-extensions -fwrapv -fPIC
	ifeq ($(DEBUG),1)
		override CXXFLAGS += -O0 -g
	else
		override CXXFLAGS += -O3 -g
	endif
	override TEST_LDFLAGS += `pkg-config --libs protobuf` -lstdc++

	# Don't use regular Protobuf-C or JSON implementation (instead implement the same methods using the C++ library)
	SRC_FILES := $(filter-out src/pg_query_outfuncs_json.c src/pg_query_outfuncs_protobuf.c, $(SRC_FILES))
	OBJ_FILES := $(filter-out $(NOT_OBJ_FILES), $(SRC_FILES:.c=.o)) $(CXX_SRC_FILES:.cc=.o)
else
	# Make sure we always clean C++ object files
	CLEANOBJS += $(CXX_SRC_FILES:.cc=.o)
endif

all: examples test build

build: $(ARLIB)

build_shared: $(SOLIB)

clean:
	-@ $(RM) $(CLEANLIBS) $(CLEANOBJS) $(CLEANFILES) $(EXAMPLES) $(TESTS)
	-@ $(RM) -rf {test,examples}/*.dSYM
	-@ $(RM) -r $(PGDIR) $(PGDIRBZ2)

.PHONY: all clean build build_shared extract_source examples test install

$(PGDIR):
	curl -o $(PGDIRBZ2) https://ftp.postgresql.org/pub/source/v$(PG_VERSION)/postgresql-$(PG_VERSION).tar.bz2
	tar -xjf $(PGDIRBZ2)
	mv $(root_dir)/postgresql-$(PG_VERSION) $(PGDIR)
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/01_parser_additional_param_ref_support.patch
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/02_parser_support_question_mark_as_param_ref.patch
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/03_lexer_track_yyllocend.patch
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/04_lexer_comments_as_tokens.patch
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/05_limit_option_enum_value_default.patch
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/06_alloc_set_delete_free_list.patch
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/07_plpgsql_start_finish_datums.patch
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/08_avoid_zero_length_delimiter_in_regression_tests.patch
	cd $(PGDIR); ./configure $(PG_CONFIGURE_FLAGS)
	cd $(PGDIR); rm src/pl/plpgsql/src/pl_gram.h
	cd $(PGDIR); make -C src/pl/plpgsql/src pl_gram.h
	cd $(PGDIR); make -C src/port pg_config_paths.h
	cd $(PGDIR); make -C src/backend generated-headers
	cd $(PGDIR); make -C src/backend parser-recursive # Triggers copying of includes to where they belong, as well as generating gram.c/scan.c

extract_source: $(PGDIR)
	-@ $(RM) -rf ./src/postgres/
	mkdir ./src/postgres
	mkdir ./src/postgres/include
	LIBCLANG=/Library/Developer/CommandLineTools/usr/lib/libclang.dylib ruby ./scripts/extract_source.rb $(PGDIR)/ ./src/postgres/
	cp $(PGDIR)/src/include/storage/dsm_impl.h ./src/postgres/include/storage
	cp $(PGDIR)/src/include/port/atomics/arch-arm.h ./src/postgres/include/port/atomics
	cp $(PGDIR)/src/include/port/atomics/arch-ppc.h ./src/postgres/include/port/atomics
	touch ./src/postgres/guc-file.c
	# This causes compatibility problems on some Linux distros, with "xlocale.h" not being available
	echo "#undef HAVE_LOCALE_T" >> ./src/postgres/include/pg_config.h
	echo "#undef LOCALE_T_IN_XLOCALE" >> ./src/postgres/include/pg_config.h
	echo "#undef WCSTOMBS_L_IN_XLOCALE" >> ./src/postgres/include/pg_config.h
	# Support 32-bit systems without reconfiguring
	echo "#undef PG_INT128_TYPE" >> ./src/postgres/include/pg_config.h
	# Support gcc earlier than 4.6.0 without reconfiguring
	echo "#undef HAVE__STATIC_ASSERT" >> ./src/postgres/include/pg_config.h
	# Avoid problems with static asserts
	echo "#undef StaticAssertDecl" >> ./src/postgres/include/c.h
	echo "#define StaticAssertDecl(condition, errmessage)" >> ./src/postgres/include/c.h
	# Avoid dependency on execinfo (requires extra library on musl-libc based systems)
	echo "#undef HAVE_EXECINFO_H" >> ./src/postgres/include/pg_config.h
	echo "#undef HAVE_BACKTRACE_SYMBOLS" >> ./src/postgres/include/pg_config.h
	# Avoid dependency on cpuid.h (only supported on x86 systems)
	echo "#undef HAVE__GET_CPUID" >> ./src/postgres/include/pg_config.h
	# Ensure we don't fail on systems that have strchrnul support (FreeBSD)
	echo "#ifdef __FreeBSD__" >> ./src/postgres/include/pg_config.h
	echo "#define HAVE_STRCHRNUL" >> ./src/postgres/include/pg_config.h
	echo "#endif" >> ./src/postgres/include/pg_config.h
	# Copy version information so its easily accessible
	sed -i "" '$(shell echo 's/\#define PG_MAJORVERSION .*/'`grep "\#define PG_MAJORVERSION " ./src/postgres/include/pg_config.h`'/')' pg_query.h
	sed -i "" '$(shell echo 's/\#define PG_VERSION .*/'`grep "\#define PG_VERSION " ./src/postgres/include/pg_config.h`'/')' pg_query.h
	sed -i "" '$(shell echo 's/\#define PG_VERSION_NUM .*/'`grep "\#define PG_VERSION_NUM " ./src/postgres/include/pg_config.h`'/')' pg_query.h
	# Copy regress SQL files so we can use them in tests
	rm -f ./test/sql/postgres_regress/*.sql
	cp $(PGDIR)/src/test/regress/sql/*.sql ./test/sql/postgres_regress/

.c.o:
	@$(ECHO) compiling $(<)
	@$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ -c $<

.cc.o:
	@$(ECHO) compiling $(<)
	@$(CXX) $(CXXFLAGS) -o $@ -c $<

$(ARLIB): $(OBJ_FILES) Makefile
	@$(AR) $@ $(OBJ_FILES)

$(SOLIB): $(OBJ_FILES) Makefile
	@$(CC) $(CFLAGS) -shared -Wl,-soname,$(SONAME) $(LDFLAGS) -o $@ $(OBJ_FILES) $(LIBS)

protobuf/pg_query.pb-c.c protobuf/pg_query.pb-c.h: protobuf/pg_query.proto
ifneq ($(shell which protoc-gen-c), )
	protoc --c_out=. protobuf/pg_query.proto
else
	@echo 'Warning: protoc-gen-c not found, skipping protocol buffer regeneration'
endif

src/pg_query_protobuf.c src/pg_query_scan.c: protobuf/pg_query.pb-c.h

# Only used when USE_PROTOBUF_CPP is used (experimental for testing only)
src/pg_query_outfuncs_protobuf_cpp.cc: protobuf/pg_query.pb.cc
protobuf/pg_query.pb.cc: protobuf/pg_query.proto
ifneq ($(shell protoc --version 2>/dev/null | cut -f2 -d" "), $(PROTOC_VERSION))
	$(error "ERROR - Wrong protobuf compiler version, need $(PROTOC_VERSION)")
endif
	protoc --cpp_out=. protobuf/pg_query.proto

EXAMPLES = examples/simple examples/scan examples/normalize examples/simple_error examples/normalize_error examples/simple_plpgsql
examples: $(EXAMPLES)
	examples/simple
	examples/scan
	examples/normalize
	examples/simple_error
	examples/normalize_error
	examples/simple_plpgsql

examples/simple: examples/simple.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ -g examples/simple.c $(ARLIB) $(TEST_LDFLAGS)

examples/scan: examples/scan.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ -g examples/scan.c $(ARLIB) $(TEST_LDFLAGS)

examples/normalize: examples/normalize.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ -g examples/normalize.c $(ARLIB) $(TEST_LDFLAGS)

examples/simple_error: examples/simple_error.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ -g examples/simple_error.c $(ARLIB) $(TEST_LDFLAGS)

examples/normalize_error: examples/normalize_error.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ -g examples/normalize_error.c $(ARLIB) $(TEST_LDFLAGS)

examples/simple_plpgsql: examples/simple_plpgsql.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ -g examples/simple_plpgsql.c $(ARLIB) $(TEST_LDFLAGS)

TESTS = test/complex test/concurrency test/deparse test/fingerprint test/normalize test/parse test/parse_protobuf test/parse_plpgsql test/scan test/split
test: $(TESTS)
ifeq ($(VALGRIND),1)
	$(VALGRIND_MEMCHECK) test/complex || (cat test/valgrind.log && false)
	$(VALGRIND_MEMCHECK) test/concurrency || (cat test/valgrind.log && false)
	$(VALGRIND_MEMCHECK) test/deparse || (cat test/valgrind.log && false)
	$(VALGRIND_MEMCHECK) test/fingerprint || (cat test/valgrind.log && false)
	$(VALGRIND_MEMCHECK) test/normalize || (cat test/valgrind.log && false)
	$(VALGRIND_MEMCHECK) test/parse || (cat test/valgrind.log && false)
	$(VALGRIND_MEMCHECK) test/parse_protobuf || (cat test/valgrind.log && false)
	$(VALGRIND_MEMCHECK) test/scan || (cat test/valgrind.log && false)
	$(VALGRIND_MEMCHECK) test/split || (cat test/valgrind.log && false)
	# Output-based tests
	$(VALGRIND_MEMCHECK) test/parse_plpgsql || (cat test/valgrind.log && false)
	diff -Naur test/plpgsql_samples.expected.json test/plpgsql_samples.actual.json
else
	test/complex
	test/concurrency
	test/deparse
	test/fingerprint
	test/normalize
	test/parse
	test/parse_protobuf
	test/scan
	test/split
	# Output-based tests
	test/parse_plpgsql
	diff -Naur test/plpgsql_samples.expected.json test/plpgsql_samples.actual.json
endif

test/complex: test/complex.c $(ARLIB)
	# We have "-Isrc/" because this test uses pg_query_fingerprint_with_opts
	$(CC) $(TEST_CFLAGS) -o $@ -Isrc/ test/complex.c $(ARLIB) $(TEST_LDFLAGS)

test/concurrency: test/concurrency.c test/parse_tests.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ test/concurrency.c $(ARLIB) $(TEST_LDFLAGS)

test/deparse: test/deparse.c test/deparse_tests.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ test/deparse.c $(ARLIB) $(TEST_LDFLAGS)

test/fingerprint: test/fingerprint.c test/fingerprint_tests.c $(ARLIB)
	# We have "-Isrc/" because this test uses pg_query_fingerprint_with_opts
	$(CC) $(TEST_CFLAGS) -o $@ -Isrc/ test/fingerprint.c $(ARLIB) $(TEST_LDFLAGS)

test/normalize: test/normalize.c test/normalize_tests.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ test/normalize.c $(ARLIB) $(TEST_LDFLAGS)

test/parse: test/parse.c test/parse_tests.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ test/parse.c $(ARLIB) $(TEST_LDFLAGS)

test/parse_plpgsql: test/parse_plpgsql.c test/parse_tests.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ test/parse_plpgsql.c $(ARLIB) $(TEST_LDFLAGS)

test/parse_protobuf: test/parse_protobuf.c test/parse_tests.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ test/parse_protobuf.c $(ARLIB) $(TEST_LDFLAGS)

test/scan: test/scan.c test/scan_tests.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ test/scan.c $(ARLIB) $(TEST_LDFLAGS)

test/split: test/split.c test/split_tests.c $(ARLIB)
	$(CC) $(TEST_CFLAGS) -o $@ test/split.c $(ARLIB) $(TEST_LDFLAGS)

prefix = /usr/local
libdir = $(prefix)/lib
includedir = $(prefix)/include

install: $(ARLIB) $(SOLIB)
	$(INSTALL) -d "$(DESTDIR)"$(libdir)
	$(INSTALL) -m 644 $(ARLIB) "$(DESTDIR)"$(libdir)/$(ARLIB)
	$(INSTALL) -m 755 $(SOLIB) "$(DESTDIR)"$(libdir)/$(SOLIBVER)
	$(LN_S) $(SOLIBVER) "$(DESTDIR)"$(libdir)/$(SONAME)
	$(LN_S) $(SOLIBVER) "$(DESTDIR)"$(libdir)/$(SOLIB)
	$(INSTALL) -d "$(DESTDIR)"$(includedir)/$(TARGET)
	$(INSTALL) -m 644 pg_query.h "$(DESTDIR)"$(includedir)/pg_query.h
	$(INSTALL) -m 644 protobuf/pg_query.proto "$(DESTDIR)"$(includedir)/$(TARGET)/pg_query.proto
