 
AM_CXXFLAGS = -Wall 
if ENABLE_DEBUG 
AM_CXXFLAGS += -DPIRE_DEBUG 
endif 
if ENABLE_CHECKED 
AM_CXXFLAGS += -DPIRE_CHECKED 
endif 
 
lib_LTLIBRARIES = libpire.la 
libpire_la_SOURCES = \ 
	align.h \ 
	any.h \ 
	classes.cpp \ 
	defs.h \ 
	determine.h \ 
	encoding.cpp \ 
	encoding.h \ 
	extra.h \ 
	fsm.cpp \ 
	fsm.h \ 
	fwd.h \ 
	glue.cpp \ 
	glue.h \ 
	minimize.h \
	half_final_fsm.cpp \
	half_final_fsm.h \
	partition.h \ 
	pire.h \ 
	re_lexer.cpp \ 
	re_lexer.h \ 
	run.h \ 
	scanner_io.cpp \ 
	vbitset.h \ 
	re_parser.ypp \ 
	scanners/half_final.h \
	scanners/loaded.h \ 
	scanners/multi.h \ 
	scanners/slow.h \ 
	scanners/simple.h \ 
	scanners/common.h \ 
	scanners/pair.h \ 
	stub/stl.h \ 
	stub/lexical_cast.h \ 
	stub/saveload.h \ 
	stub/singleton.h \ 
	stub/utf8.cpp \ 
	stub/utf8.h \ 
	stub/noncopyable.h \ 
	stub/codepage_h.h \ 
	stub/doccodes_h.h \ 
	stub/unidata_h.h \ 
	stub/unidata_cpp.h 
 
if ENABLE_EXTRA 
libpire_la_SOURCES += \ 
	extra/capture.cpp \ 
	extra/capture.h \ 
	extra/count.cpp \ 
	extra/count.h \ 
	extra/glyphs.cpp \ 
	extra/glyphs.h 
endif 
 
pire_hdrdir = $(includedir)/pire 
pire_hdr_HEADERS = \ 
	align.h \ 
	any.h \ 
	defs.h \ 
	determine.h \ 
	encoding.h \ 
	extra.h \ 
	fsm.h \ 
	fwd.h \ 
	glue.h \ 
	minimize.h \
	half_final_fsm.h \
	partition.h \ 
	pire.h \ 
	re_lexer.h \ 
	re_parser.h \ 
	run.h \ 
	static_assert.h \ 
	vbitset.h 
 
if ENABLE_EXTRA 
pire_extradir = $(includedir)/pire/extra 
pire_extra_HEADERS = \ 
	extra/capture.h \ 
	extra/count.h \ 
	extra/glyphs.h 
endif 
 
pire_scannersdir = $(includedir)/pire/scanners 
pire_scanners_HEADERS = \ 
	scanners/common.h \ 
	scanners/half_final.h \
	scanners/multi.h \ 
	scanners/slow.h \ 
	scanners/simple.h \ 
	scanners/loaded.h \ 
	scanners/pair.h 
 
pire_stubdir = $(includedir)/pire/stub 
pire_stub_HEADERS = \ 
	stub/stl.h \ 
	stub/defaults.h \ 
	stub/singleton.h \ 
	stub/saveload.h \ 
	stub/lexical_cast.h 
 
bin_PROGRAMS = pire_inline  
 
pire_inline_SOURCES = inline.lpp stub/hacks.h stub/memstreams.h 
pire_inline_LDADD = libpire.la 
 
BUILT_SOURCES = re_parser.h re_parser.cpp 
CLEANFILES    = re_parser.h re_parser.cpp 
 
AM_YFLAGS = -d 
 
