GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		cmddefs.go
		defs.go
		pkid.go
    )
ENDIF()
END()


RECURSE(
	calloc
	cformat
	cmerge
	decodecounter
	decodemeta
	encodecounter
	encodemeta
	pods
	rtcov
	slicereader
	slicewriter
	stringtab
	test
	uleb128
)
