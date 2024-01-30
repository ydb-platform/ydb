GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		go118.go
		idna10.0.0.go
		punycode.go
		tables15.0.0.go
		trie.go
		trie13.0.0.go
		trieval.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		go118.go
		idna10.0.0.go
		punycode.go
		tables15.0.0.go
		trie.go
		trie13.0.0.go
		trieval.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		go118.go
		idna10.0.0.go
		punycode.go
		tables15.0.0.go
		trie.go
		trie13.0.0.go
		trieval.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		go118.go
		idna10.0.0.go
		punycode.go
		tables15.0.0.go
		trie.go
		trie13.0.0.go
		trieval.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		go118.go
		idna10.0.0.go
		punycode.go
		tables15.0.0.go
		trie.go
		trie13.0.0.go
		trieval.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		go118.go
		idna10.0.0.go
		punycode.go
		tables15.0.0.go
		trie.go
		trie13.0.0.go
		trieval.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		go118.go
		idna10.0.0.go
		punycode.go
		tables15.0.0.go
		trie.go
		trie13.0.0.go
		trieval.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		go118.go
		idna10.0.0.go
		punycode.go
		tables15.0.0.go
		trie.go
		trie13.0.0.go
		trieval.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		go118.go
		idna10.0.0.go
		punycode.go
		tables15.0.0.go
		trie.go
		trie13.0.0.go
		trieval.go
    )
ENDIF()
END()
