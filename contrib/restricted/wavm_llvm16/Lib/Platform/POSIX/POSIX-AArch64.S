#ifdef __APPLE__
	#define C_NAME(name) _##name
	#define C_NAME_PLT(name) _##name
	#define TYPE(name,typeSpec) 
	#define SIZE(name)
#else
	#define C_NAME(name) name
	#define C_NAME_PLT(name) name@PLT
	#define TYPE(name,typeSpec) .type name, typeSpec;
	#define SIZE(name) .size name, .-name;
#endif

#define BEGIN_FUNC(name,...) \
	.globl C_NAME(name); \
	TYPE(C_NAME(name),@function) \
	.align 16; \
	C_NAME(name):; \
	.cfi_startproc __VA_ARGS__;

#define END_FUNC(name) \
	.cfi_endproc; \
	SIZE(C_NAME(name))

/* If this flag isn't present, the linker will assume this object needs an executable stack. */
#if defined(__linux__) && defined(__ELF__)
.section .note.GNU-stack, "", %progbits
#endif

.text

#ifdef __APPLE__
	.globl _wavm_probe_stack
	.align 16
_wavm_probe_stack:
	.cfi_startproc
		br x30
	.cfi_endproc
#else
BEGIN_FUNC(wavm_probe_stack)
	br x30
END_FUNC(wavm_probe_stack)
#endif
