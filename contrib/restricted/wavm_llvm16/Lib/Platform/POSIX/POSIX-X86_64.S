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

BEGIN_FUNC(wavm_probe_stack)
	/* On entry, the number of bytes of stack being allocated is in %rax.
	   The allocation is relative to the caller's %rsp (i.e. %rsp+8).
	   The function is not allowed to clobber any registers, with the exception
	   of r11 as it's a scratch register in all calling conventions.
	   https://github.com/llvm/llvm-project/blob/7770f83d614c7216e4c1e83026955fbf73298f34/llvm/lib/Target/X86/X86FrameLowering.cpp#L784
	   */

	/* Negate the allocation size, initialize the probe offset to zero, and
	   jump to the first loop step. */
	neg %rax
	xor %r11, %r11
	jmp step

loop:
	/* The request to probe the stack is relative to %rsp in the caller, so
	   probe relative to %rsp+8 to cancel out the return address pushed by
	   calling wavm_probe_stack.
	   Use test to load from the address without clobbering a register with
	   the loaded value. */
	test %rsp, 8(%rsp,%r11)

step:
	/* Decrement the probe offset by 4096 until it is less than the negative
	   allocation size. */
	sub $4096, %r11

	cmp %rax, %r11
	jg loop         /* Note the reversed GAS argument order on the cmp, so this
	                   is actually jumping if %r11 >= %rax. */

	/* After probing to the greatest multiple of 4096 that is less than the
	   allocation size, do a final probe at the allocation size. */
	test %rsp, 8(%rsp, %rax)

	neg %rax
	ret

END_FUNC(wavm_probe_stack)
