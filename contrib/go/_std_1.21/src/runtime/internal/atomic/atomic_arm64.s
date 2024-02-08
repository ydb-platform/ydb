// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include "go_asm.h"
#include "textflag.h"

TEXT ·Casint32(SB), NOSPLIT, $0-17
	B	·Cas(SB)

TEXT ·Casint64(SB), NOSPLIT, $0-25
	B	·Cas64(SB)

TEXT ·Casuintptr(SB), NOSPLIT, $0-25
	B	·Cas64(SB)

TEXT ·CasRel(SB), NOSPLIT, $0-17
	B	·Cas(SB)

TEXT ·Loadint32(SB), NOSPLIT, $0-12
	B	·Load(SB)

TEXT ·Loadint64(SB), NOSPLIT, $0-16
	B	·Load64(SB)

TEXT ·Loaduintptr(SB), NOSPLIT, $0-16
	B	·Load64(SB)

TEXT ·Loaduint(SB), NOSPLIT, $0-16
	B	·Load64(SB)

TEXT ·Storeint32(SB), NOSPLIT, $0-12
	B	·Store(SB)

TEXT ·Storeint64(SB), NOSPLIT, $0-16
	B	·Store64(SB)

TEXT ·Storeuintptr(SB), NOSPLIT, $0-16
	B	·Store64(SB)

TEXT ·Xaddint32(SB), NOSPLIT, $0-20
	B	·Xadd(SB)

TEXT ·Xaddint64(SB), NOSPLIT, $0-24
	B	·Xadd64(SB)

TEXT ·Xadduintptr(SB), NOSPLIT, $0-24
	B	·Xadd64(SB)

TEXT ·Casp1(SB), NOSPLIT, $0-25
	B ·Cas64(SB)

// uint32 ·Load(uint32 volatile* addr)
TEXT ·Load(SB),NOSPLIT,$0-12
	MOVD	ptr+0(FP), R0
	LDARW	(R0), R0
	MOVW	R0, ret+8(FP)
	RET

// uint8 ·Load8(uint8 volatile* addr)
TEXT ·Load8(SB),NOSPLIT,$0-9
	MOVD	ptr+0(FP), R0
	LDARB	(R0), R0
	MOVB	R0, ret+8(FP)
	RET

// uint64 ·Load64(uint64 volatile* addr)
TEXT ·Load64(SB),NOSPLIT,$0-16
	MOVD	ptr+0(FP), R0
	LDAR	(R0), R0
	MOVD	R0, ret+8(FP)
	RET

// void *·Loadp(void *volatile *addr)
TEXT ·Loadp(SB),NOSPLIT,$0-16
	MOVD	ptr+0(FP), R0
	LDAR	(R0), R0
	MOVD	R0, ret+8(FP)
	RET

// uint32 ·LoadAcq(uint32 volatile* addr)
TEXT ·LoadAcq(SB),NOSPLIT,$0-12
	B	·Load(SB)

// uint64 ·LoadAcquintptr(uint64 volatile* addr)
TEXT ·LoadAcq64(SB),NOSPLIT,$0-16
	B	·Load64(SB)

// uintptr ·LoadAcq64(uintptr volatile* addr)
TEXT ·LoadAcquintptr(SB),NOSPLIT,$0-16
	B	·Load64(SB)

TEXT ·StorepNoWB(SB), NOSPLIT, $0-16
	B	·Store64(SB)

TEXT ·StoreRel(SB), NOSPLIT, $0-12
	B	·Store(SB)

TEXT ·StoreRel64(SB), NOSPLIT, $0-16
	B	·Store64(SB)

TEXT ·StoreReluintptr(SB), NOSPLIT, $0-16
	B	·Store64(SB)

TEXT ·Store(SB), NOSPLIT, $0-12
	MOVD	ptr+0(FP), R0
	MOVW	val+8(FP), R1
	STLRW	R1, (R0)
	RET

TEXT ·Store8(SB), NOSPLIT, $0-9
	MOVD	ptr+0(FP), R0
	MOVB	val+8(FP), R1
	STLRB	R1, (R0)
	RET

TEXT ·Store64(SB), NOSPLIT, $0-16
	MOVD	ptr+0(FP), R0
	MOVD	val+8(FP), R1
	STLR	R1, (R0)
	RET

// uint32 Xchg(ptr *uint32, new uint32)
// Atomically:
//	old := *ptr;
//	*ptr = new;
//	return old;
TEXT ·Xchg(SB), NOSPLIT, $0-20
	MOVD	ptr+0(FP), R0
	MOVW	new+8(FP), R1
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	SWPALW	R1, (R0), R2
	MOVW	R2, ret+16(FP)
	RET
load_store_loop:
	LDAXRW	(R0), R2
	STLXRW	R1, (R0), R3
	CBNZ	R3, load_store_loop
	MOVW	R2, ret+16(FP)
	RET

// uint64 Xchg64(ptr *uint64, new uint64)
// Atomically:
//	old := *ptr;
//	*ptr = new;
//	return old;
TEXT ·Xchg64(SB), NOSPLIT, $0-24
	MOVD	ptr+0(FP), R0
	MOVD	new+8(FP), R1
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	SWPALD	R1, (R0), R2
	MOVD	R2, ret+16(FP)
	RET
load_store_loop:
	LDAXR	(R0), R2
	STLXR	R1, (R0), R3
	CBNZ	R3, load_store_loop
	MOVD	R2, ret+16(FP)
	RET

// bool Cas(uint32 *ptr, uint32 old, uint32 new)
// Atomically:
//	if(*val == old){
//		*val = new;
//		return 1;
//	} else
//		return 0;
TEXT ·Cas(SB), NOSPLIT, $0-17
	MOVD	ptr+0(FP), R0
	MOVW	old+8(FP), R1
	MOVW	new+12(FP), R2
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	MOVD	R1, R3
	CASALW	R3, (R0), R2
	CMP 	R1, R3
	CSET	EQ, R0
	MOVB	R0, ret+16(FP)
	RET
load_store_loop:
	LDAXRW	(R0), R3
	CMPW	R1, R3
	BNE	ok
	STLXRW	R2, (R0), R3
	CBNZ	R3, load_store_loop
ok:
	CSET	EQ, R0
	MOVB	R0, ret+16(FP)
	RET

// bool ·Cas64(uint64 *ptr, uint64 old, uint64 new)
// Atomically:
//      if(*val == old){
//              *val = new;
//              return 1;
//      } else {
//              return 0;
//      }
TEXT ·Cas64(SB), NOSPLIT, $0-25
	MOVD	ptr+0(FP), R0
	MOVD	old+8(FP), R1
	MOVD	new+16(FP), R2
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	MOVD	R1, R3
	CASALD	R3, (R0), R2
	CMP 	R1, R3
	CSET	EQ, R0
	MOVB	R0, ret+24(FP)
	RET
load_store_loop:
	LDAXR	(R0), R3
	CMP	R1, R3
	BNE	ok
	STLXR	R2, (R0), R3
	CBNZ	R3, load_store_loop
ok:
	CSET	EQ, R0
	MOVB	R0, ret+24(FP)
	RET

// uint32 xadd(uint32 volatile *ptr, int32 delta)
// Atomically:
//      *val += delta;
//      return *val;
TEXT ·Xadd(SB), NOSPLIT, $0-20
	MOVD	ptr+0(FP), R0
	MOVW	delta+8(FP), R1
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	LDADDALW	R1, (R0), R2
	ADD 	R1, R2
	MOVW	R2, ret+16(FP)
	RET
load_store_loop:
	LDAXRW	(R0), R2
	ADDW	R2, R1, R2
	STLXRW	R2, (R0), R3
	CBNZ	R3, load_store_loop
	MOVW	R2, ret+16(FP)
	RET

// uint64 Xadd64(uint64 volatile *ptr, int64 delta)
// Atomically:
//      *val += delta;
//      return *val;
TEXT ·Xadd64(SB), NOSPLIT, $0-24
	MOVD	ptr+0(FP), R0
	MOVD	delta+8(FP), R1
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	LDADDALD	R1, (R0), R2
	ADD 	R1, R2
	MOVD	R2, ret+16(FP)
	RET
load_store_loop:
	LDAXR	(R0), R2
	ADD	R2, R1, R2
	STLXR	R2, (R0), R3
	CBNZ	R3, load_store_loop
	MOVD	R2, ret+16(FP)
	RET

TEXT ·Xchgint32(SB), NOSPLIT, $0-20
	B	·Xchg(SB)

TEXT ·Xchgint64(SB), NOSPLIT, $0-24
	B	·Xchg64(SB)

TEXT ·Xchguintptr(SB), NOSPLIT, $0-24
	B	·Xchg64(SB)

TEXT ·And8(SB), NOSPLIT, $0-9
	MOVD	ptr+0(FP), R0
	MOVB	val+8(FP), R1
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	MVN 	R1, R2
	LDCLRALB	R2, (R0), R3
	RET
load_store_loop:
	LDAXRB	(R0), R2
	AND	R1, R2
	STLXRB	R2, (R0), R3
	CBNZ	R3, load_store_loop
	RET

TEXT ·Or8(SB), NOSPLIT, $0-9
	MOVD	ptr+0(FP), R0
	MOVB	val+8(FP), R1
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	LDORALB	R1, (R0), R2
	RET
load_store_loop:
	LDAXRB	(R0), R2
	ORR	R1, R2
	STLXRB	R2, (R0), R3
	CBNZ	R3, load_store_loop
	RET

// func And(addr *uint32, v uint32)
TEXT ·And(SB), NOSPLIT, $0-12
	MOVD	ptr+0(FP), R0
	MOVW	val+8(FP), R1
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	MVN 	R1, R2
	LDCLRALW	R2, (R0), R3
	RET
load_store_loop:
	LDAXRW	(R0), R2
	AND	R1, R2
	STLXRW	R2, (R0), R3
	CBNZ	R3, load_store_loop
	RET

// func Or(addr *uint32, v uint32)
TEXT ·Or(SB), NOSPLIT, $0-12
	MOVD	ptr+0(FP), R0
	MOVW	val+8(FP), R1
	MOVBU	internal∕cpu·ARM64+const_offsetARM64HasATOMICS(SB), R4
	CBZ 	R4, load_store_loop
	LDORALW	R1, (R0), R2
	RET
load_store_loop:
	LDAXRW	(R0), R2
	ORR	R1, R2
	STLXRW	R2, (R0), R3
	CBNZ	R3, load_store_loop
	RET
