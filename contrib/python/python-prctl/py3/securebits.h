/* Constants as defined in the kernel headers */
#define SECURE_NOROOT 0
#define SECURE_NOROOT_LOCKED 1
#define SECURE_NO_SETUID_FIXUP 2
#define SECURE_NO_SETUID_FIXUP_LOCKED 3
#define SECURE_KEEP_CAPS 4
#define SECURE_KEEP_CAPS_LOCKED 5

#define issecure_mask(X)	(1 << (X))
#define SECUREBITS_DEFAULT 0x00000000
#define SECBIT_NOROOT		(issecure_mask(SECURE_NOROOT))
#define SECBIT_NOROOT_LOCKED	(issecure_mask(SECURE_NOROOT_LOCKED))
#define SECBIT_NO_SETUID_FIXUP	(issecure_mask(SECURE_NO_SETUID_FIXUP))
#define SECBIT_NO_SETUID_FIXUP_LOCKED \
			(issecure_mask(SECURE_NO_SETUID_FIXUP_LOCKED))
#define SECBIT_KEEP_CAPS	(issecure_mask(SECURE_KEEP_CAPS))
#define SECBIT_KEEP_CAPS_LOCKED (issecure_mask(SECURE_KEEP_CAPS_LOCKED))
#define SECURE_ALL_BITS		(issecure_mask(SECURE_NOROOT) | \
				 issecure_mask(SECURE_NO_SETUID_FIXUP) | \
				 issecure_mask(SECURE_KEEP_CAPS))
#define SECURE_ALL_LOCKS	(SECURE_ALL_BITS << 1)
