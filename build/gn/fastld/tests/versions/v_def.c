/* Unversioned definition, earlier in scope than libc: must satisfy
   v_lib's reference to random@GLIBC_2.2.5. */
long random(void) { return 4242; }
