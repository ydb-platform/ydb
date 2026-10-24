/* Built twice: libip_b.so (plain) and libip_bs.so (-Bsymbolic-non-weak-functions),
   with PFX = b / bs and VAL = "B" / "BS". */
#define CAT2(a, b) a##_##b
#define CAT(a, b) CAT2(a, b)
#define F(x) CAT(PFX, x)
const char *common_fn(void) { return VAL; }
__attribute__((weak)) const char *weak_fn(void) { return VAL; }
int common_var = 2;
const char *exe_over(void) { return VAL; }
__attribute__((visibility("protected"))) const char *prot_fn(void) { return VAL; }
const char *F(calls_common)(void) { return common_fn(); }
const char *F(calls_weak)(void) { return weak_fn(); }
int F(reads_var)(void) { return common_var; }
void *F(addr_common)(void) { return (void *)common_fn; }
const char *F(calls_exe_over)(void) { return exe_over(); }
const char *F(calls_prot)(void) { return prot_fn(); }
