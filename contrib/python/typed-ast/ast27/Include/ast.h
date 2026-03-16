#ifndef Ta27_AST_H
#define Ta27_AST_H
#ifdef __cplusplus
extern "C" {
#endif

mod_ty Ta27AST_FromNode(const node *, PyCompilerFlags *flags,
				  const char *, PyArena *);

#ifdef __cplusplus
}
#endif
#endif /* !Ta27_AST_H */
