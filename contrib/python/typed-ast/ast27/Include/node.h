
/* Parse tree node interface */

#ifndef Ta27_NODE_H
#define Ta27_NODE_H
#ifdef __cplusplus
extern "C" {
#endif

typedef struct _node {
    short		n_type;
    char		*n_str;
    int			n_lineno;
    int			n_col_offset;
    int			n_nchildren;
    struct _node	*n_child;
} node;

node *Ta27Node_New(int type);
int Ta27Node_AddChild(node *n, int type,
                      char *str, int lineno, int col_offset);
void Ta27Node_Free(node *n);
Py_ssize_t _Ta27Node_SizeOf(node *n);

/* Node access functions */
#define NCH(n)		((n)->n_nchildren)
	
#define CHILD(n, i)	(&(n)->n_child[i])
#define RCHILD(n, i)	(CHILD(n, NCH(n) + i))
#define TYPE(n)		((n)->n_type)
#define STR(n)		((n)->n_str)

/* Assert that the type of a node is what we expect */
#define REQ(n, type) assert(TYPE(n) == (type))

PyAPI_FUNC(void) PyNode_ListTree(node *);

#ifdef __cplusplus
}
#endif
#endif /* !Ta27_NODE_H */
