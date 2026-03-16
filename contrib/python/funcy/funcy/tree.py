from collections import deque
from .types import is_seqcont


__all__ = ['tree_leaves', 'ltree_leaves', 'tree_nodes', 'ltree_nodes']


def tree_leaves(root, follow=is_seqcont, children=iter):
    """Iterates over tree leaves."""
    q = deque([[root]])
    while q:
        node_iter = iter(q.pop())
        for sub in node_iter:
            if follow(sub):
                q.append(node_iter)
                q.append(children(sub))
                break
            else:
                yield sub

def ltree_leaves(root, follow=is_seqcont, children=iter):
    """Lists tree leaves."""
    return list(tree_leaves(root, follow, children))


def tree_nodes(root, follow=is_seqcont, children=iter):
    """Iterates over all tree nodes."""
    q = deque([[root]])
    while q:
        node_iter = iter(q.pop())
        for sub in node_iter:
            yield sub
            if follow(sub):
                q.append(node_iter)
                q.append(children(sub))
                break

def ltree_nodes(root, follow=is_seqcont, children=iter):
    """Lists all tree nodes."""
    return list(tree_nodes(root, follow, children))
