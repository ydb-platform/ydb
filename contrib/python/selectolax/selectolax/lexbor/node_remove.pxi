
cdef lxb_dom_node_t * node_remove_deep(lxb_dom_node_t* root):
    cdef lxb_dom_node_t *tmp
    cdef lxb_dom_node_t *node = root

    while node != NULL:
        if node.first_child != NULL:
            node = node.first_child
        else:
            while node != root and node.next == NULL:
                tmp = node.parent
                lxb_dom_node_remove(node)
                node = tmp

            if node == root:
                lxb_dom_node_remove(node)
                break

            tmp = node.next
            lxb_dom_node_remove(node)
            node = tmp

    return NULL

cdef bint node_is_removed(lxb_dom_node_t* node):
    if node.parent == NULL and node.next == NULL \
       and node.prev == NULL:
        return 1
    return 0
