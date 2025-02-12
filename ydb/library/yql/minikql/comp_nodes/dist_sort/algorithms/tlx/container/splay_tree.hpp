/*******************************************************************************
 * tlx/container/splay_tree.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CONTAINER_SPLAY_TREE_HEADER
#define TLX_CONTAINER_SPLAY_TREE_HEADER

#include <functional>
#include <memory>

namespace tlx {

//! \addtogroup tlx_container
//! \{

/******************************************************************************/
// splay -- free Splay Tree methods

/*
                An implementation of top-down splaying
                    D. Sleator <sleator@cs.cmu.edu>
                             March 1992

  "Splay trees", or "self-adjusting search trees" are a simple and efficient
  data structure for storing an ordered set.  The data structure consists of a
  binary tree, without parent pointers, and no additional fields.  It allows
  searching, insertion, deletion, deletemin, deletemax, splitting, joining, and
  many other operations, all with amortized logarithmic performance.  Since the
  trees adapt to the sequence of requests, their performance on real access
  patterns is typically even better.  Splay trees are described in a number of
  texts and papers [1,2,3,4,5].

  The code here is adapted from simple top-down splay, at the bottom of page 669
  of [3].  It can be obtained via anonymous ftp from spade.pc.cs.cmu.edu in
  directory /usr/sleator/public.

  The chief modification here is that the splay operation works even if the item
  being splayed is not in the tree, and even if the tree root of the tree is
  nullptr.  So the line:

                              t = splay(i, t);

  causes it to search for item with key i in the tree rooted at t.  If it's
  there, it is splayed to the root.  If it isn't there, then the node put at the
  root is the last one before nullptr that would have been reached in a normal
  binary search for i.  (It's a neighbor of i in the tree.)  This allows many
  other operations to be easily implemented, as shown below.

  [1] "Fundamentals of data structures in C", Horowitz, Sahni,
       and Anderson-Freed, Computer Science Press, pp 542-547.
  [2] "Data Structures and Their Algorithms", Lewis and Denenberg,
       Harper Collins, 1991, pp 243-251.
  [3] "Self-adjusting Binary Search Trees" Sleator and Tarjan,
       JACM Volume 32, No 3, July 1985, pp 652-686.
  [4] "Data Structure and Algorithm Analysis", Mark Weiss,
       Benjamin Cummins, 1992, pp 119-130.
  [5] "Data Structures, Algorithms, and Performance", Derick Wood,
       Addison-Wesley, 1993, pp 367-375.
*/

//! check the tree order, recursively calculate min and max elements
template <typename Tree, typename Compare>
bool splay_check(const Tree* t,
                 const Tree*& out_tmin, const Tree*& out_tmax,
                 const Compare& cmp) {
    if (t == nullptr) return true;

    const Tree* tmin = nullptr, * tmax = nullptr;
    if (!splay_check(t->left, out_tmin, tmax, cmp) ||
        !splay_check(t->right, tmin, out_tmax, cmp))
        return false;

    if (tmax && !cmp(tmax->key, t->key))
        return false;
    if (tmin && !cmp(t->key, tmin->key))
        return false;
    return true;
}

//! check the tree order
template <typename Tree, typename Compare>
bool splay_check(const Tree* t, const Compare& cmp) {
    if (t == nullptr) return true;
    const Tree* tmin = nullptr, * tmax = nullptr;
    return splay_check(t, tmin, tmax, cmp);
}

//! Splay using the key i (which may or may not be in the tree.)  The starting
//! root is t, and the tree used is defined by rat size fields are maintained.
template <typename Key, typename Tree, typename Compare>
Tree * splay(const Key& k, Tree* t, const Compare& cmp) {
    Tree* N_left = nullptr, * N_right = nullptr;
    Tree* l = nullptr, * r = nullptr;

    if (t == nullptr) return t;

    for ( ; ; ) {
        if (cmp(k, t->key)) {
            if (t->left == nullptr) break;

            if (cmp(k, t->left->key)) {
                // rotate right
                Tree* y = t->left;
                t->left = y->right;
                y->right = t;
                t = y;
                if (t->left == nullptr) break;
            }
            // link right
            (r ? r->left : N_left) = t;
            r = t;
            t = t->left;
        }
        else if (cmp(t->key, k)) {
            if (t->right == nullptr) break;

            if (cmp(t->right->key, k)) {
                // rotate left
                Tree* y = t->right;
                t->right = y->left;
                y->left = t;
                t = y;
                if (t->right == nullptr) break;
            }
            // link left
            (l ? l->right : N_right) = t;
            l = t;
            t = t->right;
        }
        else {
            break;
        }
    }

    (l ? l->right : N_right) = (r ? r->left : N_left) = nullptr;

    // assemble
    (l ? l->right : N_right) = t->left;
    (r ? r->left : N_left) = t->right;
    t->left = N_right;
    t->right = N_left;

    return t;
}

//! Insert key i into the tree t, if it is not already there.  Before calling
//! this method, one *MUST* call splay() to rotate the tree to the right
//! position. Return a pointer to the resulting tree.
template <typename Tree, typename Compare>
Tree * splay_insert(Tree* nn, Tree* t, const Compare& cmp) {
    if (t == nullptr) {
        nn->left = nn->right = nullptr;
    }
    else if (cmp(nn->key, t->key)) {
        nn->left = t->left;
        nn->right = t;
        t->left = nullptr;
    }
    else {
        nn->right = t->right;
        nn->left = t;
        t->right = nullptr;
    }
    return nn;
}

//! Erases i from the tree if it's there.  Return a pointer to the resulting
//! tree.
template <typename Key, typename Tree, typename Compare>
Tree * splay_erase(const Key& k, Tree*& t, const Compare& cmp) {
    if (t == nullptr) return nullptr;
    t = splay(k, t, cmp);
    // k == t->key ?
    if (!cmp(k, t->key) && !cmp(t->key, k)) {
        // found it
        Tree* r = t;
        if (t->left == nullptr) {
            t = t->right;
        }
        else {
            Tree* x = splay(k, t->left, cmp);
            x->right = t->right;
            t = x;
        }
        return r;
    }
    else {
        // it wasn't there
        return nullptr;
    }
}

//! traverse the tree in preorder (left, node, right)
template <typename Tree, typename Functor>
void splay_traverse_preorder(const Functor& f, const Tree* t) {
    if (t == nullptr) return;
    splay_traverse_preorder(f, t->left);
    f(t);
    splay_traverse_preorder(f, t->right);
}

//! traverse the tree in postorder (left, right, node)
template <typename Tree, typename Functor>
void splay_traverse_postorder(const Functor& f, Tree* t) {
    if (t == nullptr) return;
    splay_traverse_postorder(f, t->left);
    splay_traverse_postorder(f, t->right);
    f(t);
}

/******************************************************************************/
// Splay Tree

template <typename Key, typename Compare = std::less<Key>,
          bool Duplicates = false,
          typename Allocator = std::allocator<Key> >
class SplayTree
{
public:
    //! splay tree node, also seen as public iterator
    struct Node {
        Node* left = nullptr, * right = nullptr;
        Key key;
        explicit Node(const Key& k) : key(k) { }
    };

    explicit SplayTree(Allocator alloc = Allocator())
        : node_allocator_(alloc) { }

    explicit SplayTree(Compare cmp, Allocator alloc = Allocator())
        : cmp_(cmp), node_allocator_(alloc) { }

    ~SplayTree() {
        clear();
    }

    //! insert key into tree if it does not exist, returns true if inserted.
    bool insert(const Key& k) {
        if (root_ != nullptr) {
            root_ = splay(k, root_, cmp_);
            // k == t->key ?
            if (!Duplicates && !cmp_(k, root_->key) && !cmp_(root_->key, k)) {
                // it's already there
                return false;
            }
        }
        Node* nn = new (node_allocator_.allocate(1)) Node(k);
        root_ = splay_insert(nn, root_, cmp_);
        size_++;
        return true;
    }

    //! erase key from tree, return true if it existed.
    bool erase(const Key& k) {
        Node* out = splay_erase(k, root_, cmp_);
        if (!out) return false;
        delete_node(out);
        return true;
    }

    //! erase node from tree, return true if it existed.
    bool erase(const Node* n) {
        return erase(n->key);
    }

    //! free all nodes
    void clear() {
        splay_traverse_postorder([this](Node* n) { delete_node(n); }, root_);
    }

    //! check if key exists
    bool exists(const Key& k) {
        root_ = splay(k, root_, cmp_);
        return !cmp_(root_->key, k) && !cmp_(k, root_->key);
    }

    //! return number of items in tree
    size_t size() const {
        return size_;
    }

    //! return true if tree is empty
    bool empty() const {
        return size_ == 0;
    }

    //! find tree node containing key or return smallest key larger than k
    Node * find(const Key& k) {
        return (root_ = splay(k, root_, cmp_));
    }

    //! check the tree order
    bool check() const {
        return splay_check(root_, cmp_);
    }

    //! traverse the whole tree in preorder (key order)s
    template <typename Functor>
    void traverse_preorder(const Functor& f) const {
        splay_traverse_preorder([&f](const Node* n) { f(n->key); }, root_);
    }

private:
    //! root tree node
    Node* root_ = nullptr;
    //! number of items in tree container
    size_t size_ = 0;
    //! key comparator
    Compare cmp_;
    //! key allocator
    Allocator alloc_;

    //! node allocator
    typedef typename std::allocator_traits<Allocator>::template rebind_alloc<Node> node_alloc_type;

    //! node allocator
    node_alloc_type node_allocator_;

    //! delete node
    void delete_node(Node* n) {
        n->~Node();
        node_allocator_.deallocate(n, 1);
        size_--;
    }
};

template <typename Key, typename Compare = std::less<Key>,
          typename Allocator = std::allocator<Key> >
using splay_set = SplayTree<Key, Compare, /* Duplicates */ false, Allocator>;

template <typename Key, typename Compare = std::less<Key>,
          typename Allocator = std::allocator<Key> >
using splay_multiset =
    SplayTree<Key, Compare, /* Duplicates */ true, Allocator>;

//! \}

} // namespace tlx

#endif // !TLX_CONTAINER_SPLAY_TREE_HEADER

/******************************************************************************/
