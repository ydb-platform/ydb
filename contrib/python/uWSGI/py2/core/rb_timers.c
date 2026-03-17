#include <contrib/python/uWSGI/py2/config.h>
/*

        uWSGI rbtree implementation based on nginx

        Copyright (C) Igor Sysoev
        Copyright (C) Nginx, Inc.
        Copyright (C) Unbit S.a.s.

        The red-black tree code is based on the algorithm described in
        the "Introduction to Algorithms" by Cormen, Leiserson and Rivest.
*/

#include "uwsgi.h"

#define uwsgi_rbt_red(node)               ((node)->color = 1)
#define uwsgi_rbt_black(node)             ((node)->color = 0)
#define uwsgi_rbt_is_red(node)            ((node)->color)
#define uwsgi_rbt_is_black(node)          (!uwsgi_rbt_is_red(node))
#define uwsgi_rbt_copy_color(n1, n2)      (n1->color = n2->color)


struct uwsgi_rbtree *uwsgi_init_rb_timer() {

	struct uwsgi_rbtree *tree = uwsgi_calloc(sizeof(struct uwsgi_rbtree));
	struct uwsgi_rb_timer *sentinel = uwsgi_calloc(sizeof(struct uwsgi_rb_timer));
	// no need to set it black, calloc already did it
	//uwsgi_rbt_black(sentinel);
	tree->root = sentinel;
	tree->sentinel = sentinel;

	return tree;
}

struct uwsgi_rb_timer *uwsgi_min_rb_timer(struct uwsgi_rbtree *tree, struct uwsgi_rb_timer *node) {

	if (!node)
		node = tree->root;
	struct uwsgi_rb_timer *sentinel = tree->sentinel;

	if (tree->root == sentinel) return NULL;

	while (node->left != sentinel) {
		node = node->left;
	}

	return node;
}

static void uwsgi_rbtree_lr(struct uwsgi_rb_timer **root, struct uwsgi_rb_timer *sentinel, struct uwsgi_rb_timer *node) {

	struct uwsgi_rb_timer *temp;

	temp = node->right;
	node->right = temp->left;

	if (temp->left != sentinel) {
		temp->left->parent = node;
	}

	temp->parent = node->parent;

	if (node == *root) {
		*root = temp;

	}
	else if (node == node->parent->left) {
		node->parent->left = temp;

	}
	else {
		node->parent->right = temp;
	}

	temp->left = node;
	node->parent = temp;
}


static void uwsgi_rbtree_rr(struct uwsgi_rb_timer **root, struct uwsgi_rb_timer *sentinel, struct uwsgi_rb_timer *node) {

	struct uwsgi_rb_timer *temp;

	temp = node->left;
	node->left = temp->right;

	if (temp->right != sentinel) {
		temp->right->parent = node;
	}

	temp->parent = node->parent;

	if (node == *root) {
		*root = temp;

	}
	else if (node == node->parent->right) {
		node->parent->right = temp;

	}
	else {
		node->parent->left = temp;
	}

	temp->right = node;
	node->parent = temp;
}


static void uwsgi_rbt_add(struct uwsgi_rb_timer *temp, struct uwsgi_rb_timer *node, struct uwsgi_rb_timer *sentinel) {
        struct uwsgi_rb_timer **p;

        for (;;) {

                p = (node->value < temp->value) ? &temp->left : &temp->right;
                if (*p == sentinel)
                        break;
                temp = *p;
        }

        *p = node;
        node->parent = temp;
        node->left = sentinel;
        node->right = sentinel;
        uwsgi_rbt_red(node);
}


struct uwsgi_rb_timer *uwsgi_add_rb_timer(struct uwsgi_rbtree *tree, uint64_t value, void *data) {

	struct uwsgi_rb_timer *node = uwsgi_malloc(sizeof(struct uwsgi_rb_timer));
	struct uwsgi_rb_timer *new_node = node;
	node->value = value;
	node->data = data;

	struct uwsgi_rb_timer *temp = NULL;

	/* a binary tree insert */

	struct uwsgi_rb_timer **root = &tree->root;
	struct uwsgi_rb_timer *sentinel = tree->sentinel;

	if (*root == sentinel) {
		node->parent = NULL;
		node->left = sentinel;
		node->right = sentinel;
		uwsgi_rbt_black(node);
		*root = node;
		return new_node;
	}

	uwsgi_rbt_add(*root, node, sentinel);

	/* re-balance tree */

	while (node != *root && uwsgi_rbt_is_red(node->parent)) {

		if (node->parent == node->parent->parent->left) {
			temp = node->parent->parent->right;

			if (uwsgi_rbt_is_red(temp)) {
				uwsgi_rbt_black(node->parent);
				uwsgi_rbt_black(temp);
				uwsgi_rbt_red(node->parent->parent);
				node = node->parent->parent;

			}
			else {
				if (node == node->parent->right) {
					node = node->parent;
					uwsgi_rbtree_lr(root, sentinel, node);
				}

				uwsgi_rbt_black(node->parent);
				uwsgi_rbt_red(node->parent->parent);
				uwsgi_rbtree_rr(root, sentinel, node->parent->parent);
			}

		}
		else {
			temp = node->parent->parent->left;

			if (uwsgi_rbt_is_red(temp)) {
				uwsgi_rbt_black(node->parent);
				uwsgi_rbt_black(temp);
				uwsgi_rbt_red(node->parent->parent);
				node = node->parent->parent;

			}
			else {
				if (node == node->parent->left) {
					node = node->parent;
					uwsgi_rbtree_rr(root, sentinel, node);
				}

				uwsgi_rbt_black(node->parent);
				uwsgi_rbt_red(node->parent->parent);
				uwsgi_rbtree_lr(root, sentinel, node->parent->parent);
			}
		}
	}

	uwsgi_rbt_black(*root);

	return new_node;
}

void uwsgi_del_rb_timer(struct uwsgi_rbtree *tree, struct uwsgi_rb_timer *node) {
	uint8_t red;
	struct uwsgi_rb_timer **root, *sentinel, *subst, *temp, *w;

	/* a binary tree delete */

	root = &tree->root;
	sentinel = tree->sentinel;

	if (node->left == sentinel) {
		temp = node->right;
		subst = node;

	}
	else if (node->right == sentinel) {
		temp = node->left;
		subst = node;

	}
	else {
		subst = uwsgi_min_rb_timer(tree, node->right);

		if (subst->left != sentinel) {
			temp = subst->left;
		}
		else {
			temp = subst->right;
		}
	}

	if (subst == *root) {
		*root = temp;
		uwsgi_rbt_black(temp);
		return;
	}

	red = uwsgi_rbt_is_red(subst);

	if (subst == subst->parent->left) {
		subst->parent->left = temp;

	}
	else {
		subst->parent->right = temp;
	}

	if (subst == node) {

		temp->parent = subst->parent;

	}
	else {

		if (subst->parent == node) {
			temp->parent = subst;

		}
		else {
			temp->parent = subst->parent;
		}

		subst->left = node->left;
		subst->right = node->right;
		subst->parent = node->parent;
		uwsgi_rbt_copy_color(subst, node);

		if (node == *root) {
			*root = subst;

		}
		else {
			if (node == node->parent->left) {
				node->parent->left = subst;
			}
			else {
				node->parent->right = subst;
			}
		}

		if (subst->left != sentinel) {
			subst->left->parent = subst;
		}

		if (subst->right != sentinel) {
			subst->right->parent = subst;
		}
	}

	if (red) {
		return;
	}

	/* a delete fixup */

	while (temp != *root && uwsgi_rbt_is_black(temp)) {

		if (temp == temp->parent->left) {
			w = temp->parent->right;

			if (uwsgi_rbt_is_red(w)) {
				uwsgi_rbt_black(w);
				uwsgi_rbt_red(temp->parent);
				uwsgi_rbtree_lr(root, sentinel, temp->parent);
				w = temp->parent->right;
			}

			if (uwsgi_rbt_is_black(w->left) && uwsgi_rbt_is_black(w->right)) {
				uwsgi_rbt_red(w);
				temp = temp->parent;

			}
			else {
				if (uwsgi_rbt_is_black(w->right)) {
					uwsgi_rbt_black(w->left);
					uwsgi_rbt_red(w);
					uwsgi_rbtree_rr(root, sentinel, w);
					w = temp->parent->right;
				}

				uwsgi_rbt_copy_color(w, temp->parent);
				uwsgi_rbt_black(temp->parent);
				uwsgi_rbt_black(w->right);
				uwsgi_rbtree_lr(root, sentinel, temp->parent);
				temp = *root;
			}

		}
		else {
			w = temp->parent->left;

			if (uwsgi_rbt_is_red(w)) {
				uwsgi_rbt_black(w);
				uwsgi_rbt_red(temp->parent);
				uwsgi_rbtree_rr(root, sentinel, temp->parent);
				w = temp->parent->left;
			}

			if (uwsgi_rbt_is_black(w->left) && uwsgi_rbt_is_black(w->right)) {
				uwsgi_rbt_red(w);
				temp = temp->parent;

			}
			else {
				if (uwsgi_rbt_is_black(w->left)) {
					uwsgi_rbt_black(w->right);
					uwsgi_rbt_red(w);
					uwsgi_rbtree_lr(root, sentinel, w);
					w = temp->parent->left;
				}

				uwsgi_rbt_copy_color(w, temp->parent);
				uwsgi_rbt_black(temp->parent);
				uwsgi_rbt_black(w->left);
				uwsgi_rbtree_rr(root, sentinel, temp->parent);
				temp = *root;
			}
		}
	}

	uwsgi_rbt_black(temp);
}
