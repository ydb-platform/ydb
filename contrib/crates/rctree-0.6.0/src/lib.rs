/*!

*rctree* is a "DOM-like" tree implemented using reference counting.

"DOM-like" here means that data structures can be used to represent
the parsed content of an HTML or XML document,
like [*the* DOM](https://dom.spec.whatwg.org/) does,
but don't necessarily have the exact same API as the DOM.
That is:

* A tree is made up of nodes.
* Each node has zero or more *child* nodes, which are ordered.
* Each node has a no more than one *parent*, the node that it is a *child* of.
* A node without a *parent* is called a *root*.
* As a consequence, each node may also have *siblings*: its *parent*'s other *children*, if any.
* From any given node, access to its
  parent, previous sibling, next sibling, first child, and last child (if any)
  can take no more than *O(1)* time.
* Each node also has data associated to it,
  which for the purpose of this project is purely generic.
  For an HTML document, the data would be either the text of a text node,
  or the name and attributes of an element node.
* The tree is mutable:
  nodes (with their sub-trees) can be inserted or removed anywhere in the tree.

The lifetime of nodes is managed through *reference counting*.
To avoid reference cycles which would cause memory leaks, the tree is *asymmetric*:
each node holds optional *strong references* to its next sibling and first child,
but only optional *weak references* to its parent, previous sibling, and last child.

Nodes are destroyed as soon as there is no strong reference left to them.
The structure is such that holding a reference to the root
is sufficient to keep the entire tree alive.
However, if for example the only reference that exists from outside the tree
is one that you use to traverse it,
you will not be able to go back "up" the tree to ancestors and previous siblings after going "down",
as those nodes will have been destroyed.

Weak references to destroyed nodes are treated as if they were not set at all.
(E.g. a node can become a root when its parent is destroyed.)

Since nodes are *aliased* (have multiple references to them),
[`RefCell`](http://doc.rust-lang.org/std/cell/index.html) is used for interior mutability.

Advantages:

* A single `Node` user-visible type to manipulate the tree, with methods.
* Memory is freed as soon as it becomes unused (if parts of the tree are removed).

Disadvantages:

* The tree can only be accessed from the thread is was created in.
* Any tree manipulation, including read-only traversals,
  requires incrementing and decrementing reference counts,
  which causes run-time overhead.
* Nodes are allocated individually, which may cause memory fragmentation and hurt performance.

*/

#![doc(html_root_url = "https://docs.rs/rctree/0.4.0")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::cell::{Ref, RefCell, RefMut};
use std::fmt;
use std::rc::{Rc, Weak};

type Link<T> = Rc<RefCell<NodeData<T>>>;
type WeakLink<T> = Weak<RefCell<NodeData<T>>>;

/// A reference to a node holding a value of type `T`. Nodes form a tree.
///
/// Internally, this uses reference counting for lifetime tracking
/// and `std::cell::RefCell` for interior mutability.
///
/// **Note:** Cloning a `Node` only increments a reference count. It does not copy the data.
pub struct Node<T>(Link<T>);

/// A weak reference to a node holding a value of type `T`.
pub struct WeakNode<T>(WeakLink<T>);

struct NodeData<T> {
    parent: Option<WeakLink<T>>,
    first_child: Option<Link<T>>,
    last_child: Option<WeakLink<T>>,
    previous_sibling: Option<WeakLink<T>>,
    next_sibling: Option<Link<T>>,
    data: T,
}

/// Cloning a `Node` only increments a reference count. It does not copy the data.
impl<T> Clone for Node<T> {
    fn clone(&self) -> Self {
        Node(Rc::clone(&self.0))
    }
}

impl<T> PartialEq for Node<T> {
    fn eq(&self, other: &Node<T>) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

impl<T: fmt::Debug> fmt::Debug for Node<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&*self.borrow(), f)
    }
}

impl<T: fmt::Display> fmt::Display for Node<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&*self.borrow(), f)
    }
}

impl<T> Node<T> {
    /// Creates a new node from its associated data.
    pub fn new(data: T) -> Node<T> {
        Node(Rc::new(RefCell::new(NodeData {
            parent: None,
            first_child: None,
            last_child: None,
            previous_sibling: None,
            next_sibling: None,
            data,
        })))
    }

    /// Returns a weak reference to a node.
    pub fn downgrade(&self) -> WeakNode<T> {
        WeakNode(Rc::downgrade(&self.0))
    }

    /// Returns a parent node, unless this node is the root of the tree.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently mutably borrowed.
    pub fn parent(&self) -> Option<Node<T>> {
        Some(Node(self.0.borrow().parent.as_ref()?.upgrade()?))
    }

    /// Returns a first child of this node, unless it has no child.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently mutably borrowed.
    pub fn first_child(&self) -> Option<Node<T>> {
        Some(Node(self.0.borrow().first_child.as_ref()?.clone()))
    }

    /// Returns a last child of this node, unless it has no child.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently mutably borrowed.
    pub fn last_child(&self) -> Option<Node<T>> {
        Some(Node(self.0.borrow().last_child.as_ref()?.upgrade()?))
    }

    /// Returns the previous sibling of this node, unless it is a first child.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently mutably borrowed.
    pub fn previous_sibling(&self) -> Option<Node<T>> {
        Some(Node(self.0.borrow().previous_sibling.as_ref()?.upgrade()?))
    }

    /// Returns the next sibling of this node, unless it is a last child.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently mutably borrowed.
    pub fn next_sibling(&self) -> Option<Node<T>> {
        Some(Node(self.0.borrow().next_sibling.as_ref()?.clone()))
    }

    /// Returns a shared reference to this node's data.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently mutably borrowed.
    pub fn borrow(&self) -> Ref<T> {
        Ref::map(self.0.borrow(), |v| &v.data)
    }

    /// Returns a unique/mutable reference to this node's data.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently borrowed.
    pub fn borrow_mut(&self) -> RefMut<T> {
        RefMut::map(self.0.borrow_mut(), |v| &mut v.data)
    }

    /// Returns an iterator of nodes to this node and its ancestors.
    ///
    /// Includes the current node.
    pub fn ancestors(&self) -> Ancestors<T> {
        Ancestors(Some(self.clone()))
    }

    /// Returns an iterator of nodes to this node and the siblings before it.
    ///
    /// Includes the current node.
    pub fn preceding_siblings(&self) -> PrecedingSiblings<T> {
        PrecedingSiblings(Some(self.clone()))
    }

    /// Returns an iterator of nodes to this node and the siblings after it.
    ///
    /// Includes the current node.
    pub fn following_siblings(&self) -> FollowingSiblings<T> {
        FollowingSiblings(Some(self.clone()))
    }

    /// Returns an iterator of nodes to this node's children.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently mutably borrowed.
    pub fn children(&self) -> Children<T> {
        Children {
            next: self.first_child(),
            next_back: self.last_child(),
        }
    }

    /// Returns `true` if this node has children nodes.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently mutably borrowed.
    pub fn has_children(&self) -> bool {
        self.first_child().is_some()
    }

    /// Returns an iterator of nodes to this node and its descendants, in tree order.
    ///
    /// Includes the current node.
    pub fn descendants(&self) -> Descendants<T> {
        Descendants(self.traverse())
    }

    /// Returns an iterator of nodes to this node and its descendants, in tree order.
    pub fn traverse(&self) -> Traverse<T> {
        Traverse {
            root: self.clone(),
            next: Some(NodeEdge::Start(self.clone())),
            next_back: Some(NodeEdge::End(self.clone())),
        }
    }

    /// Detaches a node from its parent and siblings. Children are not affected.
    ///
    /// # Panics
    ///
    /// Panics if the node or one of its adjoining nodes is currently borrowed.
    pub fn detach(&self) {
        self.0.borrow_mut().detach();
    }

    /// Appends a new child to this node, after existing children.
    ///
    /// # Panics
    ///
    /// Panics if the node, the new child, or one of their adjoining nodes is currently borrowed.
    pub fn append(&self, new_child: Node<T>) {
        assert!(*self != new_child, "a node cannot be appended to itself");

        let mut self_borrow = self.0.borrow_mut();
        let mut last_child_opt = None;
        {
            let mut new_child_borrow = new_child.0.borrow_mut();
            new_child_borrow.detach();
            new_child_borrow.parent = Some(Rc::downgrade(&self.0));
            if let Some(last_child_weak) = self_borrow.last_child.take() {
                if let Some(last_child_strong) = last_child_weak.upgrade() {
                    new_child_borrow.previous_sibling = Some(last_child_weak);
                    last_child_opt = Some(last_child_strong);
                }
            }
            self_borrow.last_child = Some(Rc::downgrade(&new_child.0));
        }

        if let Some(last_child_strong) = last_child_opt {
            let mut last_child_borrow = last_child_strong.borrow_mut();
            debug_assert!(last_child_borrow.next_sibling.is_none());
            last_child_borrow.next_sibling = Some(new_child.0);
        } else {
            // No last child
            debug_assert!(self_borrow.first_child.is_none());
            self_borrow.first_child = Some(new_child.0);
        }
    }

    /// Prepends a new child to this node, before existing children.
    ///
    /// # Panics
    ///
    /// Panics if the node, the new child, or one of their adjoining nodes is currently borrowed.
    pub fn prepend(&self, new_child: Node<T>) {
        assert!(*self != new_child, "a node cannot be prepended to itself");

        let mut self_borrow = self.0.borrow_mut();
        {
            let mut new_child_borrow = new_child.0.borrow_mut();
            new_child_borrow.detach();
            new_child_borrow.parent = Some(Rc::downgrade(&self.0));
            match self_borrow.first_child.take() {
                Some(first_child_strong) => {
                    {
                        let mut first_child_borrow = first_child_strong.borrow_mut();
                        debug_assert!(first_child_borrow.previous_sibling.is_none());
                        first_child_borrow.previous_sibling = Some(Rc::downgrade(&new_child.0));
                    }
                    new_child_borrow.next_sibling = Some(first_child_strong);
                }
                None => {
                    debug_assert!(self_borrow.first_child.is_none());
                    self_borrow.last_child = Some(Rc::downgrade(&new_child.0));
                }
            }
        }
        self_borrow.first_child = Some(new_child.0);
    }

    /// Inserts a new sibling after this node.
    ///
    /// # Panics
    ///
    /// Panics if the node, the new sibling, or one of their adjoining nodes is currently borrowed.
    pub fn insert_after(&self, new_sibling: Node<T>) {
        assert!(
            *self != new_sibling,
            "a node cannot be inserted after itself"
        );

        let mut self_borrow = self.0.borrow_mut();
        {
            let mut new_sibling_borrow = new_sibling.0.borrow_mut();
            new_sibling_borrow.detach();
            new_sibling_borrow.parent = self_borrow.parent.clone();
            new_sibling_borrow.previous_sibling = Some(Rc::downgrade(&self.0));
            match self_borrow.next_sibling.take() {
                Some(next_sibling_strong) => {
                    {
                        let mut next_sibling_borrow = next_sibling_strong.borrow_mut();
                        debug_assert!({
                            let weak = next_sibling_borrow.previous_sibling.as_ref().unwrap();
                            Rc::ptr_eq(&weak.upgrade().unwrap(), &self.0)
                        });
                        next_sibling_borrow.previous_sibling = Some(Rc::downgrade(&new_sibling.0));
                    }
                    new_sibling_borrow.next_sibling = Some(next_sibling_strong);
                }
                None => {
                    if let Some(parent_ref) = self_borrow.parent.as_ref() {
                        if let Some(parent_strong) = parent_ref.upgrade() {
                            let mut parent_borrow = parent_strong.borrow_mut();
                            parent_borrow.last_child = Some(Rc::downgrade(&new_sibling.0));
                        }
                    }
                }
            }
        }
        self_borrow.next_sibling = Some(new_sibling.0);
    }

    /// Inserts a new sibling before this node.
    ///
    /// # Panics
    ///
    /// Panics if the node, the new sibling, or one of their adjoining nodes is currently borrowed.
    pub fn insert_before(&self, new_sibling: Node<T>) {
        assert!(
            *self != new_sibling,
            "a node cannot be inserted before itself"
        );

        let mut self_borrow = self.0.borrow_mut();
        let mut previous_sibling_opt = None;
        {
            let mut new_sibling_borrow = new_sibling.0.borrow_mut();
            new_sibling_borrow.detach();
            new_sibling_borrow.parent = self_borrow.parent.clone();
            new_sibling_borrow.next_sibling = Some(self.0.clone());
            if let Some(previous_sibling_weak) = self_borrow.previous_sibling.take() {
                if let Some(previous_sibling_strong) = previous_sibling_weak.upgrade() {
                    new_sibling_borrow.previous_sibling = Some(previous_sibling_weak);
                    previous_sibling_opt = Some(previous_sibling_strong);
                }
            }
            self_borrow.previous_sibling = Some(Rc::downgrade(&new_sibling.0));
        }

        if let Some(previous_sibling_strong) = previous_sibling_opt {
            let mut previous_sibling_borrow = previous_sibling_strong.borrow_mut();
            debug_assert!({
                let rc = previous_sibling_borrow.next_sibling.as_ref().unwrap();
                Rc::ptr_eq(rc, &self.0)
            });
            previous_sibling_borrow.next_sibling = Some(new_sibling.0);
        } else {
            // No previous sibling.
            if let Some(parent_ref) = self_borrow.parent.as_ref() {
                if let Some(parent_strong) = parent_ref.upgrade() {
                    let mut parent_borrow = parent_strong.borrow_mut();
                    parent_borrow.first_child = Some(new_sibling.0);
                }
            }
        }
    }

    /// Returns a copy of a current node without children.
    ///
    /// # Panics
    ///
    /// Panics if the node is currently mutably borrowed.
    pub fn make_copy(&self) -> Node<T>
    where
        T: Clone,
    {
        Node::new(self.borrow().clone())
    }

    /// Returns a copy of a current node with children.
    ///
    /// # Panics
    ///
    /// Panics if any of the descendant nodes are currently mutably borrowed.
    pub fn make_deep_copy(&self) -> Node<T>
    where
        T: Clone,
    {
        let mut root = self.make_copy();
        Node::_make_deep_copy(&mut root, self);
        root
    }

    fn _make_deep_copy(parent: &mut Node<T>, node: &Node<T>)
    where
        T: Clone,
    {
        for child in node.children() {
            let mut new_node = child.make_copy();
            parent.append(new_node.clone());

            if child.has_children() {
                Node::_make_deep_copy(&mut new_node, &child);
            }
        }
    }
}

/// Cloning a `WeakNode` only increments a reference count. It does not copy the data.
impl<T> Clone for WeakNode<T> {
    fn clone(&self) -> Self {
        WeakNode(Weak::clone(&self.0))
    }
}

impl<T: fmt::Debug> fmt::Debug for WeakNode<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("(WeakNode)")
    }
}

impl<T> WeakNode<T> {
    /// Attempts to upgrade the WeakNode to a Node.
    pub fn upgrade(&self) -> Option<Node<T>> {
        self.0.upgrade().map(Node)
    }
}

impl<T> NodeData<T> {
    /// Detaches a node from its parent and siblings. Children are not affected.
    fn detach(&mut self) {
        let parent_weak = self.parent.take();
        let previous_sibling_weak = self.previous_sibling.take();
        let next_sibling_strong = self.next_sibling.take();

        let previous_sibling_opt = previous_sibling_weak
            .as_ref()
            .and_then(|weak| weak.upgrade());

        if let Some(next_sibling_ref) = next_sibling_strong.as_ref() {
            let mut next_sibling_borrow = next_sibling_ref.borrow_mut();
            next_sibling_borrow.previous_sibling = previous_sibling_weak;
        } else if let Some(parent_ref) = parent_weak.as_ref() {
            if let Some(parent_strong) = parent_ref.upgrade() {
                let mut parent_borrow = parent_strong.borrow_mut();
                parent_borrow.last_child = previous_sibling_weak;
            }
        }

        if let Some(previous_sibling_strong) = previous_sibling_opt {
            let mut previous_sibling_borrow = previous_sibling_strong.borrow_mut();
            previous_sibling_borrow.next_sibling = next_sibling_strong;
        } else if let Some(parent_ref) = parent_weak.as_ref() {
            if let Some(parent_strong) = parent_ref.upgrade() {
                let mut parent_borrow = parent_strong.borrow_mut();
                parent_borrow.first_child = next_sibling_strong;
            }
        }
    }
}

impl<T> Drop for NodeData<T> {
    fn drop(&mut self) {
        // Detach all descendant nodes recursively to prevent a stack overflow.
        if let Some(child) = self.first_child.take() {
            let mut open_set = vec![child];

            while let Some(node) = open_set.pop() {
                let mut node_data = node.borrow_mut();
                if let Some(next_sibling) = node_data.next_sibling.as_ref() {
                    open_set.push(next_sibling.clone());
                }

                // Child nodes should be processed if and only if strong_count is one,
                // which means self is the only reference. Otherwise keep the subtree unchanged.
                if Rc::strong_count(&node) == 1 {
                    if let Some(first_child) = node_data.first_child.as_ref() {
                        open_set.push(first_child.clone());
                    }
                }

                node_data.detach();
            }
        }
    }
}

/// An iterator of nodes to the ancestors a given node.
pub struct Ancestors<T>(Option<Node<T>>);

impl<T> Iterator for Ancestors<T> {
    type Item = Node<T>;

    /// # Panics
    ///
    /// Panics if the node about to be yielded is currently mutably borrowed.
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.0.take()?;
        self.0 = node.parent();
        Some(node)
    }
}

/// An iterator of nodes to the siblings before a given node.
pub struct PrecedingSiblings<T>(Option<Node<T>>);

impl<T> Iterator for PrecedingSiblings<T> {
    type Item = Node<T>;

    /// # Panics
    ///
    /// Panics if the node about to be yielded is currently mutably borrowed.
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.0.take()?;
        self.0 = node.previous_sibling();
        Some(node)
    }
}

/// An iterator of nodes to the siblings after a given node.
pub struct FollowingSiblings<T>(Option<Node<T>>);

impl<T> Iterator for FollowingSiblings<T> {
    type Item = Node<T>;

    /// # Panics
    ///
    /// Panics if the node about to be yielded is currently mutably borrowed.
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.0.take()?;
        self.0 = node.next_sibling();
        Some(node)
    }
}

/// A double ended iterator of nodes to the children of a given node.
pub struct Children<T> {
    next: Option<Node<T>>,
    next_back: Option<Node<T>>,
}

impl<T> Children<T> {
    // true if self.next_back's next sibling is self.next
    fn finished(&self) -> bool {
        match self.next_back {
            Some(ref next_back) => next_back.next_sibling() == self.next,
            _ => true,
        }
    }
}

impl<T> Iterator for Children<T> {
    type Item = Node<T>;

    /// # Panics
    ///
    /// Panics if the node about to be yielded is currently mutably borrowed.
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished() {
            return None;
        }

        let node = self.next.take()?;
        self.next = node.next_sibling();
        Some(node)
    }
}

impl<T> DoubleEndedIterator for Children<T> {
    /// # Panics
    ///
    /// Panics if the node about to be yielded is currently mutably borrowed.
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.finished() {
            return None;
        }

        let node = self.next_back.take()?;
        self.next_back = node.previous_sibling();
        Some(node)
    }
}

/// An iterator of nodes to a given node and its descendants, in tree order.
pub struct Descendants<T>(Traverse<T>);

impl<T> Iterator for Descendants<T> {
    type Item = Node<T>;

    /// # Panics
    ///
    /// Panics if the node about to be yielded is currently mutably borrowed.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.0.next() {
                Some(NodeEdge::Start(node)) => return Some(node),
                Some(NodeEdge::End(_)) => {}
                None => return None,
            }
        }
    }
}

/// A node type during traverse.
#[derive(Clone, Debug)]
pub enum NodeEdge<T> {
    /// Indicates that start of a node that has children.
    /// Yielded by `Traverse::next` before the node's descendants.
    /// In HTML or XML, this corresponds to an opening tag like `<div>`
    Start(Node<T>),

    /// Indicates that end of a node that has children.
    /// Yielded by `Traverse::next` after the node's descendants.
    /// In HTML or XML, this corresponds to a closing tag like `</div>`
    End(Node<T>),
}

// Implement PartialEq manually, because we do not need to require T: PartialEq
impl<T> PartialEq for NodeEdge<T> {
    fn eq(&self, other: &NodeEdge<T>) -> bool {
        match (self, other) {
            (&NodeEdge::Start(ref n1), &NodeEdge::Start(ref n2)) => *n1 == *n2,
            (&NodeEdge::End(ref n1), &NodeEdge::End(ref n2)) => *n1 == *n2,
            _ => false,
        }
    }
}

impl<T> NodeEdge<T> {
    fn next_edge(&self, root: &Node<T>) -> Option<NodeEdge<T>> {
        match *self {
            NodeEdge::Start(ref node) => match node.first_child() {
                Some(first_child) => Some(NodeEdge::Start(first_child)),
                None => Some(NodeEdge::End(node.clone())),
            },
            NodeEdge::End(ref node) => {
                if *node == *root {
                    None
                } else {
                    match node.next_sibling() {
                        Some(next_sibling) => Some(NodeEdge::Start(next_sibling)),
                        // `node.parent()` here can only be `None`
                        // if the tree has been modified during iteration,
                        // but silently stopping iteration
                        // seems a more sensible behavior than panicking.
                        None => node.parent().map(NodeEdge::End),
                    }
                }
            }
        }
    }

    fn previous_edge(&self, root: &Node<T>) -> Option<NodeEdge<T>> {
        match *self {
            NodeEdge::End(ref node) => match node.last_child() {
                Some(last_child) => Some(NodeEdge::End(last_child)),
                None => Some(NodeEdge::Start(node.clone())),
            },
            NodeEdge::Start(ref node) => {
                if *node == *root {
                    None
                } else {
                    match node.previous_sibling() {
                        Some(previous_sibling) => Some(NodeEdge::End(previous_sibling)),
                        // `node.parent()` here can only be `None`
                        // if the tree has been modified during iteration,
                        // but silently stopping iteration
                        // seems a more sensible behavior than panicking.
                        None => node.parent().map(NodeEdge::Start),
                    }
                }
            }
        }
    }
}

/// A double ended iterator of nodes to a given node and its descendants,
/// in tree order.
pub struct Traverse<T> {
    root: Node<T>,
    next: Option<NodeEdge<T>>,
    next_back: Option<NodeEdge<T>>,
}

impl<T> Traverse<T> {
    // true if self.next_back's next edge is self.next
    fn finished(&self) -> bool {
        match self.next_back {
            Some(ref next_back) => next_back.next_edge(&self.root) == self.next,
            _ => true,
        }
    }
}

impl<T> Iterator for Traverse<T> {
    type Item = NodeEdge<T>;

    /// # Panics
    ///
    /// Panics if the node about to be yielded is currently mutably borrowed.
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished() {
            return None;
        }

        let node = self.next.take()?;
        self.next = node.next_edge(&self.root);
        Some(node)
    }
}

impl<T> DoubleEndedIterator for Traverse<T> {
    /// # Panics
    ///
    /// Panics if the node about to be yielded is currently mutably borrowed.
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.finished() {
            return None;
        }

        let node = self.next_back.take()?;
        self.next_back = node.previous_edge(&self.root);
        Some(node)
    }
}
