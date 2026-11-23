use crate::traits::{Consumer, Producer};

/// Split the ring buffer onto producer and consumer.
pub trait Split {
    /// Producer type.
    type Prod: Producer;
    /// Consumer type.
    type Cons: Consumer;

    /// Perform splitting.
    fn split(self) -> (Self::Prod, Self::Cons);
}

/// Split the ring buffer by reference onto producer and consumer.
pub trait SplitRef {
    /// Ref producer type.
    type RefProd<'a>: Producer + 'a
    where
        Self: 'a;
    /// Ref consumer type.
    type RefCons<'a>: Consumer + 'a
    where
        Self: 'a;

    /// Perform splitting by reference.
    fn split_ref(&mut self) -> (Self::RefProd<'_>, Self::RefCons<'_>);
}
