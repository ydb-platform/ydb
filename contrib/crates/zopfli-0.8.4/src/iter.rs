use core::iter::Peekable;

pub struct FlagLastIterator<I: Iterator> {
    inner: Peekable<I>,
}

impl<I: Iterator> Iterator for FlagLastIterator<I> {
    type Item = (I::Item, bool);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|val| (val, self.inner.peek().is_none()))
    }
}

pub trait ToFlagLastIterator: Iterator + Sized {
    fn flag_last(self) -> FlagLastIterator<Self> {
        FlagLastIterator {
            inner: self.peekable(),
        }
    }
}

impl<T: Iterator + Sized> ToFlagLastIterator for T {}
