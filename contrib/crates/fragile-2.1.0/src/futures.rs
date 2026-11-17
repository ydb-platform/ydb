use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{stack_token, Fragile, SemiSticky, Sticky};

impl<F: Future> Future for Fragile<F> {
    type Output = F::Output;

    #[track_caller]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|s| s.get_mut()) }.poll(cx)
    }
}

impl<F: Future> Future for Sticky<F> {
    type Output = F::Output;

    #[track_caller]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        stack_token!(tok);
        unsafe { Pin::new_unchecked(Sticky::get_mut(&mut self, tok)) }.poll(cx)
    }
}

impl<F: Future> Future for SemiSticky<F> {
    type Output = F::Output;

    #[track_caller]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        stack_token!(tok);
        unsafe { Pin::new_unchecked(SemiSticky::get_mut(&mut self, tok)) }.poll(cx)
    }
}

#[cfg(feature = "stream")]
mod stream {
    use super::*;
    use futures_core::Stream;

    impl<S: Stream> Stream for Fragile<S> {
        type Item = S::Item;

        #[track_caller]
        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            unsafe { self.map_unchecked_mut(|s| s.get_mut()) }.poll_next(cx)
        }

        #[inline]
        fn size_hint(&self) -> (usize, Option<usize>) {
            match self.try_get() {
                Ok(x) => x.size_hint(),
                Err(_) => (0, None),
            }
        }
    }

    impl<S: Stream> Stream for Sticky<S> {
        type Item = S::Item;

        #[track_caller]
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            stack_token!(tok);
            unsafe { Pin::new_unchecked(Sticky::get_mut(&mut self, tok)) }.poll_next(cx)
        }

        #[inline]
        fn size_hint(&self) -> (usize, Option<usize>) {
            stack_token!(tok);
            match Sticky::try_get(self, tok) {
                Ok(x) => x.size_hint(),
                Err(_) => (0, None),
            }
        }
    }

    impl<S: Stream> Stream for SemiSticky<S> {
        type Item = S::Item;

        #[track_caller]
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            stack_token!(tok);
            unsafe { Pin::new_unchecked(SemiSticky::get_mut(&mut self, tok)) }.poll_next(cx)
        }

        #[inline]
        fn size_hint(&self) -> (usize, Option<usize>) {
            stack_token!(tok);
            match SemiSticky::try_get(self, tok) {
                Ok(x) => x.size_hint(),
                Err(_) => (0, None),
            }
        }
    }

    #[test]
    fn test_stream() {
        use futures_executor as executor;
        use futures_util::{future, stream, StreamExt};
        let mut w1 = Fragile::new(stream::once(future::ready(42)));
        let mut w2 = Fragile::new(stream::once(future::ready(42)));
        assert_eq!(
            format!("{:?}", executor::block_on(w1.next())),
            format!("{:?}", executor::block_on(w2.next())),
        );

        let mut w1 = Sticky::new(stream::once(future::ready(42)));
        let mut w2 = Sticky::new(stream::once(future::ready(42)));
        assert_eq!(
            format!("{:?}", executor::block_on(w1.next())),
            format!("{:?}", executor::block_on(w2.next())),
        );

        let mut w1 = SemiSticky::new(stream::once(future::ready(42)));
        let mut w2 = SemiSticky::new(stream::once(future::ready(42)));
        assert_eq!(
            format!("{:?}", executor::block_on(w1.next())),
            format!("{:?}", executor::block_on(w2.next())),
        );
    }

    #[test]
    fn test_stream_panic() {
        use futures_executor as executor;
        use futures_util::{future, stream, StreamExt};

        let mut w = Fragile::new(stream::once(future::ready(42)));
        let t = std::thread::spawn(move || executor::block_on(w.next()));
        assert!(t.join().is_err());

        let mut w = Sticky::new(stream::once(future::ready(42)));
        let t = std::thread::spawn(move || executor::block_on(w.next()));
        assert!(t.join().is_err());

        let mut w = SemiSticky::new(stream::once(future::ready(42)));
        let t = std::thread::spawn(move || executor::block_on(w.next()));
        assert!(t.join().is_err());
    }
}

#[test]
fn test_future() {
    use futures_executor as executor;
    use futures_util::future;
    let w1 = Fragile::new(future::ready(42));
    let w2 = w1.clone();
    assert_eq!(
        format!("{:?}", executor::block_on(w1)),
        format!("{:?}", executor::block_on(w2)),
    );

    let w1 = Sticky::new(future::ready(42));
    let w2 = w1.clone();
    assert_eq!(
        format!("{:?}", executor::block_on(w1)),
        format!("{:?}", executor::block_on(w2)),
    );

    let w1 = SemiSticky::new(future::ready(42));
    let w2 = w1.clone();
    assert_eq!(
        format!("{:?}", executor::block_on(w1)),
        format!("{:?}", executor::block_on(w2)),
    );
}

#[test]
fn test_future_panic() {
    use futures_executor as executor;
    use futures_util::future;
    let w = Fragile::new(future::ready(42));
    let t = std::thread::spawn(move || executor::block_on(w));
    assert!(t.join().is_err());

    let w = Sticky::new(future::ready(42));
    let t = std::thread::spawn(move || executor::block_on(w));
    assert!(t.join().is_err());

    let w = SemiSticky::new(future::ready(42));
    let t = std::thread::spawn(move || executor::block_on(w));
    assert!(t.join().is_err());
}
