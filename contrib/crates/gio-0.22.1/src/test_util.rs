// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(test)]
use std::sync::mpsc::{Sender, channel};

#[cfg(test)]
use glib::{MainContext, MainLoop};

#[cfg(test)]
pub fn run_async<T: Send + 'static, Q: FnOnce(Sender<T>, MainLoop) + Send + 'static>(
    start: Q,
) -> T {
    let c = MainContext::new();
    let l = MainLoop::new(Some(&c), false);
    let l_clone = l.clone();

    let (tx, rx) = channel();

    c.spawn(async move {
        start(tx, l_clone);
    });

    l.run();

    rx.recv().unwrap()
}

#[cfg(test)]
pub fn run_async_local<T: 'static, Q: FnOnce(Sender<T>, MainLoop) + Send + 'static>(start: Q) -> T {
    let c = MainContext::new();
    let l = MainLoop::new(Some(&c), false);
    let l_clone = l.clone();

    let (tx, rx) = channel();

    c.spawn_local(async move {
        start(tx, l_clone);
    });

    l.run();

    rx.recv().unwrap()
}
