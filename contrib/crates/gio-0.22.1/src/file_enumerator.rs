// Take a look at the license at the top of the repository in the LICENSE file.

use crate::{FileEnumerator, FileInfo, prelude::*};
use futures_core::future::LocalBoxFuture;
use futures_util::FutureExt;
use glib::translate::{ToGlibPtr, from_glib, from_glib_full};
use std::{iter::FusedIterator, task::Poll};

impl Iterator for FileEnumerator {
    type Item = Result<FileInfo, glib::Error>;

    fn next(&mut self) -> Option<Result<FileInfo, glib::Error>> {
        match self.next_file(crate::Cancellable::NONE) {
            Err(err) => Some(Err(err)),
            Ok(file_info) => file_info.map(Ok),
        }
    }
}

impl FusedIterator for FileEnumerator {}

pub trait FileEnumeratorExtManual: IsA<FileEnumerator> {
    // rustdoc-stripper-ignore-next
    /// Converts the enumerator into a [`Stream`](futures_core::Stream).
    fn into_stream(self, num_files: i32, priority: glib::Priority) -> FileEnumeratorStream {
        let future: Option<std::pin::Pin<Box<dyn Future<Output = _>>>> =
            Some(Box::pin(self.next_files_future(num_files, priority)));
        FileEnumeratorStream {
            enumerator: self.upcast(),
            future,
            num_files,
            priority,
        }
    }

    #[doc(alias = "g_file_enumerator_close")]
    fn close(
        &self,
        cancellable: Option<&impl IsA<crate::Cancellable>>,
    ) -> (bool, Option<glib::Error>) {
        unsafe {
            let mut error = std::ptr::null_mut();
            let ret = crate::ffi::g_file_enumerator_close(
                self.as_ref().to_glib_none().0,
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                &mut error,
            );
            (from_glib(ret), from_glib_full(error))
        }
    }
}

impl<O: IsA<FileEnumerator>> FileEnumeratorExtManual for O {}

// rustdoc-stripper-ignore-next
/// A [`Stream`](futures_core::Stream) used to enumerate files in directories.
pub struct FileEnumeratorStream {
    enumerator: FileEnumerator,
    future: Option<LocalBoxFuture<'static, Result<Vec<FileInfo>, glib::Error>>>,
    num_files: i32,
    priority: glib::Priority,
}

impl std::fmt::Debug for FileEnumeratorStream {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileEnumeratorStream")
            .field("enumerator", &self.enumerator)
            .field("num_files", &self.num_files)
            .field("priority", &self.priority)
            .finish()
    }
}

impl futures_core::Stream for FileEnumeratorStream {
    type Item = Result<Vec<FileInfo>, glib::Error>;

    #[inline]
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.future.take() {
            Some(mut f) => match f.poll_unpin(cx) {
                Poll::Ready(Ok(fs)) if fs.is_empty() => Poll::Ready(None),
                Poll::Ready(Ok(fs)) => {
                    self.future = Some(Box::pin(
                        self.enumerator
                            .next_files_future(self.num_files, self.priority),
                    ));
                    Poll::Ready(Some(Ok(fs)))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Pending => {
                    self.future = Some(f);
                    Poll::Pending
                }
            },
            None => Poll::Ready(None),
        }
    }
}

impl futures_core::FusedStream for FileEnumeratorStream {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.future.is_none()
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use futures_util::StreamExt;
    use std::{cell::Cell, rc::Rc};
    #[test]
    fn file_enumerator_stream() {
        let dir = std::env::current_dir().unwrap();
        let ctx = glib::MainContext::new();
        let lp = glib::MainLoop::new(Some(&ctx), false);
        let res = Rc::new(Cell::new(None));

        let lp_clone = lp.clone();
        let res_clone = res.clone();
        ctx.spawn_local(async move {
            res_clone.replace(Some(
                async {
                    let dir = crate::File::for_path(dir);
                    let mut stream = dir
                        .enumerate_children_future(
                            crate::FILE_ATTRIBUTE_STANDARD_NAME,
                            crate::FileQueryInfoFlags::NONE,
                            glib::Priority::default(),
                        )
                        .await?
                        .into_stream(4, glib::Priority::default());
                    while let Some(files) = stream.next().await {
                        for file in files? {
                            let _ = file.name();
                        }
                    }
                    Ok::<_, glib::Error>(())
                }
                .await,
            ));
            lp_clone.quit();
        });
        lp.run();
        // propagate any error from the future into a panic
        Rc::try_unwrap(res)
            .unwrap_or_else(|_| panic!("future not finished"))
            .into_inner()
            .unwrap()
            .unwrap();
    }
}
