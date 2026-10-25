// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures_util::io::{AsyncBufReadExt, AsyncReadExt};
    use glib::Bytes;

    use crate::{MemoryInputStream, prelude::*};

    #[test]
    fn new() {
        let strm = MemoryInputStream::new();
        let ret = strm.skip(1, crate::Cancellable::NONE);
        assert!(ret.is_ok());
        assert_eq!(ret.unwrap(), 0);

        let mut buf = vec![0; 10];
        let ret = strm.read(&mut buf, crate::Cancellable::NONE).unwrap();
        assert_eq!(ret, 0);
    }

    #[test]
    fn from_bytes() {
        let b = Bytes::from_owned(vec![1, 2, 3]);
        let strm = MemoryInputStream::from_bytes(&b);
        let mut buf = vec![0; 10];
        let ret = strm.read(&mut buf, crate::Cancellable::NONE).unwrap();
        assert_eq!(ret, 3);
        assert_eq!(buf[0], 1);
        assert_eq!(buf[1], 2);
        assert_eq!(buf[2], 3);

        let ret = strm.skip(10, crate::Cancellable::NONE).unwrap();
        assert_eq!(ret, 0);
    }

    #[test]
    fn add_bytes() {
        let strm = MemoryInputStream::new();
        let b = Bytes::from_owned(vec![1, 2, 3]);
        strm.add_bytes(&b);
        let mut buf = vec![0; 10];
        let ret = strm.read(&mut buf, crate::Cancellable::NONE).unwrap();
        assert_eq!(ret, 3);
        assert_eq!(buf[0], 1);
        assert_eq!(buf[1], 2);
        assert_eq!(buf[2], 3);

        let ret = strm.skip(10, crate::Cancellable::NONE).unwrap();
        assert_eq!(ret, 0);
    }

    #[test]
    fn read_future() {
        use futures_util::future::TryFutureExt;

        let c = glib::MainContext::new();

        let buf = vec![0; 10];
        let b = glib::Bytes::from_owned(vec![1, 2, 3]);
        let strm = MemoryInputStream::from_bytes(&b);

        let res = c
            .block_on(
                strm.read_future(buf, glib::Priority::default())
                    .map_err(|(_buf, err)| err)
                    .map_ok(move |(mut buf, len)| {
                        buf.truncate(len);
                        buf
                    }),
            )
            .unwrap();

        assert_eq!(res, vec![1, 2, 3]);
    }

    #[test]
    fn async_read() {
        async fn run() -> Result<(), Box<dyn Error>> {
            let b = Bytes::from_owned(vec![1, 2, 3]);

            // Adapter is big enough to read everything in one read
            let mut read = MemoryInputStream::from_bytes(&b).into_async_buf_read(8);
            let mut buf = [0u8; 4];
            assert_eq!(read.read(&mut buf).await?, 3);
            assert_eq!(buf, [1, 2, 3, 0]);
            assert_eq!(read.read(&mut buf).await?, 0);

            let mut read = MemoryInputStream::from_bytes(&b).into_async_buf_read(8);
            let mut buf = [0u8; 1];
            assert_eq!(read.read(&mut buf).await?, 1);
            assert_eq!(buf, [1]);
            assert_eq!(read.read(&mut buf).await?, 1);
            assert_eq!(buf, [2]);
            assert_eq!(read.read(&mut buf).await?, 1);
            assert_eq!(buf, [3]);
            assert_eq!(read.read(&mut buf).await?, 0);

            // Adapter is NOT big enough to read everything in one read
            let mut read = MemoryInputStream::from_bytes(&b).into_async_buf_read(2);
            let mut buf = [0u8; 4];
            assert_eq!(read.read(&mut buf).await?, 2);
            assert_eq!(buf, [1, 2, 0, 0]);
            assert_eq!(read.read(&mut buf).await?, 1);
            assert_eq!(buf[0], 3);
            assert_eq!(read.read(&mut buf).await?, 0);

            let mut read = MemoryInputStream::from_bytes(&b).into_async_buf_read(2);
            let mut buf = [0u8; 1];
            assert_eq!(read.read(&mut buf).await?, 1);
            assert_eq!(buf, [1]);
            assert_eq!(read.read(&mut buf).await?, 1);
            assert_eq!(buf, [2]);
            assert_eq!(read.read(&mut buf).await?, 1);
            assert_eq!(buf, [3]);
            assert_eq!(read.read(&mut buf).await?, 0);

            Ok(())
        }

        let main_context = glib::MainContext::new();
        main_context.block_on(run()).unwrap();
    }

    #[test]
    fn async_buf_read() {
        async fn run() -> Result<(), Box<dyn Error>> {
            let b = Bytes::from_owned(vec![1, 2, 3]);
            // Adapter is big enough to read everything in one read
            let mut read = MemoryInputStream::from_bytes(&b).into_async_buf_read(16);
            let mut buf = String::new();
            assert_eq!(read.read_line(&mut buf).await?, 3);
            assert_eq!(buf.as_bytes(), [1, 2, 3]);
            assert_eq!(read.read_line(&mut buf).await?, 0);

            // Adapter is NOT big enough to read everything in one read
            let mut read = MemoryInputStream::from_bytes(&b).into_async_buf_read(2);
            let mut buf = String::new();
            assert_eq!(read.read_line(&mut buf).await?, 3);
            assert_eq!(buf.as_bytes(), [1, 2, 3]);
            assert_eq!(read.read_line(&mut buf).await?, 0);

            Ok(())
        }

        let main_context = glib::MainContext::new();
        main_context.block_on(run()).unwrap();
    }
}
