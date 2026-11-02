// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(test)]
mod tests {
    use crate::{MemoryOutputStream, prelude::*};

    #[test]
    fn steal_empty() {
        let strm = MemoryOutputStream::new_resizable();
        assert_eq!(strm.data_size(), 0);

        assert!(strm.close(crate::Cancellable::NONE).is_ok());
        assert_eq!(strm.steal_as_bytes(), [].as_ref());
    }

    #[test]
    fn steal() {
        let strm = MemoryOutputStream::new_resizable();

        assert!(strm.write(&[1, 2, 3], crate::Cancellable::NONE).is_ok());
        assert_eq!(strm.data_size(), 3);

        assert!(strm.write(&[4, 5], crate::Cancellable::NONE).is_ok());
        assert_eq!(strm.data_size(), 5);

        assert!(strm.close(crate::Cancellable::NONE).is_ok());
        assert_eq!(strm.steal_as_bytes(), [1, 2, 3, 4, 5].as_ref());
    }
}
