use super::Rb;
use crate::{storage::Array, traits::*};
use std::io::{self, Read};

macro_rules! assert_eq_kind {
    ($left:expr, $right:expr) => {
        assert_eq!($left.map(|r| r.map_err(|e| e.kind())), $right);
    };
}

#[test]
fn from() {
    let mut rb0 = Rb::<Array<u8, 4>>::default();
    let mut rb1 = Rb::<Array<u8, 4>>::default();
    let (mut prod0, mut cons0) = rb0.split_ref();
    let (mut prod1, mut cons1) = rb1.split_ref();

    let mut tmp = [0; 5];

    assert_eq!(prod0.push_slice(&[0, 1, 2]), 3);

    assert_eq_kind!(prod1.read_from(&mut cons0, None), Some(Ok(3)));
    assert_eq_kind!(prod1.read_from(&mut cons0, None), Some(Err(io::ErrorKind::WouldBlock)));

    assert_eq!(cons1.pop_slice(&mut tmp), 3);
    assert_eq!(tmp[0..3], [0, 1, 2]);

    assert_eq!(prod0.push_slice(&[3, 4, 5]), 3);

    assert_eq_kind!(prod1.read_from(&mut cons0, None), Some(Ok(1)));
    assert_eq!(cons1.pop_slice(&mut tmp), 1);
    assert_eq!(tmp[0..1], [3]);

    assert_eq_kind!(prod1.read_from(&mut cons0, None), Some(Ok(2)));
    assert_eq!(cons1.pop_slice(&mut tmp), 2);
    assert_eq!(tmp[0..2], [4, 5]);

    assert_eq!(prod1.push_slice(&[6, 7, 8]), 3);
    assert_eq!(prod0.push_slice(&[9, 10]), 2);

    assert_eq_kind!(prod1.read_from(&mut cons0, None), Some(Ok(1)));
    assert_eq_kind!(prod1.read_from(&mut cons0, None), None);

    assert_eq!(cons1.pop_slice(&mut tmp), 4);
    assert_eq!(tmp[0..4], [6, 7, 8, 9]);
}

#[test]
fn into() {
    let mut rb0 = Rb::<Array<u8, 4>>::default();
    let mut rb1 = Rb::<Array<u8, 4>>::default();
    let (mut prod0, mut cons0) = rb0.split_ref();
    let (mut prod1, mut cons1) = rb1.split_ref();

    let mut tmp = [0; 5];

    assert_eq!(prod0.push_slice(&[0, 1, 2]), 3);

    assert_eq_kind!(cons0.write_into(&mut prod1, None), Some(Ok(3)));
    assert_eq_kind!(cons0.write_into(&mut prod1, None), None);

    assert_eq!(cons1.pop_slice(&mut tmp), 3);
    assert_eq!(tmp[0..3], [0, 1, 2]);

    assert_eq!(prod0.push_slice(&[3, 4, 5]), 3);

    assert_eq_kind!(cons0.write_into(&mut prod1, None), Some(Ok(1)));
    assert_eq!(cons1.pop_slice(&mut tmp), 1);
    assert_eq!(tmp[0..1], [3]);

    assert_eq_kind!(cons0.write_into(&mut prod1, None), Some(Ok(2)));
    assert_eq!(cons1.pop_slice(&mut tmp), 2);
    assert_eq!(tmp[0..2], [4, 5]);

    assert_eq!(prod1.push_slice(&[6, 7, 8]), 3);
    assert_eq!(prod0.push_slice(&[9, 10]), 2);

    assert_eq_kind!(cons0.write_into(&mut prod1, None), Some(Ok(1)));
    assert_eq_kind!(cons0.write_into(&mut prod1, None), Some(Err(io::ErrorKind::WouldBlock)));

    assert_eq!(cons1.pop_slice(&mut tmp), 4);
    assert_eq!(tmp[0..4], [6, 7, 8, 9]);
}

#[test]
fn count() {
    let mut rb0 = Rb::<Array<u8, 4>>::default();
    let mut rb1 = Rb::<Array<u8, 4>>::default();
    let (mut prod0, mut cons0) = rb0.split_ref();
    let (mut prod1, mut cons1) = rb1.split_ref();

    let mut tmp = [0; 5];

    assert_eq!(prod0.push_slice(&[0, 1, 2, 3]), 4);

    assert_eq_kind!(prod1.read_from(&mut cons0, Some(3)), Some(Ok(3)));

    assert_eq!(cons1.pop_slice(&mut tmp), 3);
    assert_eq!(tmp[0..3], [0, 1, 2]);

    assert_eq!(prod0.push_slice(&[4, 5, 6]), 3);

    assert_eq_kind!(cons0.write_into(&mut prod1, Some(3)), Some(Ok(1)));
    assert_eq_kind!(cons0.write_into(&mut prod1, Some(2)), Some(Ok(2)));
    assert_eq_kind!(cons0.write_into(&mut prod1, Some(2)), Some(Ok(1)));

    assert_eq!(cons1.pop_slice(&mut tmp), 4);
    assert_eq!(tmp[0..4], [3, 4, 5, 6]);
}

#[test]
fn read_from() {
    struct Reader;

    impl Read for Reader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            for b in buf.iter_mut() {
                // Read buffer before writing to ensure its initialized.
                *b = b.wrapping_add(1);
            }
            buf.fill(2);
            Ok(buf.len())
        }
    }

    let mut rb = Rb::<Array<u8, 4>>::default();
    let (mut prod, mut cons) = rb.split_ref();
    prod.try_push(1).unwrap();
    assert_eq!(cons.try_pop().unwrap(), 1);

    assert_eq!(prod.read_from(&mut Reader, None).unwrap().unwrap(), 3);

    assert!(cons.pop_iter().eq([2; 3]));
}
