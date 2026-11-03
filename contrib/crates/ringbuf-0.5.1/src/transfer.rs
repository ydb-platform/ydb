use crate::{consumer::Consumer, producer::Producer};

/// Moves at most `count` items from the `src` consumer to the `dst` producer.
///
/// Consumer and producer may be of different buffers as well as of the same one.
/// `count` is the number of items being moved, if `None` - as much as possible items will be moved.
///
/// Returns number of items been moved.
pub fn transfer<T, C: Consumer<Item = T>, P: Producer<Item = T>>(src: &mut C, dst: &mut P, count: Option<usize>) -> usize {
    let (src_left, src_right) = src.occupied_slices();
    let (dst_left, dst_right) = dst.vacant_slices_mut();
    let src_iter = src_left.iter().chain(src_right.iter());
    let dst_iter = dst_left.iter_mut().chain(dst_right.iter_mut());

    let mut actual_count = 0;
    for (src_elem, dst_place) in src_iter.zip(dst_iter) {
        if let Some(count) = count
            && actual_count >= count
        {
            break;
        }
        unsafe { dst_place.write(src_elem.as_ptr().read()) };
        actual_count += 1;
    }
    unsafe { src.advance_read_index(actual_count) };
    unsafe { dst.advance_write_index(actual_count) };
    actual_count
}
