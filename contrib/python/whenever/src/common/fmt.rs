//! Helpers for writing formatted strings

pub(crate) fn write_2_digits(n: u8, buf: &mut [u8]) {
    buf[0] = n / 10 + b'0';
    buf[1] = n % 10 + b'0';
}

pub(crate) fn write_4_digits(n: u16, buf: &mut [u8]) {
    buf[0] = (n / 1000) as u8 + b'0';
    buf[1] = (n / 100 % 10) as u8 + b'0';
    buf[2] = (n / 10 % 10) as u8 + b'0';
    buf[3] = (n % 10) as u8 + b'0';
}
