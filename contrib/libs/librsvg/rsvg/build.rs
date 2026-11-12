use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

#[cfg(docsrs)]
fn probe_system_deps() {
    // do not probe libraries since the docs.rs environment doesn't have them
}

#[cfg(not(docsrs))]
fn probe_system_deps() {
    if let Err(e) = system_deps::Config::new().probe() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn main() {
    probe_system_deps();
    generate_srgb_tables();
}

/// Converts an sRGB color value to a linear sRGB color value (undoes the gamma correction).
///
/// The input and the output are supposed to be in the [0, 1] range.
#[inline]
fn linearize(c: f64) -> f64 {
    if c <= (12.92 * 0.0031308) {
        c / 12.92
    } else {
        ((c + 0.055) / 1.055).powf(2.4)
    }
}

/// Converts a linear sRGB color value to a normal sRGB color value (applies the gamma correction).
///
/// The input and the output are supposed to be in the [0, 1] range.
#[inline]
fn unlinearize(c: f64) -> f64 {
    if c <= 0.0031308 {
        12.92 * c
    } else {
        1.055 * c.powf(1f64 / 2.4) - 0.055
    }
}

fn print_table<W, F>(w: &mut W, name: &str, f: F, len: u32)
where
    W: Write,
    F: Fn(f64) -> f64,
{
    writeln!(w, "const {name}: [u8; {len}] = [").unwrap();

    for i in 0..len {
        let x = f(i as f64 / 255.0);
        let v = (x * 255.0).round() as u8;
        writeln!(w, "    {v},").unwrap();
    }

    writeln!(w, "];").unwrap();
}

fn generate_srgb_tables() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("srgb-codegen.rs");
    let mut file = BufWriter::new(File::create(path).unwrap());

    print_table(&mut file, "LINEARIZE", linearize, 256);
    print_table(&mut file, "UNLINEARIZE", unlinearize, 256);
}
