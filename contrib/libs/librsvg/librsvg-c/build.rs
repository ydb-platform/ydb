use regex::Regex;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, Result, Write};
use std::path::Path;

fn main() {
    let version = read_version().expect("could not parse version from meson.build");
    write_version_rs(&version);
}

struct Version {
    major: String,
    minor: String,
    micro: String,
}

fn read_version() -> Result<Version> {
    {
        let output =
            Path::new(&env::var("CARGO_MANIFEST_DIR").expect("Manifest directory unknown"))
                .parent()
                .expect("Invalid manifest path")
                .join("meson.build");
        let file = File::open(output)?;

        // This function reads one of the build configuration files (meson.build) and scans
        // it for the package's version number.
        //
        // The start of meson.build should contain this:
        //
        //   project('librsvg',
        //           'rust',
        //           'c',
        //           version: '2.53.0',
        //           meson_version: '>= 0.59')
        //
        // This regex looks for the "version" line.

        let regex = Regex::new(r#"^\s+version: '(\d+\.\d+\.\d+)'"#).unwrap();

        for line in BufReader::new(file).lines() {
            match line {
                Ok(line) => {
                    if let Some(caps) = regex.captures(&line) {
                        let version_str = &caps[1];
                        let mut components = version_str.split('.');
                        let major = components.next().unwrap().to_string();
                        let minor = components.next().unwrap().to_string();
                        let micro = components.next().unwrap().to_string();
                        return Ok(Version {
                            major,
                            minor,
                            micro,
                        });
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    panic!("Version not found in meson.build");
}

fn write_version_rs(version: &Version) {
    let output = Path::new(&env::var("OUT_DIR").unwrap()).join("version.rs");
    let mut file = File::create(output).expect("open version.rs for writing");
    file.write_all(
        format!(
            r#"
use std::os::raw::c_uint;

#[unsafe(no_mangle)]
pub static rsvg_major_version: c_uint = {};

#[unsafe(no_mangle)]
pub static rsvg_minor_version: c_uint = {};

#[unsafe(no_mangle)]
pub static rsvg_micro_version: c_uint = {};
"#,
            version.major, version.minor, version.micro,
        )
        .as_bytes(),
    )
    .expect("write version.rs");
}
