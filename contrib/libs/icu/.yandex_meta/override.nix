self: super: with self; rec {
  version = "76.1";
  version_dash = "${lib.replaceStrings [ "." ] [ "-" ] version}";
  version_us = "${lib.replaceStrings [ "." ] [ "_" ] version}";

  src = fetchurl {
    url = "https://github.com/unicode-org/icu/releases/download/release-${version_dash}/icu4c-${version_us}-src.tgz";
    hash = "sha256-36y0a/5HR0EEcs4+EUS/KKEC/uqk44dbrJtMbPMPTz4=";
  };

  sourceRoot = "icu/source";

  configureFlags = [
    "--build=x86_64-unknown-linux-gnu"
  ];
}
