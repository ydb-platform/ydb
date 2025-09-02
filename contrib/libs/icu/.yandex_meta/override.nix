self: super: with self; rec {
  version = "77.1";
  version_dash = "${lib.replaceStrings [ "." ] [ "-" ] version}";
  version_us = "${lib.replaceStrings [ "." ] [ "_" ] version}";

  src = fetchurl {
    url = "https://github.com/unicode-org/icu/releases/download/release-${version_dash}/icu4c-${version_us}-src.tgz";
    hash = "sha256-WI5DH3cyfDkDH/u4hDwOO8EiwhE3RIX6h9xfP6/yQGE=";
  };

  sourceRoot = "icu/source";

  configureFlags = [
    "--build=x86_64-unknown-linux-gnu"
  ];
}
