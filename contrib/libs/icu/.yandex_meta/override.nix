self: super: with self; rec {
  version = "78.3";

  src = fetchurl {
    url = "https://github.com/unicode-org/icu/releases/download/release-${version}/icu4c-${version}-sources.tgz";
    hash = "sha256-Oi56R2BLpwLzRYeDCOb+/sphLuiVz0pfIi55Vfq/4MA=";
  };

  sourceRoot = "icu/source";

  configureFlags = [
    "--build=x86_64-unknown-linux-gnu"
  ];
}
