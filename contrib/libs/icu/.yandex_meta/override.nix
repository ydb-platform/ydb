self: super: with self; rec {
  version = "79.1";

  src = fetchurl {
    url = "https://github.com/unicode-org/icu/releases/download/release-${version}/icu4c-${version}-sources.tgz";
    hash = "sha256-Bvpj7LUhBI+AgvEq2Yj2tnjZDL2IqvziTLyUkCsZHZk=";
  };

  sourceRoot = "icu/source";

  configureFlags = [
    "--build=x86_64-unknown-linux-gnu"
  ];
}
