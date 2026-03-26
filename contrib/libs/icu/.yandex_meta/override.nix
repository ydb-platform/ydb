self: super: with self; rec {
  version = "78.2";

  src = fetchurl {
    url = "https://github.com/unicode-org/icu/releases/download/release-${version}/icu4c-${version}-sources.tgz";
    hash = "sha256-Pploe1xDXUsgljDi0uu3mQbJhGheeGNQeLZy4DyJ3zU=";
  };

  sourceRoot = "icu/source";

  configureFlags = [
    "--build=x86_64-unknown-linux-gnu"
  ];
}
