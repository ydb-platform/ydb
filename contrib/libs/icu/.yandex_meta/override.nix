self: super: with self; rec {
  version = "78.1";

  src = fetchurl {
    url = "https://github.com/unicode-org/icu/releases/download/release-${version}/icu4c-${version}-sources.tgz";
    hash = "sha256-Yhf1jKObIxJ2Bc/Gx+DTR1/ksNYxVwETg9cWy0FheIY=";
  };

  sourceRoot = "icu/source";

  configureFlags = [
    "--build=x86_64-unknown-linux-gnu"
  ];
}
