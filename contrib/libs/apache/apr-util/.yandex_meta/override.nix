pkgs: attrs: with pkgs; with attrs; rec {
  name = "apr-util-${version}";
  version = "1.6.3";

  src = fetchurl {
    url = "mirror://apache/apr/${name}.tar.bz2";
    hash = "sha256-pBB243EHRjJsOUUEKZStmk/KwM4Cd92P6gdv7DyXcrU=";
  };

  configureFlags = [
    "--with-apr=${apr.dev}"
    "--with-expat=${expat.dev}"
    "--with-crypto"
    "--with-openssl=${openssl.dev}"
  ];
}
