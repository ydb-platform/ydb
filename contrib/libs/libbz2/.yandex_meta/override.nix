pkgs: attrs: with pkgs; rec {
  version = "1.0.8";

  src = fetchurl {
    url = "https://sourceware.org/pub/bzip2/bzip2-${version}.tar.gz";
    sha256 = "0s92986cv0p692icqlw1j42y9nld8zd83qwhzbqd61p1dqbh6nmb";
  };

  nativeBuildInputs = [];
  patches = [];
}
