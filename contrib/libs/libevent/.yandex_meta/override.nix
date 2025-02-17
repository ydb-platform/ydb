pkgs: attrs: with pkgs; rec {
  pname = "libevent";
  version = "2.1.12";

  src = fetchurl {
    url = "https://github.com/libevent/libevent/releases/download/release-${version}-stable/libevent-${version}-stable.tar.gz";
    sha256 = "1fq30imk8zd26x8066di3kpc5zyfc5z6frr3zll685zcx4dxxrlj";
  };

  buildInputs = attrs.buildInputs ++ [ zlib ];
}
