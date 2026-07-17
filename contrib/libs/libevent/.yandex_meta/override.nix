pkgs: attrs: with pkgs; rec {
  pname = "libevent";
  version = "2.1.13";

  src = fetchurl {
    url = "https://github.com/libevent/libevent/releases/download/release-${version}-stable/libevent-${version}-stable.tar.gz";
    sha256 = "sha256-9+k4O4wLqoG2h+W17swBvu+vGxm2QVHZXtYWR/56MVw=";
  };

  buildInputs = attrs.buildInputs ++ [ zlib ];
}
