pkgs: attrs: with pkgs; rec {
  version = "1.14.0";

  src = fetchurl {
    url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
    hash = "sha256-0fv56ukpIb/TMVTasldLxLfXk29IY5bWx4v/+Q7Vs10=";
  };

  patches = [];

  buildInputs = [
    libev
    nghttp3
    quictls
  ];
}
