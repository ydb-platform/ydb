pkgs: attrs: with pkgs; rec {
  version = "1.13.0";

  src = fetchurl {
    url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
    hash = "sha256-zJjN19DOAFC1WJyZ+JrHL7NK7m/4i7M1HyOUB6ZWmf4=";
  };

  patches = [];

  buildInputs = [
    libev
    nghttp3
    quictls
  ];
}
