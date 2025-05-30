pkgs: attrs: with pkgs; rec {
  version = "1.12.0";

  src = fetchurl {
    url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
    hash = "sha256-JSekyTBdvtYQoACoj5RpZSaqiVn3QkmlnyuW7nNjBik=";
  };

  patches = [];

  buildInputs = [
    libev
    nghttp3
    quictls
  ];
}
