pkgs: attrs: with pkgs; rec {
  version = "1.11.0";

  src = fetchurl {
    url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
    hash = "sha256-OCwVv2a2MPJgIWMbJbfrH+rELGfB1oJhidYQkd+qTQk=";
  };

  patches = [];

  buildInputs = [
    libev
    nghttp3
    quictls
  ];
}
