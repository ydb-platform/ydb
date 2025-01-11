pkgs: attrs: with pkgs; rec {
  version = "1.8.1";

  src = fetchurl {
    url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
    hash = "sha256-rIRKees/FT5Mzc/szt9CxXqzUruKuS7IrF00F6ec+xE=";
  };

  patches = [];

  buildInputs = [
    libev
    nghttp3
    quictls
  ];
}
