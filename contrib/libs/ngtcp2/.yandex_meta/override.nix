pkgs: attrs: with pkgs; rec {
  version = "1.10.0";

  src = fetchurl {
    url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
    hash = "sha256-T43B1hlXIF0Bw9aqbxyWx7K6wf7qcf2vly2G219kZd8=";
  };

  patches = [];

  buildInputs = [
    libev
    nghttp3
    quictls
  ];
}
