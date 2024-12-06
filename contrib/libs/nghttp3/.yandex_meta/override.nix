pkgs: attrs: with pkgs; with attrs; rec {
    pname = "nghttp3";
    version = "1.6.0";

    nativeBuildInputs = [
      cmake pkg-config autoconf libtool automake
    ];

    buildInputs = [ ];

    src = fetchurl {
      url = "https://github.com/ngtcp2/nghttp3/releases/download/v${version}/nghttp3-${version}.tar.xz";
      hash = "sha256-6qkBlUvElANNNzjvGRMN5pOH1qPaApBExg2drpF5Ko0=";
    };
}
