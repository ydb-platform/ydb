pkgs: attrs: with pkgs; with attrs; rec {
    pname = "nghttp3";
    version = "1.15.0";

    nativeBuildInputs = [
      cmake pkg-config autoconf libtool automake
    ];

    buildInputs = [ ];

    src = fetchurl {
      url = "https://github.com/ngtcp2/nghttp3/releases/download/v${version}/nghttp3-${version}.tar.xz";
      hash = "sha256:6da0cd06b428d32a54c58137838505d9dc0371a900bb8070a46b29e1ceaf2e0f";
    };
}
