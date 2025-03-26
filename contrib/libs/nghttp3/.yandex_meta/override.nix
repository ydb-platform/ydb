pkgs: attrs: with pkgs; with attrs; rec {
    pname = "nghttp3";
    version = "1.8.0";

    nativeBuildInputs = [
      cmake pkg-config autoconf libtool automake
    ];

    buildInputs = [ ];

    src = fetchurl {
      url = "https://github.com/ngtcp2/nghttp3/releases/download/v${version}/nghttp3-${version}.tar.xz";
      hash = "sha256-qd0olwl35oAqPq8s+urm0PrmDI0sDyxM5gADanmY7po=";
    };
}
