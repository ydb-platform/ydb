pkgs: attrs: with pkgs; with attrs; rec {
    pname = "nghttp3";
    version = "1.17.0";

    nativeBuildInputs = [
      cmake pkg-config autoconf libtool automake
    ];

    buildInputs = [ ];

    src = fetchurl {
      url = "https://github.com/ngtcp2/nghttp3/releases/download/v${version}/nghttp3-${version}.tar.xz";
      hash = "sha256:e8b798272b9282045cb83577dcf7bd7fcd22bb3a43aec0eb1a24f675b4cef0b8";
    };
}
