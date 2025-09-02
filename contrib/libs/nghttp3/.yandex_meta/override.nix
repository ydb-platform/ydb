pkgs: attrs: with pkgs; with attrs; rec {
    pname = "nghttp3";
    version = "1.11.0";

    nativeBuildInputs = [
      cmake pkg-config autoconf libtool automake
    ];

    buildInputs = [ ];

    src = fetchurl {
      url = "https://github.com/ngtcp2/nghttp3/releases/download/v${version}/nghttp3-${version}.tar.xz";
      hash = "sha256-J9CEUY8G14J5sFDMnN/yQY+A+3U9oBlCfOhTzskg8z8=";
    };
}
