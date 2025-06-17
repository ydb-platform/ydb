pkgs: attrs: with pkgs; with attrs; rec {
    pname = "nghttp3";
    version = "1.10.1";

    nativeBuildInputs = [
      cmake pkg-config autoconf libtool automake
    ];

    buildInputs = [ ];

    src = fetchurl {
      url = "https://github.com/ngtcp2/nghttp3/releases/download/v${version}/nghttp3-${version}.tar.xz";
      hash = "sha256-5rjrqt+OV8unej407o3kZf6VJIH793xPmNSHN731DgM=";
    };
}
