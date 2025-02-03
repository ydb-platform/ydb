pkgs: attrs: with pkgs; with attrs; rec {
    pname = "nghttp3";
    version = "1.7.0";

    nativeBuildInputs = [
      cmake pkg-config autoconf libtool automake
    ];

    buildInputs = [ ];

    src = fetchurl {
      url = "https://github.com/ngtcp2/nghttp3/releases/download/v${version}/nghttp3-${version}.tar.xz";
      hash = "sha256-tOtrzrmSk9mp3yAxwarRZq89V7PjNlWsoGmTl7bw11E=";
    };
}
