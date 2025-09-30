pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.67.0";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-k3N5l98iuwG+ka1loSRp2p+QJ/4La+VBNiWBMyXBitY=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
