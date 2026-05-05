pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.68.1";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-3NfTM4DqnT6u7VnpzkJ3KwgN282fAh75mZN6wcX7EmQ=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
