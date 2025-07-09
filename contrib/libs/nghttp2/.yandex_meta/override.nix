pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.66.0";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-67cfeZtbHGceBHzbsliyR1ehoikg0JnKZF6Jlpgz8HA=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
