pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.70.0";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-YoQ0hYyQGgfh75rEVaJPrFb4Gxc8Vs+ZuAbhBbkNg6k=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
