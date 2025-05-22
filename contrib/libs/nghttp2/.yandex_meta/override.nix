pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.65.0";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-YdHRnXerKOFWggvRM0ROpCDexHRi8+EDj9i1b6UOapM=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
