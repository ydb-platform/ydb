pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.69.0";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-miYPVlMK85RbS/r0ZnV/fgNh18abZUW9ZJd1FN7/wV8=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
