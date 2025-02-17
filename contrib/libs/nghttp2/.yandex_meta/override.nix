pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.64.0";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-h+2X+GZ8Uk3zhkcwIRNH/21r7Yj01ao+U8kmZt/DUo8=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
