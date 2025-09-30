pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.67.1";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-LpuOPC82Rt9EvvLvAYGXRpQYz50Q/Ec3VYTS6/xehAA=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
