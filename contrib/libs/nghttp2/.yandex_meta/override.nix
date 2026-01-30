pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.68.0";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-qH0QsedFWj+cpmm9jc3DG6gxb4O+znpn2seiY6y0EjM=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
