pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.63.0";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-Mw2MttT2BC65w/GRqFCMj4hDlSNxCsdN3Q1nOawfJSk=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
