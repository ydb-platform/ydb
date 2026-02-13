pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.4.0";

  src = fetchFromGitHub {
    owner = "google";
    repo = "double-conversion";
    rev = "v${version}";
    hash = "sha256-gxaPqQ51RyXZaTHkvh4RBpedPopcRiuWDoT+PPbI1uw=";
  };

  patches = [];
}

