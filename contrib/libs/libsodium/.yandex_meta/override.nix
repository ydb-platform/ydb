pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.0.18";

  src = fetchFromGitHub {
    owner = "jedisct1";
    repo = "libsodium";
    rev = "${version}";
    hash = "sha256-TOtnEEeiAC+VoCerqtsKd+MIf/k2zTbUhFhPBnovv4w=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  patches = [
    ./build-all.patch
    ./PR839.patch
    ./PR1813089.patch
  ];
}
