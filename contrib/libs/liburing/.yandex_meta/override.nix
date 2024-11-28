pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.8";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-10zmoMDzO41oNRVXE/6FzDGPVRVJTJTARVUmc1b7f+o=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
