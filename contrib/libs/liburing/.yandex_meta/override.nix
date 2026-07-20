pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.15";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-oBNu5DI2RMk0BPm6NT8qaYuyhk7+KIpFSsBsVwI7BO8=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
