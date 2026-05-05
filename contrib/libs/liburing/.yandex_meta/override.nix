pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.14";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-bSq4M28JRND4bdaIv/KXcCDB35cYM7gra1GVO3poWfc=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
