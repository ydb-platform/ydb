pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.12";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-sEMzkyjrCc49ogfUnzdgNtEXmW0Tz/PUKo99C965428=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
