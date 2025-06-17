pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.10";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-yw21Krg/xsBGCbwwQDIbrq/7q+LNCwC3cXyGPANjkEA=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
