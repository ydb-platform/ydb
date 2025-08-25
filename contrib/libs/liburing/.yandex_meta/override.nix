pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.11";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-V73QP89WMrL2fkPRbo/TSkfO7GeDsCudlw2Ut5baDzA=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
