pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.11";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-V73QP89WMrL2fkPRbo/TSkfO7GeDsCudlw2Ut5baDzA=";
  };

  # workaround for 'Unable to create temporary directory'
  # see https://github.com/direnv/direnv/issues/1345 and https://github.com/NixOS/nix/issues/11929
  shellHook = ''
    mkdir -p $yamaker_out/tmp_build
    TMPDIR=$yamaker_out/tmp_build
  '';

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
