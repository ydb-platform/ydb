pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.13";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-ZWM+SKeRw5iivyj0mHSxC6yw492N7CThx/pp4FJhkCo=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
