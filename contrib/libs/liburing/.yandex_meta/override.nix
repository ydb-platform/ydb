pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.9";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-zOC53i52YJsH3AQIy4afjTGlX/IvVnW2QnYOppFxKiI=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
