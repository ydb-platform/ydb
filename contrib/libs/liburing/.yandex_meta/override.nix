pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.7";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-WhNlO2opPM7v4LOLWpmzPv31++zmn5Hmb6Su9IQBDH8=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
