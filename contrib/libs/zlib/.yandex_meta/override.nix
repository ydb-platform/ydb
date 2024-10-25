pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.3.1";

  src = fetchFromGitHub {
    owner = "madler";
    repo = "zlib";
    rev = "v${version}";
    hash = "sha256-TkPLWSN5QcPlL9D0kc/yhH0/puE9bFND24aj5NVDKYs=";
  };
}
