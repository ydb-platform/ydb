pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.98.1";

  src = fetchFromGitHub {
    owner = "BLAKE2";
    repo = "libb2";
    rev = "v${version}";
    sha256 = "0qj8aaqvfcavj1vj5asm4pqm03ap7q8x4c2fy83cqggvky0frgya";
  };
}
