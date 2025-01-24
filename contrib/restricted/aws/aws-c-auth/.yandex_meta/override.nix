pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.6.27";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-auth";
    rev = "v${version}";
    hash = "sha256-rjluBj8C4GjE67Os0+1CKKI/2V9RnkbYKhpdqQBryik=";
  };
}
