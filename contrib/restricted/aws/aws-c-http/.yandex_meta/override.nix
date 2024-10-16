pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.7.6";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-http";
    rev = "v${version}";
    hash = "sha256-pJGzGbIuz8UJkfmTQEZgXSOMuYixMezNZmgaRlcnmfg=";
  };
}
