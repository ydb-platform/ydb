pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.3.2";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-compression";
    rev = "v${version}";
    hash = "sha256-YckyQZNk+48g5jrT4q8Clmy4LRwswKONvFbVtJxgpYQ=";
  };
}
