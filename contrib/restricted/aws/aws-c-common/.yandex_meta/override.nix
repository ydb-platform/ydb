pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.14.3";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-Y3tow0kebhiTgo4ob+rRcgXODyclJO/yUTd2hQG5F6A=";
  };
}
