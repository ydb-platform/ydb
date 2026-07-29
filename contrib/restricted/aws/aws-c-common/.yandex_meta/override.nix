pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.14.2";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-kfRymCcPnkd+JA0Vk3QGMv68bdmd8C+dv5etrhc8tDM=";
  };
}
