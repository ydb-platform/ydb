pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.2.16";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-compression";
    rev = "v${version}";
    hash = "sha256-aQ5UsMms8aJh5yrE9of1AQgIGTAk9vyBRaybwYqUY68=";
  };
}
