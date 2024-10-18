pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.1.9";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-sdkutils";
    rev = "v${version}";
    hash = "sha256-iKHO8awWWB8tvYCr+/R6hhK8a/PnanYYEAJ7zNOJC3w=";
  };
}
