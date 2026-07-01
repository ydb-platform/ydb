pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.14.0";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-common";
    rev = "v${version}";
    hash = "sha256-UxFnIgGDqBc62OF2x4svO/NsPqXw1APFmdUbbBcxKn8=";
  };
}
