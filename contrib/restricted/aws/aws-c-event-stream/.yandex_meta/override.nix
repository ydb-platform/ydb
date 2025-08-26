pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.5.6";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-event-stream";
    rev = "v${version}";
    hash = "sha256-wGSACoe5dfdd9CQu+BnTQU+r3ApM0Dzv2zaYyWwYVL4=";
  };
}
