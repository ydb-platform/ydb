pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.19.8";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-crt-cpp";
    rev = "v${version}";
    hash = "sha256-z/9ifBv4KbH5RiR1t1Dz8cCWZlHrMSyB8/w4pdTscw0=";
  };
}
