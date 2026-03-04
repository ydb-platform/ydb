pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.13.5";

  src = fetchFromGitHub {
    owner = "awslabs";
    repo = "aws-c-mqtt";
    rev = "v${version}";
    hash = "sha256-NU+gLFxUJwF6BR4MS+itRTbmGxMOhYdHC3MRztXw4pM=";
  };
}
