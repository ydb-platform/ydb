self: super: with self; rec {
  version = "2.27.7-1";

  src = fetchFromGitHub {
    owner = "NVIDIA";
    repo = "nccl";
    rev = "v${version}";
    hash = "sha256-mlZhcan/KqfFrpoHnesW1HEA7b0Uxg3PBkRyFlsk/ps=";
  };
}
