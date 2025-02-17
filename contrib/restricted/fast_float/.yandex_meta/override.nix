self: super: with self; rec {
  name = "fast_float";
  version = "7.0.0";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-CG5je117WYyemTe5PTqznDP0bvY5TeXn8Vu1Xh5yUzQ=";
  };
}
