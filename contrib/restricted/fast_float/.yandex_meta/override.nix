self: super: with self; rec {
  name = "fast_float";
  version = "8.2.2";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-IZCZZayK3INU5P8HwZvWfOJuRfBrbPYuMQCjgCjHCWE=";
  };
}
