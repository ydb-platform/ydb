self: super: with self; rec {
  name = "fast_float";
  version = "8.2.10";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-DOwDLnMwlXAP5SfDJuxhkmLmcBZ1wBvXEJr5RuHepp4=";
  };
}
