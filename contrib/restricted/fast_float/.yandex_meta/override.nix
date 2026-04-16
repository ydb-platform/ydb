self: super: with self; rec {
  name = "fast_float";
  version = "8.2.4";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-VuiOuslq9BpATlMgcoIJSDC1Y4unF0GAs1ypnMMfrQU=";
  };
}
