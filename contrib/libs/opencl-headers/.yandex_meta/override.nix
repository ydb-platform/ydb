pkgs: attrs: with pkgs; with attrs; rec {
  version = "2023.12.14";

  src = fetchFromGitHub {
    owner = "KhronosGroup";
    repo = "OpenCL-Headers";
    rev = "v${version}";
    hash = "sha256-wF9KQjzYKJf6ulXRy80o53bp6lTtm8q1NubKbcH+RY0=";
  };
}
