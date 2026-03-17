pkgs: attrs: with pkgs; rec {
  version = "0.2.1";

  src = fetchFromGitHub {
    owner = "google";
    repo = "sentencepiece";
    rev = "v${version}";
    hash = "sha256-q0JgMxoD9PLqr6zKmOdrK2A+9RXVDub6xy7NOapS+vs=";
  };

  buildInputs = [
    abseil-cpp
    protobuf
  ];
  cmakeFlags = [
    "-DSPM_ABSL_PROVIDER=package"
    "-DSPM_PROTOBUF_PROVIDER=package"
  ];
}
