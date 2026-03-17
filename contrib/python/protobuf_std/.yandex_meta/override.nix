pkgs: attrs: with pkgs; with python310.pkgs; with attrs; rec {
  pname = "protobuf";
  version = "5.27.5";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-f6gbxVAgEUSjL0R4ZZ2gbgsuvk1TA6rM6aICocPVF40=";
  };

  prePatch = "";
  patches = [];
  postPatch = "";

  propagatedBuildInputs = [];

  sourceRoot = "protobuf-${version}";

  setupPyGlobalFlags = [];
}
