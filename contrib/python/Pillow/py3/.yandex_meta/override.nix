pkgs: attrs: with pkgs.python310.pkgs; with pkgs; with attrs; rec {
  pname = "pillow";
  version = "10.2.0";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-6H8LLHgVfhLXaGsn1jwHD9ZdmU6N2ubzKODc9KDNAH4=";
  };

  patches = [];
}
